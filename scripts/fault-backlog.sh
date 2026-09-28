#!/usr/bin/env bash
# 停消费者时库存仍然很大，让 Redis 预扣和 MySQL 订单拉开差距，再拉起消费者等到账平。
# 8000 库存的 fault.sh 会在停掉之前就售罄并消化完，看不到积压。这个脚本专门补那一刀。
set -u
cd "$(dirname "$0")/.."

BASE="${BASE_URL:-http://127.0.0.1:18080}"
OUT="${OUT_DIR:-docs/bench-results}"
mkdir -p "$OUT" bin
export MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"

go build -o bin/reconcile ./cmd/reconcile

BODY="$(curl -fsS -X POST "$BASE/v1/coupons" \
  -H 'Content-Type: application/json' \
  -d '{"name":"fault-backlog","total_stock":500000,"shard_count":8,"strategy":"sharded","pay_timeout_sec":3600}')"
printf '%s\n' "$BODY" >"$OUT/fault-backlog-coupon.json"
ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$BODY")"
echo "$ID" >"$OUT/fault-backlog-id.txt"

LOG="$OUT/fault-backlog.log"
{
  echo "coupon $ID stock 500000"
  echo "k6 start $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} | tee "$LOG"

k6 run --quiet --summary-export "$OUT/fault-backlog-k6.json" \
  -e BASE_URL="$BASE" -e COUPON_ID="$ID" -e VUS=80 -e DURATION=18s -e PREFIX="backlog" -e USER_BASE=820000000 \
  scripts/k6/grab.js &
K6PID=$!

sleep 3
echo "stopping consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
docker compose stop consumer | tee -a "$LOG"

echo "mid reconcile $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
./bin/reconcile -coupon "$ID" >"$OUT/fault-backlog-mid.json"
echo "mid exit $?" | tee -a "$LOG"
python3 - <<'PY' | tee -a "$LOG"
import json
r=json.load(open("docs/bench-results/fault-backlog-mid.json"))
s=r["snapshot"]
print(f"mid consistent={r['consistent']} balanced={r['balanced']} oversell={r['oversell']} active={r['active']} redis={s['RedisRemaining']} users={s['RedisUsers']} db_pending={s['DBPending']} mq_pending={s['MQPending']} lag={s['MQLag']} total={s['Total']}")
print("notes", r.get("notes"))
PY
docker compose exec -T redis redis-cli XINFO GROUPS tb:stream:orders | tee -a "$LOG" || true

# 消费者保持停止，直到这轮 k6 结束，积压才会留在流里。
wait "$K6PID"
echo "k6 done exit $? at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"

echo "held reconcile $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
./bin/reconcile -coupon "$ID" >"$OUT/fault-backlog-held.json"
echo "held exit $?" | tee -a "$LOG"
python3 - <<'PY' | tee -a "$LOG"
import json
r=json.load(open("docs/bench-results/fault-backlog-held.json"))
s=r["snapshot"]
print(f"held consistent={r['consistent']} balanced={r['balanced']} oversell={r['oversell']} active={r['active']} redis={s['RedisRemaining']} users={s['RedisUsers']} db_pending={s['DBPending']} mq_pending={s['MQPending']} lag={s['MQLag']} total={s['Total']}")
print("notes", r.get("notes"))
PY

echo "starting consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
docker compose start consumer | tee -a "$LOG"
echo "final reconcile $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
./bin/reconcile -coupon "$ID" -wait 240s >"$OUT/fault-backlog-final.json"
echo "final exit $? at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
python3 - <<'PY' | tee -a "$LOG"
import json
r=json.load(open("docs/bench-results/fault-backlog-final.json"))
s=r["snapshot"]
print(f"final consistent={r['consistent']} balanced={r['balanced']} oversell={r['oversell']} active={r['active']} redis={s['RedisRemaining']} users={s['RedisUsers']} db_pending={s['DBPending']} mq_pending={s['MQPending']} lag={s['MQLag']} total={s['Total']}")
print("notes", r.get("notes"))
PY
echo "fault-backlog done"
