#!/usr/bin/env bash
# 故障注入：停消费者、重启 Redis。结果原样落在 docs/bench-results，不手填。
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE_URL:-http://127.0.0.1:18080}"
OUT="${OUT_DIR:-docs/bench-results}"
mkdir -p "$OUT" bin
export MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"
go build -o bin/reconcile ./cmd/reconcile

create_coupon() {
  curl -fsS -X POST "$BASE/v1/coupons" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"total_stock\":$2,\"shard_count\":8,\"strategy\":\"sharded\",\"pay_timeout_sec\":3600}"
}
coupon_id() { python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'; }

echo "===== fault: stop consumer =====" | tee "$OUT/fault-consumer.log"
C1="$(create_coupon fault-consumer 8000)"
ID1="$(printf '%s' "$C1" | coupon_id)"
echo "coupon $ID1" | tee -a "$OUT/fault-consumer.log"
k6 run --quiet --summary-export "$OUT/fault-consumer-k6.json" \
  -e BASE_URL="$BASE" -e COUPON_ID="$ID1" -e VUS=60 -e DURATION=24s -e PREFIX="fc" \
  scripts/k6/grab.js &
K6PID=$!
sleep 6
echo "stopping consumer at $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-consumer.log"
docker compose stop consumer | tee -a "$OUT/fault-consumer.log"
sleep 8
echo "mid-fault reconcile $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-consumer.log"
./bin/reconcile -coupon "$ID1" >"$OUT/fault-consumer-mid.json" || true
python3 - <<'PY' | tee -a "$OUT/fault-consumer.log"
import json
r=json.load(open("docs/bench-results/fault-consumer-mid.json"))
s=r["snapshot"]
print(f"mid consistent={r['consistent']} oversell={r['oversell']} active={r['active']} redis={s['RedisRemaining']} pending={s['MQPending']} lag={s['MQLag']}")
PY
echo "starting consumer at $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-consumer.log"
docker compose start consumer | tee -a "$OUT/fault-consumer.log"
wait "$K6PID" || true
echo "final reconcile $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-consumer.log"
if ./bin/reconcile -coupon "$ID1" -wait 120s >"$OUT/fault-consumer-final.json"; then
  echo "consumer fault recovered consistent" | tee -a "$OUT/fault-consumer.log"
else
  echo "consumer fault final NOT consistent" | tee -a "$OUT/fault-consumer.log"
fi

echo "===== fault: restart redis =====" | tee "$OUT/fault-redis.log"
# 等 API 从刚才的 redis 停止里恢复连接池。restart 会再来一次。
sleep 2
C2="$(create_coupon fault-redis 8000)"
ID2="$(printf '%s' "$C2" | coupon_id)"
echo "coupon $ID2" | tee -a "$OUT/fault-redis.log"
k6 run --quiet --summary-export "$OUT/fault-redis-k6.json" \
  -e BASE_URL="$BASE" -e COUPON_ID="$ID2" -e VUS=40 -e DURATION=24s -e PREFIX="fr" \
  scripts/k6/grab.js &
K6PID=$!
sleep 6
echo "restarting redis at $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-redis.log"
docker compose restart redis | tee -a "$OUT/fault-redis.log"
# 重启后健康检查通过再继续等 k6。
for i in $(seq 1 30); do
  if docker compose exec -T redis redis-cli ping | grep -q PONG; then
    echo "redis ping ok at $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-redis.log"
    break
  fi
  sleep 1
done
wait "$K6PID" || true
# 消费者在 Redis 重启期间会报错，起来后继续消费。
echo "final reconcile $(date -u +%H:%M:%S)" | tee -a "$OUT/fault-redis.log"
if ./bin/reconcile -coupon "$ID2" -wait 120s >"$OUT/fault-redis-final.json"; then
  echo "redis restart recovered consistent" | tee -a "$OUT/fault-redis.log"
else
  echo "redis restart final NOT consistent" | tee -a "$OUT/fault-redis.log"
fi
echo "fault injection done"
