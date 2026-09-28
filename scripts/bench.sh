#!/usr/bin/env bash
# 三种库存路径的压测。数字只来自 k6 的 summary-export，脚本不改写延迟。
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE_URL:-http://127.0.0.1:18080}"
OUT="${OUT_DIR:-docs/bench-results}"
DURATION="${DURATION:-12s}"
mkdir -p "$OUT" bin

export MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"

if ! curl -fsS "$BASE/healthz" >/dev/null; then
  echo "API 没在 $BASE 上，先 docker compose up -d" >&2
  exit 1
fi

go build -o bin/reconcile ./cmd/reconcile

{
  echo "collected_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  echo "nproc: $(nproc)"
  lscpu
  echo "--- mem ---"
  free -h
  go version
  k6 version
  docker version --format 'docker {{.Server.Version}}'
  docker exec "$(docker compose ps -q redis)" redis-server --version
  docker exec "$(docker compose ps -q mysql)" mysql --version
} >"$OUT/machine.txt"

create_coupon() {
  local strategy="$1" stock="$2" shards="$3" name="$4"
  curl -fsS -X POST "$BASE/v1/coupons" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"total_stock\":$stock,\"shard_count\":$shards,\"strategy\":\"$strategy\",\"pay_timeout_sec\":3600}"
}

coupon_id() {
  python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

run_case() {
  local name="$1" coupon="$2" vus="$3" base="${4:-1}"
  echo "== $name vus=$vus coupon=$coupon user_base=$base =="
  k6 run --quiet --summary-export "$OUT/${name}.json" \
    -e BASE_URL="$BASE" -e COUPON_ID="$coupon" -e VUS="$vus" -e DURATION="$DURATION" -e PREFIX="$name" -e USER_BASE="$base" \
    scripts/k6/grab.js
}

# 库存远大于 12s 内可能打出的请求，避免售罄把延迟测矮。
# docs/benchmark.md 里的对照表来自券 18/19/20（db 30 万、redis/sharded 300 万），不是以后再跑一次的新券。
DB_BODY="$(create_coupon db 300000 1 bench-db)"
REDIS_BODY="$(create_coupon redis 3000000 1 bench-redis)"
SHARD_BODY="$(create_coupon sharded 3000000 16 bench-sharded)"
DB_ID="$(printf '%s' "$DB_BODY" | coupon_id)"
REDIS_ID="$(printf '%s' "$REDIS_BODY" | coupon_id)"
SHARD_ID="$(printf '%s' "$SHARD_BODY" | coupon_id)"
echo "coupons db=$DB_ID redis=$REDIS_ID sharded=$SHARD_ID" | tee "$OUT/coupons.txt"

# USER_BASE 每轮错开，避免后一轮打到前一轮已经成功的用户，把 QPS 测成 already_owned。
run_case db-vu20 "$DB_ID" 20 100000000
run_case db-vu50 "$DB_ID" 50 200000000
run_case redis-vu50 "$REDIS_ID" 50 300000000
run_case redis-vu100 "$REDIS_ID" 100 400000000
run_case redis-vu200 "$REDIS_ID" 200 500000000
run_case sharded-vu50 "$SHARD_ID" 50 600000000
run_case sharded-vu100 "$SHARD_ID" 100 700000000
run_case sharded-vu200 "$SHARD_ID" 200 800000000

# 超卖证明：库存只有 400，请求远多于库存。
PROOF_BODY="$(create_coupon sharded 400 8 oversell-proof)"
PROOF_ID="$(printf '%s' "$PROOF_BODY" | coupon_id)"
echo "proof=$PROOF_ID" | tee -a "$OUT/coupons.txt"
k6 run --quiet --summary-export "$OUT/oversell-proof.json" \
  -e BASE_URL="$BASE" -e COUPON_ID="$PROOF_ID" -e VUS=80 -e DURATION=8s -e PREFIX="proof" -e USER_BASE=900000000 \
  scripts/k6/grab.js
echo "waiting for consumer to drain coupon $PROOF_ID"
if ./bin/reconcile -coupon "$PROOF_ID" -wait 90s >"$OUT/oversell-proof-reconcile.json"; then
  echo "proof consistent"
else
  echo "proof NOT consistent (exit $?)"
fi

# 大库存的 redis / sharded 也等到消化完再对账，确认成功请求没有超卖。
for pair in "redis:$REDIS_ID" "sharded:$SHARD_ID" "db:$DB_ID"; do
  name="${pair%%:*}"
  id="${pair##*:}"
  echo "reconcile $name $id"
  ./bin/reconcile -coupon "$id" -wait 180s >"$OUT/reconcile-${name}.json" || true
done

echo "bench raw results in $OUT"
