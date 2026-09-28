#!/usr/bin/env bash
# 突发 + Zipf + 机器人/重试 + nginx 双实例。数字只来自 k6 和 reconcile，脚本不改写延迟。
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE_URL:-http://127.0.0.1:18080}"
OUT="${OUT_DIR:-docs/bench-results/realistic}"
mkdir -p "$OUT" bin /tmp
export MYSQL_DSN="${MYSQL_DSN:-root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"

go build -o bin/reconcile ./cmd/reconcile

wait_http() {
  local url="$1" i
  for i in $(seq 1 60); do
    if curl -fsS --max-time 2 "$url" >/dev/null; then
      echo "up $url $(date -u +%Y-%m-%dT%H:%M:%SZ)"
      return 0
    fi
    sleep 1
  done
  echo "DOWN $url" >&2
  return 1
}

sample_stats() {
  local out="$1"
  echo "ts,name,cpu,mem" >"$out"
  while true; do
    local ts
    ts="$(date -u +%Y-%m-%dT%H:%M:%S)"
    docker stats --no-stream --format "${ts},{{.Name}},{{.CPUPerc}},{{.MemUsage}}" >>"$out" 2>/dev/null || true
    sleep 1
  done
}

create_set() {
  local name="$1" hot="$2" other="$3" n="$4"
  local i body id ids=""
  for i in $(seq 0 $((n - 1))); do
    local stock="$other"
    if [[ "$i" -eq 0 ]]; then stock="$hot"; fi
    body="$(curl -fsS --max-time 10 -X POST "$BASE/v1/coupons" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"${name}-${i}\",\"total_stock\":${stock},\"shard_count\":8,\"strategy\":\"sharded\",\"pay_timeout_sec\":3600}")"
    id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$body")"
    if [[ -n "$ids" ]]; then ids+=","; fi
    ids+="$id"
  done
  printf '%s\n' "$ids"
}

run_k6() {
  local name="$1" ids="$2" user_base="$3"
  local raw="/tmp/k6-${name}.json"
  rm -f "$raw"
  echo "== $name user_base=$user_base $(date -u +%Y-%m-%dT%H:%M:%SZ) ==" | tee "$OUT/${name}.log"
  docker compose exec -T api1 printenv COMBINED_HOTPATH SNOWFLAKE_WORKER | tee -a "$OUT/${name}.log"
  docker compose exec -T api2 printenv COMBINED_HOTPATH SNOWFLAKE_WORKER | tee -a "$OUT/${name}.log"
  sample_stats "$OUT/${name}-stats.csv" &
  local spid=$!
  (
    sleep 4
    {
      echo "----- $(date -u +%Y-%m-%dT%H:%M:%SZ) -----"
      docker compose exec -T redis redis-cli INFO stats
      echo "----- commandstats -----"
      docker compose exec -T redis redis-cli INFO commandstats
    } >"$OUT/${name}-redis-info.txt" 2>&1 || true
  ) &
  local ipid=$!
  k6 run --quiet --summary-export "$OUT/${name}.json" --out "json=${raw}" \
    -e BASE_URL="$BASE" -e COUPON_IDS="$ids" -e PREFIX="$name" -e USER_BASE="$user_base" \
    -e BOT_BASE="$((user_base + 50000000000))" -e REPLAY_BASE="$((user_base + 60000000000))" \
    scripts/k6/realistic.js || echo "k6 exit $?" | tee -a "$OUT/${name}.log"
  wait "$ipid" 2>/dev/null || true
  kill "$spid" 2>/dev/null || true
  wait "$spid" 2>/dev/null || true
  python3 scripts/k6/reduce_points.py <"$raw" >"$OUT/${name}-per-second.csv"
  rm -f "$raw"
  return 0
}

reconcile_ids() {
  local tag="$1" ids_csv="$2" id
  IFS=',' read -r -a arr <<<"$ids_csv"
  for id in "${arr[@]}"; do
    echo "reconcile $tag $id $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/${tag}-reconcile.log"
    if ./bin/reconcile -coupon "$id" -wait 150s >"$OUT/${tag}-reconcile-${id}.json"; then
      echo "consistent $id" | tee -a "$OUT/${tag}-reconcile.log"
    else
      echo "NOT consistent $id exit $?" | tee -a "$OUT/${tag}-reconcile.log"
    fi
  done
}

{
  echo "collected_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  echo "nproc: $(nproc)"
  lscpu | sed -n '1,30p'
  echo "--- mem ---"
  free -h
  go version
  k6 version
  docker version --format 'docker {{.Server.Version}}'
} >"$OUT/machine.txt"

echo "bringing stack up COMBINED_HOTPATH=0"
COMBINED_HOTPATH=0 docker compose up -d --build --remove-orphans
wait_http "$BASE/healthz"

MODE="${1:-all}"

{
  echo "inspect $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  docker inspect -f '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}' \
    $(docker compose ps -q)
} | tee "$OUT/limits.txt"

if [[ "$MODE" == "fixed" ]]; then
  echo "fixed-path phases only (bloom checks Redis on a local miss), COMBINED_HOTPATH=0"
  FIXED_IDS="$(create_set realistic-fixed 80000 80000 8)"
  echo "$FIXED_IDS" | tee "$OUT/fixed-ids.txt"
  run_k6 fixed "$FIXED_IDS" 990000000000
  reconcile_ids fixed "$FIXED_IDS"

  echo "===== oversell after bloom fix ====="
  HOT_IDS="$(create_set realistic-fixed-hot 400 20000 8)"
  echo "$HOT_IDS" | tee "$OUT/fixed-oversell-ids.txt"
  run_k6 fixed-oversell "$HOT_IDS" 991000000000
  reconcile_ids fixed-oversell "$HOT_IDS"

  echo "===== chaos: kill api2 (fixed) ====="
  CHAOS_API_IDS="$(create_set chaos-api-fixed 80000 80000 8)"
  echo "$CHAOS_API_IDS" | tee "$OUT/fixed-chaos-api-ids.txt"
  API2="$(docker compose ps -q api2)"
  run_k6 fixed-chaos-api "$CHAOS_API_IDS" 992000000000 &
  KPID=$!
  sleep 6
  KILL_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "kill api2 at $KILL_AT id=$API2" | tee "$OUT/fixed-chaos-api-events.log"
  docker kill "$API2" | tee -a "$OUT/fixed-chaos-api-events.log"
  sleep 5
  START_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "start api2 at $START_AT" | tee -a "$OUT/fixed-chaos-api-events.log"
  docker start "$API2" | tee -a "$OUT/fixed-chaos-api-events.log"
  for i in $(seq 1 40); do
    if docker exec "$API2" wget -q -O - http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
      echo "api2 healthy at $(date -u +%Y-%m-%dT%H:%M:%SZ) after start $START_AT" | tee -a "$OUT/fixed-chaos-api-events.log"
      break
    fi
    sleep 0.5
  done
  wait "$KPID" || true
  reconcile_ids fixed-chaos-api "$CHAOS_API_IDS"

  echo "===== chaos: restart redis (fixed) ====="
  CHAOS_REDIS_IDS="$(create_set chaos-redis-fixed 80000 80000 8)"
  echo "$CHAOS_REDIS_IDS" | tee "$OUT/fixed-chaos-redis-ids.txt"
  run_k6 fixed-chaos-redis "$CHAOS_REDIS_IDS" 993000000000 &
  KPID=$!
  sleep 6
  echo "restart redis at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/fixed-chaos-redis-events.log"
  docker compose restart redis | tee -a "$OUT/fixed-chaos-redis-events.log"
  for i in $(seq 1 40); do
    if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
      echo "redis pong at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-redis-events.log"
      break
    fi
    sleep 0.5
  done
  wait_http "$BASE/healthz" | tee -a "$OUT/fixed-chaos-redis-events.log" || true
  wait "$KPID" || true
  reconcile_ids fixed-chaos-redis "$CHAOS_REDIS_IDS"

  echo "===== chaos: stop consumer (fixed) ====="
  CHAOS_C_IDS="$(create_set chaos-consumer-fixed 200000 200000 8)"
  echo "$CHAOS_C_IDS" | tee "$OUT/fixed-chaos-consumer-ids.txt"
  HOTTEST="${CHAOS_C_IDS%%,*}"
  run_k6 fixed-chaos-consumer "$CHAOS_C_IDS" 994000000000 &
  KPID=$!
  sleep 5
  echo "stop consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/fixed-chaos-consumer-events.log"
  docker compose stop consumer | tee -a "$OUT/fixed-chaos-consumer-events.log"
  sleep 2
  echo "mid reconcile $HOTTEST $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  ./bin/reconcile -coupon "$HOTTEST" >"$OUT/fixed-chaos-consumer-mid.json" || true
  wait "$KPID" || true
  echo "held reconcile $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  ./bin/reconcile -coupon "$HOTTEST" >"$OUT/fixed-chaos-consumer-held.json" || true
  echo "start consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  docker compose start consumer | tee -a "$OUT/fixed-chaos-consumer-events.log"
  echo "final wait $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  if ./bin/reconcile -coupon "$HOTTEST" -wait 180s >"$OUT/fixed-chaos-consumer-final.json"; then
    echo "consumer recovered consistent at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  else
    echo "consumer NOT consistent at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/fixed-chaos-consumer-events.log"
  fi
  reconcile_ids fixed-chaos-consumer "$CHAOS_C_IDS"
  echo "fixed realistic bench done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  exit 0
fi

BEFORE_IDS="$(create_set realistic-before 80000 80000 8)"
echo "$BEFORE_IDS" | tee "$OUT/before-ids.txt"
run_k6 before "$BEFORE_IDS" 930000000000
reconcile_ids before "$BEFORE_IDS"

echo "switching COMBINED_HOTPATH=1"
COMBINED_HOTPATH=1 docker compose up -d --no-deps --force-recreate api1 api2
wait_http "$BASE/healthz"
# 重建后等两边都健康，避免流量只打到一个实例。
for i in $(seq 1 30); do
  if docker compose exec -T api1 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null && \
     docker compose exec -T api2 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null; then
    echo "both api healthy $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep 1
done

AFTER_IDS="$(create_set realistic-after 80000 80000 8)"
echo "$AFTER_IDS" | tee "$OUT/after-ids.txt"
run_k6 after "$AFTER_IDS" 940000000000
reconcile_ids after "$AFTER_IDS"

echo "===== oversell on hot coupon, combined path ====="
HOT_IDS="$(create_set realistic-hot 400 20000 8)"
echo "$HOT_IDS" | tee "$OUT/oversell-ids.txt"
run_k6 oversell "$HOT_IDS" 950000000000
reconcile_ids oversell "$HOT_IDS"

echo "===== chaos: kill api2 ====="
CHAOS_API_IDS="$(create_set chaos-api 80000 80000 8)"
echo "$CHAOS_API_IDS" | tee "$OUT/chaos-api-ids.txt"
API2="$(docker compose ps -q api2)"
run_k6 chaos-api "$CHAOS_API_IDS" 960000000000 &
KPID=$!
sleep 6
KILL_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "kill api2 at $KILL_AT id=$API2" | tee "$OUT/chaos-api-events.log"
docker kill "$API2" | tee -a "$OUT/chaos-api-events.log"
sleep 5
START_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "start api2 at $START_AT" | tee -a "$OUT/chaos-api-events.log"
docker start "$API2" | tee -a "$OUT/chaos-api-events.log"
for i in $(seq 1 40); do
  if docker exec "$API2" wget -q -O - http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    echo "api2 healthy at $(date -u +%Y-%m-%dT%H:%M:%SZ) after start $START_AT" | tee -a "$OUT/chaos-api-events.log"
    break
  fi
  sleep 0.5
done
wait "$KPID" || true
reconcile_ids chaos-api "$CHAOS_API_IDS"

echo "===== chaos: restart redis ====="
CHAOS_REDIS_IDS="$(create_set chaos-redis 80000 80000 8)"
echo "$CHAOS_REDIS_IDS" | tee "$OUT/chaos-redis-ids.txt"
run_k6 chaos-redis "$CHAOS_REDIS_IDS" 970000000000 &
KPID=$!
sleep 6
echo "restart redis at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/chaos-redis-events.log"
docker compose restart redis | tee -a "$OUT/chaos-redis-events.log"
for i in $(seq 1 40); do
  if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "redis pong at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-redis-events.log"
    break
  fi
  sleep 0.5
done
wait_http "$BASE/healthz" | tee -a "$OUT/chaos-redis-events.log" || true
wait "$KPID" || true
reconcile_ids chaos-redis "$CHAOS_REDIS_IDS"

echo "===== chaos: stop consumer mid-sale ====="
CHAOS_C_IDS="$(create_set chaos-consumer 200000 200000 8)"
echo "$CHAOS_C_IDS" | tee "$OUT/chaos-consumer-ids.txt"
HOTTEST="${CHAOS_C_IDS%%,*}"
run_k6 chaos-consumer "$CHAOS_C_IDS" 980000000000 &
KPID=$!
sleep 5
echo "stop consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/chaos-consumer-events.log"
docker compose stop consumer | tee -a "$OUT/chaos-consumer-events.log"
sleep 2
echo "mid reconcile $HOTTEST $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
./bin/reconcile -coupon "$HOTTEST" >"$OUT/chaos-consumer-mid.json" || true
python3 - <<'PY' | tee -a "$OUT/chaos-consumer-events.log"
import json
r=json.load(open("docs/bench-results/realistic/chaos-consumer-mid.json"))
s=r["snapshot"]
print(f"mid consistent={r['consistent']} oversell={r['oversell']} active={r['active']} redis={s['RedisRemaining']} users={s['RedisUsers']} total={s['Total']} lag={s['MQLag']}")
PY
wait "$KPID" || true
echo "held reconcile $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
./bin/reconcile -coupon "$HOTTEST" >"$OUT/chaos-consumer-held.json" || true
echo "start consumer at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
docker compose start consumer | tee -a "$OUT/chaos-consumer-events.log"
echo "final wait $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
if ./bin/reconcile -coupon "$HOTTEST" -wait 180s >"$OUT/chaos-consumer-final.json"; then
  echo "consumer recovered consistent at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
else
  echo "consumer NOT consistent at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/chaos-consumer-events.log"
fi
reconcile_ids chaos-consumer "$CHAOS_C_IDS"

echo "realistic bench done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
