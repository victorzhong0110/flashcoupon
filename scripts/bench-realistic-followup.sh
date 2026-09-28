#!/usr/bin/env bash
# 负载均衡、合并 Lua、Redis SIGKILL 的同一套开卖形状。取消时延另记。
# 数字只来自 k6、mysql、redis-cli 和 reconcile。
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
  for i in $(seq 1 90); do
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

expired_pending() {
  docker compose exec -T mysql mysql -uroot -proot -N coupon -e \
    "SELECT COUNT(*) FROM orders WHERE status='PENDING' AND expire_at<=UTC_TIMESTAMP(3)"
}

prove_one() {
  local tag="$1" user="$2"
  local backlog body id
  backlog="$(expired_pending | tr -d '[:space:]')"
  echo "tag=$tag backlog_expired_pending=$backlog at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/${tag}.log"
  body="$(curl -fsS --max-time 10 -X POST "$BASE/v1/coupons" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${tag}\",\"total_stock\":20,\"shard_count\":8,\"strategy\":\"sharded\",\"pay_timeout_sec\":2}")"
  id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$body")"
  echo "coupon=$id" | tee -a "$OUT/${tag}.log"
  curl -fsS --max-time 10 -X POST "$BASE/v1/coupons/${id}/grab" \
    -H "X-User-Id: ${user}" -H "Idempotency-Key: ${tag}-once" \
    -o "$OUT/${tag}-grab.json"
  echo "grab_at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/${tag}.log"
  python3 scripts/prove_cancel_wait.py "$id" "$user" 20 8 "$OUT/${tag}-result.json" | tee -a "$OUT/${tag}.log"
  ./bin/reconcile -coupon "$id" -wait 30s >"$OUT/${tag}-reconcile.json" || echo "reconcile exit $?" | tee -a "$OUT/${tag}.log"
}

echo "followup collected_at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/followup-machine.txt"
{
  uname -a
  echo "nproc: $(nproc)"
  go version
  k6 version
  docker version --format 'docker {{.Server.Version}}'
} >>"$OUT/followup-machine.txt"

echo "bringing stack up COMBINED_HOTPATH=0 with new consumer and nginx"
COMBINED_HOTPATH=0 docker compose up -d --build --remove-orphans
docker compose up -d --force-recreate --no-deps nginx
wait_http "$BASE/healthz"
for i in $(seq 1 40); do
  if docker compose exec -T api1 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null && \
     docker compose exec -T api2 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null; then
    echo "both api healthy $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep 1
done

echo "===== cancel while backlog is still there ====="
prove_one cancel-fresh 998000000001
echo "drain sample start $(expired_pending | tr -d '[:space:]') $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/cancel-drain.log"
sleep 10
echo "drain sample end $(expired_pending | tr -d '[:space:]') $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/cancel-drain.log"
prove_one cancel-fresh-2 998000000002

echo "===== balanced nginx, COMBINED_HOTPATH=0 ====="
BAL_IDS="$(create_set realistic-balanced 80000 80000 8)"
echo "$BAL_IDS" | tee "$OUT/balanced-ids.txt"
echo "backlog_at_balanced_start $(expired_pending | tr -d '[:space:]')" | tee "$OUT/balanced-backlog.txt"
run_k6 balanced "$BAL_IDS" 995000000000
reconcile_ids balanced "$BAL_IDS"

echo "switching COMBINED_HOTPATH=1"
COMBINED_HOTPATH=1 docker compose up -d --no-deps --force-recreate api1 api2
wait_http "$BASE/healthz"
for i in $(seq 1 30); do
  if docker compose exec -T api1 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null && \
     docker compose exec -T api2 wget -q -O - http://127.0.0.1:8080/healthz >/dev/null; then
    echo "both api healthy $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep 1
done

echo "===== combined lua, same nginx ====="
COM_IDS="$(create_set realistic-combined 80000 80000 8)"
echo "$COM_IDS" | tee "$OUT/combined-ids.txt"
run_k6 combined "$COM_IDS" 996000000000
reconcile_ids combined "$COM_IDS"

echo "===== chaos: redis SIGKILL ====="
KILL_IDS="$(create_set chaos-redis-kill9 80000 80000 8)"
echo "$KILL_IDS" | tee "$OUT/kill9-ids.txt"
RID="$(docker compose ps -q redis)"
run_k6 kill9 "$KILL_IDS" 997000000000 &
KPID=$!
sleep 6
KILL_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "docker kill --signal=KILL redis at $KILL_AT id=$RID" | tee "$OUT/kill9-events.log"
docker kill --signal=KILL "$RID" | tee -a "$OUT/kill9-events.log"
sleep 1
START_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "docker start redis at $START_AT" | tee -a "$OUT/kill9-events.log"
docker start "$RID" | tee -a "$OUT/kill9-events.log"
for i in $(seq 1 120); do
  if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG \
    && docker compose exec -T redis redis-cli INFO persistence 2>/dev/null | grep -q 'loading:0'; then
    echo "redis loading:0 at $(date -u +%Y-%m-%dT%H:%M:%SZ) after kill $KILL_AT start $START_AT" | tee -a "$OUT/kill9-events.log"
    break
  fi
  sleep 0.5
done
wait_http "$BASE/healthz" | tee -a "$OUT/kill9-events.log" || true
wait "$KPID" || true
echo "reconcile after redis ready $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/kill9-events.log"
reconcile_ids kill9 "$KILL_IDS"
echo "followup bench done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
