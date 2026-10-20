#!/usr/bin/env bash
# Failure injection under load for the local stack (docker compose up). Each scenario starts the load
# generator (tools/ocs-loadgen) at a steady rate, breaks one component for a few seconds, restores it,
# and lets the generator reconcile every balance afterwards. The run reports what the fault did to
# availability, second by second, and fails if money was created, lost or taken twice.
#
#   dev/chaos.sh replica-freeze primary-stop primary-kill gateway-kill redis-and-gateway gateway-drain session-loss     (or: all)
#
# Needs docker compose, Go and a running stack. Development stack only: it stops and kills containers.
# Environment: RATE (sessions/s, default 150), SUBSCRIBERS (300), HOLD (seconds the fault lasts, 8),
# SESSION (how long each charging session lasts, default 2s: sessions must be in flight when the fault strikes).
set -uo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-150}
SUBSCRIBERS=${SUBSCRIBERS:-300}
HOLD=${HOLD:-8}
SESSION=${SESSION:-2s}
WARMUP=3
BEFORE=10                         # seconds of healthy load before the fault
AFTER=${AFTER:-25}                # seconds after the fault is removed, to see recovery
DURATION=$((BEFORE + HOLD + AFTER))
OUT=${OUT:-$(mktemp -d)}

command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
BIN="$OUT/ocs-loadgen"
(cd tools/ocs-loadgen && go build -o "$BIN" .) || exit 2

wait_ready() { # wait_ready <admin port> <seconds>
  for _ in $(seq "$2"); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$1/readyz" || true)" = 200 ] && return 0
    sleep 1
  done
  return 1
}

durability() { # the gateway's own counters: commits acknowledged by fewer replicas than required, commits that skipped the wait,
  # and usage released while Redis was down (saved, charged afterwards, lost)
  curl -fsS --max-time 3 http://127.0.0.1:9080/metrics 2>/dev/null |
    awk '/^ocs_durability_short_total /{s=$2} /^ocs_durability_suspended_total /{u=$2}
         /^ocs_engine_unsettled_terminates_total /{t=$2} /^ocs_engine_unsettled_recorded_total /{r=$2} /^ocs_unsettled_settled_total /{d=$2}
         /^ocs_engine_unsettled_unrecorded_total /{l=$2} /^ocs_unsettled_dead_total /{x=$2} /^ocs_engine_unsettled_updates_total /{w=$2} /^ocs_unsettled_ambiguous_total /{a=$2}
         END{printf "short=%s suspended=%s | unsettled terminates=%s updates=%s recorded=%s charged later=%s lost=%s ambiguous=%s", s+0, u+0, t+0, w+0, r+0, d+0, l+0+x, a+0}'
}

inject() { # inject <scenario> <start|stop>
  case "$1:$2" in
    replica-freeze:start) docker compose pause redis-replica >/dev/null ;;
    replica-freeze:stop)  docker compose unpause redis-replica >/dev/null ;;
    primary-stop:start)   docker compose stop redis >/dev/null 2>&1 ;;
    primary-stop:stop)    docker compose start redis >/dev/null 2>&1 ;;
    primary-kill:start)   docker compose kill redis >/dev/null 2>&1 ;;
    primary-kill:stop)    docker compose start redis >/dev/null 2>&1 ;;
    redis-and-gateway:start) # Redis down, then the gateway restarts while usage it could not charge is still on disk
      docker compose stop redis >/dev/null 2>&1; sleep 3; docker compose kill gateway >/dev/null 2>&1; docker compose start gateway >/dev/null 2>&1 ;;
    redis-and-gateway:stop) docker compose start redis >/dev/null 2>&1 ;;
    session-loss:start) # every session hash is deleted: the data tier no longer knows any session (a lossy failover, a flush, an expiry)
      docker compose exec -T redis sh -c 'P="${OCS_DEV_REDIS_PASSWORD:-dev-only-redis-password}"; redis-cli --tls --cacert /certs/ca.crt -a "$P" --no-auth-warning --scan --pattern "*}:s:*" |
        xargs -r -n 200 redis-cli --tls --cacert /certs/ca.crt -a "$P" --no-auth-warning DEL | awk "{n+=\$1} END{print \"    deleted \" n+0 \" session hashes\"}"' 2>&1 ;;
    session-loss:stop) : ;;
    gateway-drain:start)  docker compose stop redis >/dev/null 2>&1 ;;
    gateway-drain:stop)   # the gateway is told to stop while Redis is still down, with usage saved on disk; Redis returns during its drain
      docker compose stop gateway >/dev/null 2>&1 & local gp=$!
      sleep 2; docker compose start redis >/dev/null 2>&1; wait "$gp"; docker compose start gateway >/dev/null 2>&1 ;;
    gateway-kill:start)   docker compose kill gateway >/dev/null 2>&1 ;;
    gateway-kill:stop)    docker compose start gateway >/dev/null 2>&1 ;;
    *) echo "unknown scenario $1" >&2; return 2 ;;
  esac
}

worst=0
run_scenario() {
  local s=$1 log="$OUT/$1.txt" json="$OUT/$1.json"
  echo "=== $s: $RATE sessions/s, fault after ${BEFORE}s for ${HOLD}s, then ${AFTER}s of recovery"
  for p in 9080 9081; do wait_ready "$p" 120 || { echo "service on admin port $p is not ready before the run" >&2; return 2; }; done
  local settle=0s; [ "$s" = session-loss ] && settle=45s   # reservations orphaned by the loss expire after validity + grace
  "$BIN" -subscribers "$SUBSCRIBERS" -rate "$RATE" -duration "${DURATION}s" -warmup "${WARMUP}s" -conns 4 -settle-wait "$settle" \
    -timeout 3s -hold "$SESSION" -timeline -audit -gateway-admin http://127.0.0.1:9080 -json "$json" > "$log" 2>&1 &
  local pid=$!
  # seeding finishes when the generator starts driving; the fault clock starts from the end of warm-up
  for _ in $(seq 120); do grep -q '^driving' "$log" 2>/dev/null && break; kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  sleep $((WARMUP + BEFORE))
  echo "    $(date +%T) injecting: $s"
  inject "$s" start
  sleep "$HOLD"
  echo "    $(date +%T) restoring"
  inject "$s" stop
  wait "$pid"; local rc=$?
  local dur; dur=$(durability)
  sed -n '/=== load test result ===/,$p' "$log" | grep -v '^$'
  echo "gateway counters after the run: $dur  (reset if the gateway restarted)"
  [ "$s" = gateway-drain ] && docker compose logs --no-color gateway 2>/dev/null | grep "shutdown:" | tail -1 | sed 's/^[^|]*| */gateway log: /'
  # leave the stack healthy for the next scenario
  for p in 9080 9081; do wait_ready "$p" 120 || echo "WARNING: admin port $p did not become ready again after $s" >&2; done
  case $rc in
    0) echo "--- $s: balances reconcile" ;;
    1) echo "--- $s: FAILED (see VIOLATION lines above)"; [ "$worst" -lt 1 ] && worst=1 ;;
    *) echo "--- $s: could not be carried out (exit $rc)"; worst=2 ;;
  esac
  echo
}

scenarios=("$@")
[ "${#scenarios[@]}" -eq 0 ] && { echo "usage: dev/chaos.sh replica-freeze|primary-stop|primary-kill|gateway-kill|redis-and-gateway|gateway-drain|session-loss ... | all" >&2; exit 2; }
[ "${scenarios[0]}" = all ] && scenarios=(replica-freeze primary-stop primary-kill gateway-kill redis-and-gateway gateway-drain session-loss)
for s in "${scenarios[@]}"; do run_scenario "$s"; done
echo "logs and JSON: $OUT"
exit "$worst"
