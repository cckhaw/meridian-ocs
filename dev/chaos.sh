#!/usr/bin/env bash
# Failure injection under load for the local stack (docker compose up). Each scenario starts the load
# generator (tools/ocs-loadgen) at a steady rate, breaks one component for a few seconds, restores it,
# and lets the generator reconcile every balance afterwards. The run reports what the fault did to
# availability, second by second, and fails if money was created, lost or taken twice.
#
#   dev/chaos.sh replica-freeze primary-stop primary-kill gateway-kill     (or: all)
#
# Needs docker compose, Go and a running stack. Development stack only: it stops and kills containers.
# Environment: RATE (sessions/s, default 150), SUBSCRIBERS (300), HOLD (seconds the fault lasts, 8).
set -uo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-150}
SUBSCRIBERS=${SUBSCRIBERS:-300}
HOLD=${HOLD:-8}
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

durability() { # the gateway's own counters: commits acknowledged by fewer replicas than required, and commits that skipped the wait
  curl -fsS --max-time 3 http://127.0.0.1:9080/metrics 2>/dev/null |
    awk '/^ocs_durability_short_total /{s=$2} /^ocs_durability_suspended_total /{u=$2} END{printf "short=%s suspended=%s", s+0, u+0}'
}

inject() { # inject <scenario> <start|stop>
  case "$1:$2" in
    replica-freeze:start) docker compose pause redis-replica >/dev/null ;;
    replica-freeze:stop)  docker compose unpause redis-replica >/dev/null ;;
    primary-stop:start)   docker compose stop redis >/dev/null 2>&1 ;;
    primary-stop:stop)    docker compose start redis >/dev/null 2>&1 ;;
    primary-kill:start)   docker compose kill redis >/dev/null 2>&1 ;;
    primary-kill:stop)    docker compose start redis >/dev/null 2>&1 ;;
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
  "$BIN" -subscribers "$SUBSCRIBERS" -rate "$RATE" -duration "${DURATION}s" -warmup "${WARMUP}s" -conns 4 \
    -timeout 3s -timeline -audit -gateway-admin http://127.0.0.1:9080 -json "$json" > "$log" 2>&1 &
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
  echo "gateway durability counters after the run: $dur  (commits that were not replica-acknowledged; reset if the gateway restarted)"
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
[ "${#scenarios[@]}" -eq 0 ] && { echo "usage: dev/chaos.sh replica-freeze|primary-stop|primary-kill|gateway-kill ... | all" >&2; exit 2; }
[ "${scenarios[0]}" = all ] && scenarios=(replica-freeze primary-stop primary-kill gateway-kill)
for s in "${scenarios[@]}"; do run_scenario "$s"; done
echo "logs and JSON: $OUT"
exit "$worst"
