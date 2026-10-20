#!/usr/bin/env bash
# Smoke test for the local stack (docker compose up). It provisions a subscriber
# over REST, then charges it through the 5G (Nchf) interface and checks the
# balance moved. Needs only curl. Exits non-zero on the first failure.
set -euo pipefail

PROV=${PROV:-http://127.0.0.1:8443}
CATALOG=${CATALOG:-http://127.0.0.1:8444}
NCHF=${NCHF:-http://127.0.0.1:8000}
ADMIN_PORTS=${ADMIN_PORTS:-"9080 9081 9082 9083 9084"}

n=0
ok()  { n=$((n + 1)); echo "PASS  $1"; }
die() { echo "FAIL  $1" >&2; [ -n "${2:-}" ] && echo "$2" >&2; exit 1; }
field() { sed -n "s/.*\"$1\": *\"\{0,1\}\([^\",}]*\).*/\1/p" | head -1; }   # first value of a JSON key

# The stack may have only just started: give the services up to two minutes.
for p in $ADMIN_PORTS; do
  up=""
  for _ in $(seq 120); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$p/readyz" || true)" = 200 ] && { up=1; break; }
    sleep 1
  done
  [ -n "$up" ] || die "the service on admin port $p is not ready after 2 minutes (docker compose ps; docker compose logs)"
done
ok "all services answer /readyz"

curl -fsS "$CATALOG/v1/catalog/versions" | grep -q '"inEffect":true' || die "no catalog version in effect"
ok "a catalog version is in effect"

# ---- provision: create, attach an offering, top up
suffix=$(date +%s)
key="smoke-$suffix"
imsi="26201$(printf '%010d' $((suffix % 10000000000)))"
msisdn="49170$(printf '%07d' $((suffix % 10000000)))"

submit() { # submit <path> <idempotency-key> <json>  -> prints the command id
  curl -fsS -X POST -H 'Content-Type: application/json' -H "Idempotency-Key: $2" -d "$3" "$PROV$1" | field commandId
}
await() { # await <command id> -> waits until it is applied
  local id=$1 rec state
  for _ in $(seq 60); do
    rec=$(curl -fsS "$PROV/v1/commands/$id")
    state=$(field state <<<"$rec")
    [ "$state" = applied ] && return 0
    [ "$state" = failed ] && die "command $id failed" "$rec"
    sleep 0.5
  done
  die "command $id was not applied within 30 s" "$rec"
}

await "$(submit /v1/subscribers "$key-create" "{\"subscriberKey\":\"$key\",\"paymentType\":\"prepaid\",\"identifiers\":[{\"type\":\"imsi\",\"value\":\"$imsi\"},{\"type\":\"msisdn\",\"value\":\"$msisdn\"}]}")"
ok "created subscriber $key (IMSI $imsi, MSISDN $msisdn)"
await "$(submit "/v1/subscribers/$key/offering" "$key-offering" '{"offering":"gold-offer"}')"
ok "attached gold-offer (a 10 GiB bundle and 50 promo events)"
await "$(submit "/v1/subscribers/$key/top-ups" "$key-topup" '{"amountMinorUnits":5000,"currency":"EUR"}')"
ok "topped up 5000 minor units"

sub=$(curl -fsS "$PROV/v1/subscribers?msisdn=$msisdn")
[ "$(field subscriberKey <<<"$sub")" = "$key" ] || die "lookup by MSISDN did not find $key" "$sub"
grep -q 'data10g' <<<"$sub" || die "the offering's bundle is missing" "$sub"
ok "found by MSISDN, with its allowance buckets"

# ---- charge through Nchf (HTTP/2 without TLS in the dev stack)
bundle() { curl -fsS "$PROV/v1/subscribers/$key" | tr -d '\n ' | sed -n 's/.*"balance":\([0-9]*\)[^}]*"id":"data10g".*/\1/p'; }
before=$(bundle)
[ -n "$before" ] || die "cannot read the data10g balance"
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
create=$(cat <<JSON
{"subscriberIdentifier":"imsi-$imsi","nfConsumerIdentification":{"nodeFunctionality":"SMF","nFName":"smf-smoke"},
 "invocationTimeStamp":"$now","invocationSequenceNumber":1,
 "multipleUnitUsage":[{"ratingGroup":100,"requestedUnit":{"totalVolume":1000000}}],
 "pDUSessionChargingInformation":{"pduSessionInformation":{"pduSessionID":5,"ratType":"NR"}}}
JSON
)
hdrs=$(mktemp); body=$(curl -sS --http2-prior-knowledge -D "$hdrs" -X POST -H 'Content-Type: application/json' -d "$create" "$NCHF/nchf-convergedcharging/v3/chargingdata")
grep -qi '^HTTP/2 201' "$hdrs" || die "Nchf create did not answer 201" "$(cat "$hdrs")$body"
grep -q '"SUCCESS"' <<<"$body" || die "Nchf did not grant quota" "$body"
ok "Nchf granted 1,000,000 bytes: $(field totalVolume <<<"${body#*grantedUnit}") from the bundle"
after=$(bundle)
[ "$after" -lt "$before" ] || die "the bundle balance did not drop ($before -> $after)"
ok "the reservation moved the bundle balance: $before -> $after"

loc=$(sed -n 's/^[Ll]ocation: *//p' "$hdrs" | tr -d '\r')
rel=$(curl -sS --http2-prior-knowledge -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d "{\"subscriberIdentifier\":\"imsi-$imsi\",\"nfConsumerIdentification\":{\"nodeFunctionality\":\"SMF\",\"nFName\":\"smf-smoke\"},\"invocationTimeStamp\":\"$now\",\"invocationSequenceNumber\":2,\"multipleUnitUsage\":[{\"ratingGroup\":100,\"usedUnitContainer\":[{\"localSequenceNumber\":1,\"totalVolume\":250000}]}]}" \
  "$loc/release")
[ "$rel" = 204 ] || die "Nchf release did not answer 204 (got $rel)"
settled=$(bundle)
[ "$settled" -gt "$after" ] || die "the unused part of the reservation was not returned ($after -> $settled)"
ok "release settled 250,000 bytes used and returned the rest: $after -> $settled"

# ---- offline mediation: a CDR file for this subscriber is enriched and written out (needs docker compose)
if command -v docker >/dev/null && docker compose ps --status running mediation 2>/dev/null | grep -q mediation; then
  counter() { curl -fsS http://127.0.0.1:9083/metrics | sed -n "s/^ocs_mediation_records_total{outcome=\"$1\"} //p"; }
  out_before=$(counter output)
  cdr=$(mktemp); chmod 644 "$cdr"   # the service runs as a non-root user and must be able to read the copy
  echo "{\"type\":\"PGW\",\"nodeId\":\"pgw-smoke\",\"chargingId\":$suffix,\"sequence\":1,\"imsi\":\"$imsi\",\"ratType\":\"LTE\",\"openTime\":\"$now\",\"durationSeconds\":60,\"containers\":[{\"ratingGroup\":100,\"uplink\":1000,\"downlink\":9000}]}" > "$cdr"
  docker compose cp "$cdr" "mediation:/data/in/smoke-$suffix.jsonl" >/dev/null
  for _ in $(seq 60); do [ "$(counter output)" -gt "$out_before" ] && break; sleep 1; done
  [ "$(counter output)" -gt "$out_before" ] || die "mediation did not output the CDR within a minute"
  ok "mediation enriched a CDR file for $key and wrote it out"
else
  echo "SKIP  mediation check (docker compose is not managing a running stack here)"
fi

# ---- observability and consoles
curl -fsS http://127.0.0.1:9080/metrics | grep -q '^ocs_engine_requests_total [1-9]' || die "the gateway counted no requests"
curl -fsS http://127.0.0.1:9081/metrics | grep -q 'ocs_prov_commands_total{outcome="applied"} [1-9]' || die "provisioning counted no applied commands"
ok "metrics reflect the traffic (gateway and provisioning)"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$PROV/ui/")" = 200 ] && [ "$(curl -s -o /dev/null -w '%{http_code}' "$CATALOG/ui/")" = 200 ] || die "a console is not served"
ok "consoles: $PROV/ui/  and  $CATALOG/ui/"

echo "all $n checks passed"
