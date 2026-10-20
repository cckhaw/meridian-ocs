#!/usr/bin/env bash
# Chart checks that need only helm (and kubeconform if it is installed):
#   - both CI value sets lint and render, and the output is schema-valid
#   - every deliberate refusal still refuses, with the expected reason
#   - the security properties of the rendered pods hold
# Usage: deploy/helm/test.sh        (HELM=/path/to/helm to choose the binary)
set -uo pipefail
cd "$(dirname "$0")/meridian-ocs"
HELM=${HELM:-helm}
fail=0
ok()  { echo "PASS  $1"; }
bad() { echo "FAIL  $1"; fail=1; }

$HELM lint . -f ci/secure.yaml >/dev/null && ok "lint, secure values" || bad "lint, secure values"
$HELM lint . -f ci/dev.yaml >/dev/null && ok "lint, dev values" || bad "lint, dev values"
secure=$($HELM template r . -f ci/secure.yaml) || { bad "render, secure values"; secure=""; }
dev=$($HELM template r . -f ci/dev.yaml) || { bad "render, dev values"; dev=""; }

if command -v kubeconform >/dev/null; then
  if printf '%s\n---\n%s\n' "$secure" "$dev" | kubeconform -strict -ignore-missing-schemas -kubernetes-version 1.29.0 -summary - >/dev/null; then
    ok "rendered manifests are valid Kubernetes objects"
  else bad "rendered manifests are valid Kubernetes objects"; fi
else echo "SKIP  kubeconform not installed"; fi

# --- generated files are current
if python3 ../gen-dashboard.py --check; then ok "dashboard.json is current"; else bad "dashboard.json is out of date (run deploy/helm/gen-dashboard.py)"; fi
if python3 -c "import yaml" 2>/dev/null; then
  if python3 ../gen-schema.py --check; then ok "values.schema.json is current"; else bad "values.schema.json is out of date (run deploy/helm/gen-schema.py)"; fi
else echo "SKIP  PyYAML not installed (schema currency)"; fi

# --- alert rules and dashboard
rules=$(awk 'BEGIN{RS="---\n"} /kind: PrometheusRule/' <<<"$secure")
dash=$(awk 'BEGIN{RS="---\n"} /meridian-ocs.json/' <<<"$secure")
[ -n "$rules" ] && ok "PrometheusRule rendered" || bad "PrometheusRule not rendered"
[ -n "$dash" ] && ok "dashboard ConfigMap rendered" || bad "dashboard ConfigMap not rendered"
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
sed -n '/^spec:/,$p' <<<"$rules" | sed '1d;s/^  //' > "$tmp/rules.yaml"
if command -v promtool >/dev/null; then
  promtool check rules "$tmp/rules.yaml" >/dev/null && ok "promtool accepts the rules" || bad "promtool rejects the rules"
  cp ../rules-test.yaml "$tmp/"
  if out=$(promtool test rules "$tmp/rules-test.yaml" 2>&1); then ok "alert unit tests pass ($(grep -c '^  - name:' ../rules-test.yaml) scenarios)"; else bad "alert unit tests: $out"; fi
else echo "SKIP  promtool not installed"; fi
# Every metric the rules and the dashboard use must be one the Go code registers
# (names built from a prefix, like <prefix>_http_requests_total, are checked in two parts).
root=$(cd ../../.. && pwd)
names=$( { cat "$tmp/rules.yaml"; cat files/dashboard.json; } | grep -oE '\b(ocs|go)_[a-z0-9_]+' | sed -E 's/_(bucket|sum|count)$//' | sort -u)
missing=""
for m in $names; do
  if grep -rqF --include='*.go' --exclude='*_test.go' "\"$m\"" "$root/services" "$root/pkg"; then continue; fi
  if [[ $m =~ ^(.+)_http_(requests_total|request_duration_seconds)$ ]]; then
    p=${BASH_REMATCH[1]}; sfx=${BASH_REMATCH[2]}
    grep -rqF --include='*.go' --exclude='*_test.go' "\"$p\"" "$root/services" && grep -rqF --include='*.go' --exclude='*_test.go' "\"_http_$sfx\"" "$root/pkg" && continue
  fi
  missing="$missing $m"
done
[ -z "$missing" ] && ok "every metric in the rules and dashboard is registered in the code ($(wc -w <<<"$names") names)" || bad "metrics used but never registered:$missing"

# refuses <label> <expected message fragment> <helm args...>
refuses() {
  local label=$1 want=$2; shift 2
  local out
  if out=$($HELM template r . "$@" 2>&1); then bad "$label (rendered, should refuse)"
  elif grep -qF -- "$want" <<<"$out"; then ok "$label"
  else bad "$label (refused for another reason: $(tail -1 <<<"$out" | cut -c1-160))"; fi
}
S="-f ci/secure.yaml"
refuses "no image repository"                      "image.repository is required"            $S --set image.repository=
refuses "no image tag"                             "image.tag is required"                   $S --set image.tag=
refuses "an open Redis (no password) is refused"      "redis.password.secretName is required" $S --set redis.password.secretName=
refuses "an unencrypted Redis connection is refused" "redis.tls.enabled=true is required"    $S --set redis.tls.enabled=false --set redis.tls.secretName= --set redis.tls.clientCert=false
refuses "Redis TLS and a mesh are exclusive"        "choose one"                             $S --set redis.tls.mesh=true
refuses "Redis TLS options without TLS"             "have no effect without redis.tls.enabled" $S --set allowInsecure=true --set redis.tls.enabled=false
refuses "a Redis client certificate needs a Secret" "redis.tls.clientCert needs"             $S --set redis.tls.secretName=
refuses "a Redis user needs a password"             "redis.username needs"                   -f ci/dev.yaml --set redis.username=app
refuses "no redis address"                         "redis.addr is required"                  $S --set redis.addr=
refuses "provisioning auth must be chosen"        'provisioning.auth must be set'           $S --set provisioning.auth=
refuses "auth off needs allowInsecure"             "provisioning.auth=off is refused"        $S --set-string provisioning.auth=off --set provisioning.clients=null --set provisioning.privileged=null
refuses "mtls needs a certificate secret"          "provisioning.tls.secretName"             $S --set provisioning.tls.secretName=
refuses "mtls needs named callers"                 "provisioning.clients and/or"             $S --set provisioning.clients=null --set provisioning.privileged=null
refuses "named callers with auth off are refused"  "have no effect with auth=off"            -f ci/dev.yaml --set provisioning.clients={crm}
refuses "provisioning needs a currency"            "provisioning.currency is required"       $S --set provisioning.currency=
refuses "provisioning needs kafka"                 "kafka.brokers is required"               $S --set kafka.brokers=
refuses "catalog needs publishers"                 "catalog.publishers"                      $S --set catalog.publishers=null
refuses "catalog auth must be chosen"              "catalog.auth must be set"                $S --set catalog.auth=
refuses "nchf oauth must be chosen"                "gateway.nchf.oauth must be"              $S --set gateway.nchf.oauth=
refuses "nchf oauth off needs allowInsecure"       "gateway.nchf.oauth=off is refused"       $S --set-string gateway.nchf.oauth=off
refuses "nchf oauth needs the NRF keys"            "oauthKeys.secretName"                    $S --set gateway.nchf.oauthKeys.secretName=
refuses "nchf must not run in clear"               "bearer tokens must not cross"            $S --set gateway.nchf.tls.secretName=
refuses "client certs need the tls secret"         "requireClientCert needs"                 $S --set gateway.nchf.tls.secretName= --set gateway.nchf.tls.mesh=true
refuses "gateway needs a diameter realm"           "gateway.diameter.realm is required"      $S --set gateway.diameter.realm=
refuses "gateway needs an origin host suffix"      "originHostSuffix is required"            $S --set gateway.diameter.originHostSuffix=
refuses "catalog tariff needs a default plan"      "gateway.tariff=catalog needs"            $S --set gateway.defaultPlan=
refuses "control plane auth must be chosen"        "gateway.control.auth must be"            $S --set gateway.control.auth=
refuses "control plane needs someone allowed"      "nobody would be allowed in"              $S --set gateway.control.agents=null --set gateway.control.approvers=null
refuses "running without replicas needs allowInsecure" "redis.allowNoReplica=true needs"     $S --set redis.allowNoReplica=true
refuses "network policy needs allowed clients"     "networkPolicy.allowFrom is required"     $S --set networkPolicy.allowFrom=null
refuses "network policy needs a scrape source"     "networkPolicy.monitoringFrom is required" $S --set networkPolicy.monitoringFrom=null
refuses "mediation needs a source"                 "mediation needs a source"                $S --set mediation.input.enabled=false --set mediation.kafka.enabled=false
refuses "catalog rating needs a default plan"      "mediation.rating=catalog needs"          $S --set mediation.defaultPlan=
refuses "aiops auth must be chosen"                "aiops.auth must be"                      $S --set aiops.auth=
refuses "a misspelt value is an error"             "Additional property"                     $S --set gateway.replica=3
refuses "an unquoted off (a YAML boolean) is an error" "Expected: string, given: boolean"    $S --set provisioning.auth=false

# Properties of what is rendered. Pod specs only: awk keeps the workload documents.
count() { grep -cE -- "$2" <<<"$1" || true; }
workloads=$(awk 'BEGIN{RS="---\n"; ORS="---\n"} /kind: (Deployment|StatefulSet)/' <<<"$secure")
n=$(count "$workloads" '^kind: (Deployment|StatefulSet)')
[ "$n" -eq 6 ] && ok "6 workloads rendered (gateway, provisioning api and worker, catalog, aiops, mediation)" || bad "expected 6 workloads, got $n"
for prop in 'runAsNonRoot: true' 'readOnlyRootFilesystem: true' 'allowPrivilegeEscalation: false' 'automountServiceAccountToken: false' 'type: RuntimeDefault'; do
  got=$(count "$workloads" "$prop")
  [ "$got" -ge "$n" ] && ok "every workload sets: $prop" || bad "$prop on $got of $n workloads"
done
grep -qE 'cpu: *"?[0-9a-z]+"?$' <<<"$(awk '/limits:/{f=1;next} /requests:|^[^ ]/{f=0} f' <<<"$workloads")" && bad "a CPU limit is set (throttling hurts latency)" || ok "no CPU limits"
grep -q 'kind: StatefulSet' <<<"$secure" && grep -A3 'kind: StatefulSet' <<<"$secure" | grep -q . && ok "mediation is a StatefulSet" || bad "mediation StatefulSet"
st=$(awk 'BEGIN{RS="---\n"} /kind: StatefulSet/' <<<"$secure")
grep -qE '^  replicas: 1$' <<<"$st" && ok "mediation runs exactly one pod (its ledger is per output directory)" || bad "mediation replicas"
# the admin port is only reachable through the -admin Services
svcs=$(awk 'BEGIN{RS="---\n"} /kind: Service/ && !/-admin/' <<<"$secure")
grep -q 'port: 8080' <<<"$svcs" && bad "a client-facing Service exposes the admin port" || ok "client-facing Services never expose the admin port"
# Redis credentials reach the pods as files from Secrets, never as environment values
grep -q 'OCS_REDIS_PASSWORD_FILE' <<<"$workloads" && ! grep -qE 'name: OCS_REDIS_PASSWORD[,}]' <<<"$workloads" && ok "the Redis password is a mounted file, not an environment value" || bad "the Redis password is exposed as an environment value"
n_redis=$(count "$workloads" 'name: redis-password$')
[ "$n_redis" -ge 5 ] && ok "every Redis-using workload mounts the password Secret ($n_redis volumes)" || bad "expected the password Secret on 5 workloads, got $n_redis"
aiops=$(awk 'BEGIN{RS="---\n"} /kind: Deployment/ && /name: r-aiops$/' <<<"$secure")
grep -q 'redis' <<<"$aiops" && bad "AI Ops (no Redis) mounts Redis credentials" || ok "AI Ops, which does not use Redis, gets no Redis credentials"
grep -qiE 'secretKey|apiKey|BEGIN (RSA|EC )?PRIVATE KEY' <<<"$secure$dev" && bad "something secret-looking is in the rendered output" || ok "no secret material in the rendered output"

exit $fail
