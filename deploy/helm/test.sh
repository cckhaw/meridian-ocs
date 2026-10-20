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
grep -qiE 'password|secretKey|apiKey|BEGIN (RSA|PRIVATE)' <<<"$secure$dev" && bad "something secret-looking is in the rendered output" || ok "no secret material in the rendered output"

exit $fail
