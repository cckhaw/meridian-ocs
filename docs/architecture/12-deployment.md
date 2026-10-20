# Deployment (Helm)

The chart is `deploy/helm/meridian-ocs`. One release installs the gateway,
provisioning (API and workers), the catalog service, offline mediation and,
optionally, the AI Ops layer. **It does not install Redis or Kafka**: the hot
state store and the bus are run by their own operators or as managed services,
with the durability settings in [07](07-durability.md), and the chart only
points at them (`redis.addr`, `kafka.brokers`).

## Principles

- **No silent security defaults.** The services refuse to start without an
  explicit `...AUTH` choice; the chart refuses to render the same way. Every
  authentication mode (`provisioning.auth`, `catalog.auth`, `aiops.auth`,
  `gateway.nchf.oauth`, `gateway.control.auth`) must be set, `"off"` is rejected
  unless `allowInsecure: true`, and a restriction list that would have no effect
  (named callers with auth off) is an error, as in the services. The Nchf listener
  must have TLS (a Secret, or `tls.mesh: true` when a mesh terminates it) because
  OAuth tokens are bearer credentials.
- **Redis is never open.** `redis.password.secretName` is required, and so is
  `redis.tls.enabled` (or `redis.tls.mesh` when a mesh encrypts the connection), unless
  `allowInsecure`. The password is mounted as a file and re-read for every new connection
  (rotation needs no restart); the TLS Secret carries `ca.crt` and, with `redis.tls.clientCert`,
  `tls.crt` / `tls.key`. Options that would have no effect are refused. The password never appears
  as an environment value (checked by `test.sh`). Details: [07](07-durability.md#connecting-to-redis-credentials-and-tls).
- **Two-person approval is a stated choice.** `provisioning.approval.policy` is required (`"off"` or rules such as
  `adjust_balance>5000,change_lifecycle:terminated,remove_bucket`); the chart checks the rule syntax, refuses a
  policy with mutual TLS unless at least two different callers can approve, refuses `allowSelf` with mutual TLS,
  refuses approvers or `allowSelf` with `"off"`, and gives the API and worker pods the same policy (the worker
  enforces it too). With policy `"off"` the install notes say so. Details: [08](08-provisioning.md#two-person-approval).
- **No secrets in values.** Certificates and keys are Kubernetes Secrets you create
  (cert-manager works: `tls.crt`, `tls.key`, `ca.crt`) and reference by name, mounted
  read-only. The NRF's token-signing certificate is a Secret too.
- **Typos are errors.** `values.schema.json` is generated from `values.yaml` with
  `additionalProperties: false`, enums for modes and Service types, and numeric
  bounds. An unquoted `off` is a YAML boolean and is rejected with a type error
  (write `"off"`).
- **Same checks as the services.** Every setting maps to the service's own
  environment variable, and the rendered environment of every workload was run
  against the real binaries (below).

## What is rendered

| Component | Kind | Notes |
|---|---|---|
| gateway | Deployment + HPA + PDB | Diameter and Nchf in separate Services (`LoadBalancer` for peers outside the cluster); each pod is its own Diameter identity `<pod>.<originHostSuffix>` with `OCS_HOST_IP` from the pod IP; optional control-plane Service |
| provisioning | two Deployments (`api`, `worker`) or one (`split: false`) | the API scales on requests, workers on Kafka partitions; REST and gRPC Service |
| catalog | Deployment + Service | |
| mediation | **StatefulSet, one pod** + PVC | its exactly-once ledger is per output directory; config (mapping, CDR mapping) from a ConfigMap, mounted as a directory so a change hot-reloads |
| aiops | Deployment + Service | optional |
| all | ServiceAccount (no token mounted), `<name>-admin` Services, optional NetworkPolicy and ServiceMonitor | |

Every pod: non-root, read-only root filesystem, all capabilities dropped,
`seccompProfile: RuntimeDefault`, a 64 MiB `/tmp`, no CPU limit (throttling shows
up as latency on a charging path; memory is limited), topology spread over nodes and
zones, `maxUnavailable: 0` rolling updates, a slow scale-in (600 s) for the gateway
because connections and sessions are sticky. Probes use the services' own
`/healthz` and `/readyz` (503 until Redis is connected and the listeners are bound,
and again while draining). The admin port (health and metrics) is exposed only by
the `-admin` Services, never by a client-facing one.

## Observability

Every service serves `/metrics` (Prometheus text) on its admin port; the chart can
render a `ServiceMonitor`, a `PrometheusRule` (`alerts.enabled`) and a Grafana
dashboard ConfigMap (`dashboard.enabled`, for the Grafana sidecar). The gateway and
mediation already exported metrics ([11](11-aiops.md)); this part adds the rest:

- **Provisioning**: submissions by result (`accepted`, `duplicate`, `rejected`,
  `conflict`, `unavailable`), accepted by command type, commands finished by outcome,
  dead-lettered messages, worker retries and commits, **command lag** (acceptance to
  recorded outcome, a histogram), API requests by status class and their duration,
  and the data-tier durability counters (provisioning moves money too); with `provisioning.graceSweep` set, the
  grace-expiry counters and the overdue gauges ([08](08-provisioning.md#the-end-of-a-grace-period); no alert rule or
  dashboard panel uses them yet, and the value was added without rendering the chart because the sandbox has no `helm`).
- **Catalog** (every node that reads it: gateway, provisioning, catalog service,
  mediation): version in effect, latest version, **seconds since the shared catalog
  was last read**, replication errors; the catalog service adds publishes by result
  and API requests.
- **Shared**: Go runtime gauges. No path, subscriber key, MSISDN, certificate name or
  author is ever a label (checked by tests): labels are bounded enums (result,
  outcome, type, code class).

Alerts (35, six groups; thresholds in `alerts.*`): scrape target down; charging P99
above 10 ms (warning) and 50 ms (critical); requests answered by the fail policy;
breaker open, or forced open for 30 minutes; **commits acknowledged by fewer replicas
than required** (the durability promise); replica wait suspended; **usage released without being charged** (a terminate acknowledged while the data tier was down), **not draining** from the gateway's spool, **lost**, and **left to a person** (a refused update whose usage could be charged twice); load shedding; audit
dropped; identity directory errors and conflicts; usage the catalog cannot price;
catalog sync stale or failing, **no version in effect**, **nodes on different versions**;
catalog publish errors and denied publishes; a request **waiting too long for approval**, an attempted self-approval, approval events not published; provisioning stalled (commands accepted
but none finishing), lag, dead letters, worker retries, API 5xx, commands that cannot
be queued; mediation reject share, ledger commits not replicated, mapping rejected.
Rules are scoped to the release by the admin-Service job name and namespace, but
metric names are not release-labelled: two releases scraped into one Prometheus mix in
the aggregate alerts.

## Installing

- **Losing revenue is a stated choice too.** `gateway.unsettledSpool.mode` is required: `emptyDir` gives each gateway
  pod a volume for the usage it released while Redis was down (charged when Redis returns, [03](03-rating-engine.md#usage-released-while-the-data-tier-is-down-and-its-reconciler)),
  `off` discards that usage and is refused unless `allowInsecure`. An `emptyDir` survives a container restart, not the loss of the pod or node.

1. Redis (primary plus the replicas you configure) and Kafka exist, and the
   Secrets are created.
2. Install with `--set image.repository=... --set image.tag=...` and your values
   (see `ci/secure.yaml` for a complete production-shaped example and
   `ci/dev.yaml` for a throwaway one).
3. **Publish a first catalog version** (catalog service or editor). With
   `gateway.tariff=catalog` or `mediation.rating=catalog` the gateway and mediation
   refuse to start until one exists, by design: pricing on a missing catalog is
   worse than a pod that is not ready. Helm does not model this ordering; the pods
   restart until it is satisfied.

Images are built per service: `docker build -f build/Dockerfile --build-arg
SERVICE=ocs-signaling-gateway .`

## Verification

`deploy/helm/test.sh` (CI job `helm`, helm and kubeconform pinned and
checksum-verified; helm, kubeconform and promtool): lint and render both value sets; kubeconform on the output; 28
deliberate refusals each checked for the right reason (missing image, auth not
chosen, `off` without `allowInsecure`, mTLS without a Secret or named callers,
Nchf in clear, OAuth without keys, control plane with nobody allowed in,
`allowNoReplica`, NetworkPolicy with empty lists, typos, an unquoted `off`, ...);
`promtool check rules` and **15 alert unit-test scenarios** (the alerts fire on synthetic
series when they should and stay quiet when they should: degraded serving,
under-replicated commits, provisioning stalled vs keeping up, catalog version skew,
slow vs fast charging, approvals waiting too long, self-approval attempts); a check that **every metric name in the rules and dashboard is
registered in the Go code**; the generated `values.schema.json` and `dashboard.json`
are current (`gen-schema.py`, `gen-dashboard.py`); and properties of the rendered pods (non-root, read-only filesystem, no privilege
escalation, no token, seccomp, no CPU limits, mediation single pod, admin port not on
client Services, no secret material). Mutation-checked: allowing `off` without
`allowInsecure`, a writable root filesystem and Nchf in clear each make it fail.

Beyond static checks, the rendered environment of every workload of both value
sets was run against the **real binaries** (with generated certificates, a real
Redis primary and replica): all six start and answer `/readyz`. That check found
that a placeholder mapping in the example values was rejected by the service
(`mapping: 1 problem(s)`), and confirmed the install-order behaviour above (gateway
and mediation exit with "no catalog version is in effect" until one is published).
It was done by hand with a throwaway script, not in CI. The same run scraped `/metrics`
from all of them and confirmed that **every metric the alerts and dashboard use is
actually emitted** by the real binaries (the CI check is the cheaper static version).

## Known gaps

1. **Never installed in a cluster.** There is no Kubernetes cluster in the
   development environment: manifests are schema-valid and the services accept the
   environment, but scheduling, Services, LoadBalancers, probes and rollouts have not
   been exercised.
2. **No Redis or Kafka** in the chart, no Redis Sentinel/operator integration, and
   nothing makes the gateway's replica requirement visible to the scheduler (it
   refuses to start until the replicas are connected).
3. **NetworkPolicy covers ingress only**; egress to Redis, Kafka and the NRF is not
   restricted. Policy peers are your input.
4. **Mediation is a single pod**; there is no failover of its volume beyond what the
   storage class gives, and no SFTP pull or sidecar for delivering CDR files into
   `/data/in`.
5. **HPA scales on CPU only**; no custom metrics (TPS, in-flight) and no KEDA for
   workers. **Kafka consumer lag is not exported**: the stalled-provisioning alert
   infers it from accepted-versus-finished rates and the command-lag histogram, which
   only observes commands that finish. gRPC calls are not counted (REST is). No traces
   (OpenTelemetry), and the dashboard and alerts have been validated by tooling, not
   looked at in a running Grafana and Prometheus.
6. **No mesh topology, no multi-region.** Active-active across regions is not built.
7. **The standalone rating-engine and data-layer services** are still scaffolds and
   are not in the chart (the gateway runs the engine in-process).
8. **Images are built and smoke-tested in CI** (job `stack`, [13](13-local-stack.md)) but not
   published to a registry, signed or scanned.
9. **Redis access control is coarse.** One user and password are shared by every service in a
   release; there is no least-privilege ACL profile per service (for example a catalog user that
   cannot touch balances), and a rotated *CA* certificate (unlike the password and client
   certificate) is read at start-up only. Sentinel and Cluster addressing are not supported.
