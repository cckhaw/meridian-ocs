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

## Installing

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
checksum-verified): lint and render both value sets; kubeconform on the output; 28
deliberate refusals each checked for the right reason (missing image, auth not
chosen, `off` without `allowInsecure`, mTLS without a Secret or named callers,
Nchf in clear, OAuth without keys, control plane with nobody allowed in,
`allowNoReplica`, NetworkPolicy with empty lists, typos, an unquoted `off`, ...);
and properties of the rendered pods (non-root, read-only filesystem, no privilege
escalation, no token, seccomp, no CPU limits, mediation single pod, admin port not on
client Services, no secret material). Mutation-checked: allowing `off` without
`allowInsecure`, a writable root filesystem and Nchf in clear each make it fail.

Beyond static checks, the rendered environment of every workload of both value
sets was run against the **real binaries** (with generated certificates, a real
Redis primary and replica): all six start and answer `/readyz`. That check found
that a placeholder mapping in the example values was rejected by the service
(`mapping: 1 problem(s)`), and confirmed the install-order behaviour above (gateway
and mediation exit with "no catalog version is in effect" until one is published).
It was done by hand with a throwaway script, not in CI.

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
   workers (Kafka lag).
6. **No mesh topology, no multi-region.** Active-active across regions is not built.
7. **The standalone rating-engine and data-layer services** are still scaffolds and
   are not in the chart (the gateway runs the engine in-process).
8. **Images are not built or published** by CI; the Dockerfile exists but is
   untested here (no container runtime in the development environment).
