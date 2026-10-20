# Local stack (Docker Compose)

`docker-compose.yml` runs the whole system on one machine for development and
demonstration: Redis (a primary **and** a replica, so commits really wait for an
acknowledgement), Redpanda (the Kafka API) and the gateway, provisioning, catalog,
mediation and AI Ops services, each built from `build/Dockerfile`.

**For development only.** The service listeners run with authentication off (the services
allow that only when told so explicitly, as the compose file does), nothing is backed
up, and every published port is bound to `127.0.0.1`. Redis is protected as in production:
it needs a password and only speaks TLS (replication included), using a throwaway CA that
the `certs` job generates into a volume on first start, so the services' Redis access control
([07](07-durability.md#connecting-to-redis-credentials-and-tls)) is exercised. The development
password is public (`OCS_DEV_REDIS_PASSWORD` overrides it): never reuse it. Production uses
mTLS, Secrets and the Helm chart ([12](12-deployment.md)).

```sh
docker compose up --build      # first start compiles five services; later starts take seconds
dev/smoke.sh                   # provisions a subscriber, charges it over Nchf, checks mediation and metrics
docker compose down -v         # stop and forget everything
```

`make stack-up`, `make stack-smoke` and `make stack-down` do the same. It needs Docker
with Compose v2 and about 2 GB of memory. Images are built for the host architecture,
so it runs natively on Apple Silicon.

## What starts, and in which order

| Service | Image | Purpose | Port on this machine |
|---|---|---|---|
| `certs` | `alpine/openssl` | generates a throwaway CA and a Redis server certificate once, then exits | none |
| `redis`, `redis-replica` | `redis:7.4-alpine` | balances, sessions, identity directory, catalog; password and TLS only | none |
| `redpanda`, `topics` | `redpandadata/redpanda:v26.1.18` | Kafka API; `topics` creates the three provisioning topics (3 partitions) and exits | 19092 (Kafka API) |
| `catalog` | built | catalog REST and editor | 8444 (`/ui/`) |
| `seed` | `curlimages/curl` | publishes `dev/seed-catalog.json` once (plans `basic`, `gold`; offerings `basic-offer`, `gold-offer`), then exits; leaves an existing catalog alone | none |
| `provisioning` | built | REST + console, gRPC, command workers; **two-person approval on** for corrections above 1000 and for terminations, with self-approval allowed because authentication is off (try it in the console: the *Approvals* tab) | 8443 (`/ui/`), 9443 |
| `gateway` | built | Diameter Gy and Nchf, rating engine in-process; tariff from the catalog, default plan `basic` | 3868, 8000 |
| `mediation` | built | offline CDR pipeline; input and output on the `mediation-data` volume | none |
| `aiops` | built | mapping workbench | 8445 |

Admin ports (`/healthz`, `/readyz`, `/metrics`): gateway 9080, provisioning 9081,
catalog 9082, mediation 9083, aiops 9084.

Start-up order is enforced where it matters: the replica must be healthy (the gateway and
provisioning refuse to start without the replica they wait for), the topics must exist, and
the gateway and provisioning wait for the catalog to be seeded (the gateway refuses to start
with `OCS_TARIFF=catalog` until a version exists). The services themselves restart on
failure, so a slow dependency only costs a retry.

## Trying it

- **Consoles**: <http://127.0.0.1:8443/ui/> (find, create, top up, change offering, correct
  a balance, change status) and <http://127.0.0.1:8444/ui/> (offerings, price plans, publish).
- **Charge it**: `dev/smoke.sh` shows the calls; the Nchf request it sends is a normal
  `POST /nchf-convergedcharging/v3/chargingdata` over HTTP/2 without TLS
  (`curl --http2-prior-knowledge`).
- **Mediation**: copy a CDR file into the input directory and look at the metrics and the output:
  ```sh
  chmod 644 my.jsonl                       # the service runs as a non-root user and must be able to read it
  docker compose cp my.jsonl mediation:/data/in/my.jsonl
  curl -s localhost:9083/metrics | grep ocs_mediation_records_total
  ```
  Output goes to `/data/out` (`cdr/`, `suspense/`, `rejects/`) on the volume. A file the service
  cannot read is retried and logged (`permission denied (will retry)`), not dropped.
- **Kafka from the host**: `rpk -X brokers=localhost:19092 topic consume ocs.provisioning.events`.

## Verification

Run for real with Docker (daemon and Compose v5 in the development sandbox, linux/amd64): the five images
build through the real Dockerfile (15 to 30 MB each) and the whole stack starts with the
ordering above; `dev/smoke.sh` passes (14 checks): all services ready, a catalog version in
effect, subscriber created, offering attached and topped up through Kafka and the workers,
found by MSISDN, Nchf quota granted from the offering's bundle at the catalog's price with the
balance moving (10 GiB bundle: -1,000,000 on grant, +750,000 back when 250,000 were used and the
session was released), a correction of -2000 held for approval with the balance unchanged and then applied after approval (the Kafka events topic shows `approval.requested`, `approval.approved` and `balance.modified` with `approvedBy`), a CDR enriched by mediation, and metrics reflecting the traffic. A
restart (`down` then `up`, volumes kept) keeps the subscriber and does not reseed the catalog.
The same run was repeated with Redis behind a password and TLS (replication over TLS with the
replica acknowledgement still arriving; a plaintext client gets an I/O error and a client without the
password gets `NOAUTH`). The CI job `stack` repeats the build and the smoke test on every change.

Found while verifying: the non-root service could not write a fresh named volume (fixed in the
Dockerfile, which now creates `/data` owned by the service user), and a CDR file copied in
with owner-only permissions is unreadable by the service (documented above).

## Known gaps

1. **The service listeners have authentication off** in this stack; it does not exercise mTLS, OAuth,
   the privileged/standard caller split or the cross-origin checks (those are covered by tests and
   the chart). Redis does use a password and TLS, but no client certificate or ACL user.
2. **Single Redis primary and replica with no failover**: stopping the primary stops charging.
   There is no Sentinel, so the durability and failover behaviour is only what `WAIT` gives.
3. **No monitoring stack** (Prometheus, Grafana): `/metrics` is exposed, the chart's alert
   rules and dashboard are not part of the compose file.
4. **Not run on Apple Silicon**: the images, Redis and Redpanda all publish arm64 builds and
   the Go code is pure Go, but it was verified here on linux/amd64 only.
5. **Kafka is a single Redpanda node** in development mode with one replica per topic.
6. **Each service image copies the whole repository** into its build stage (the Go module and
   build caches are shared between builds through BuildKit cache mounts, so repeat builds are quick).
