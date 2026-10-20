# Durability: replica-acknowledged commits

Code: `pkg/datalayer/durability.go`, `pkg/datalayer/redistest` (primary + replica test harness), wired in `services/ocs-signaling-gateway/main.go`.

## The risk

Redis replicates asynchronously. If the primary dies right after answering, a write the network element already believes in can be missing on the promoted replica, and AOF `everysec` can lose about a second more. This was risk R1 in doc 01; the decision is to **wait for at least one replica to confirm commits**.

## What waits

| Step | Waits? | Why |
|---|---|---|
| Initial reservation | no | If the reservation is lost, the final settlement finds none, charges the reported usage as over-use, and the balance still ends correct. |
| Update, terminate, event | **yes** | These commit usage. |
| Provisioning writes (top-ups, adjustments, lifecycle) | **yes** | A paid top-up that vanishes is the worst outcome. |
| Expiry sweep | no | Idempotent, retried. |

`WAIT` only covers writes made on its own connection, so the script and the `WAIT` are sent in one pipeline. A `NOSCRIPT` reply (script not yet cached) is retried with `EVAL`, still followed by `WAIT`.

## What a shortfall means

If fewer replicas than required acknowledge within the timeout (default 25 ms), the call still returns the business result and sets `Response.Replication = Short`:

- The write **is applied on the primary**. Returning an error would make the caller retry or fall back to the fail policy for a charge that already happened; the stored answer would be replayed anyway.
- The exposure is exactly the one the wait was meant to close: the write could be lost if the primary fails before the replica catches up.
- It is counted: `datalayer.DurabilityStats().Short` and `engine.Stats().Unreplicated`.

**Suspension.** A dead replica would otherwise add the full timeout to every commit and wreck the latency budget. After 5 consecutive shortfalls, waiting is suspended for 5 s (commits stay fast, are marked `Short` and counted as `Suspended`), then one probe waits again. Both numbers are configurable. The consequence to accept: **while suspended, commits are not durable**. Alert on a rising `Short` / `Suspended` count.

## Connecting to Redis: credentials and TLS

Balances live in Redis, so a connection that anyone on the network can open, or read, is a
hole in everything above. Every service that uses Redis (gateway, provisioning, catalog, mediation)
takes the same settings (`pkg/svcenv.ApplyRedisAccess`):

| Variable | Meaning |
|---|---|
| `OCS_REDIS_USERNAME` | ACL user (Redis 6+); empty = the default user |
| `OCS_REDIS_PASSWORD` or `OCS_REDIS_PASSWORD_FILE` | the password, inline or from a file (prefer the file: mount a Secret). Not both. The file is **read again for every new connection**, so a rotated password needs no restart: change it on Redis (`ACL SETUSER ... >new`) and in the file, and the pool picks it up as connections are replaced |
| `OCS_REDIS_TLS` | `on` or `off` (default `off`) |
| `OCS_REDIS_TLS_CA` | PEM file of the CA that signed the server certificate (default: system roots) |
| `OCS_REDIS_TLS_CERT`, `OCS_REDIS_TLS_KEY` | client certificate for a Redis that requires one (`tls-auth-clients yes`); re-read for every handshake, so a renewed certificate needs no restart |
| `OCS_REDIS_TLS_SERVER_NAME` | name to verify in the server certificate (default: the host of `OCS_REDIS_ADDR`) |

Server verification is never switched off (there is no "skip verify"). Settings that would have no
effect are errors, in the same spirit as the listeners' `AUTH` settings: TLS options with TLS off, a
username without a password, both a password and a password file. A start-up check
(`CheckRedisAccess`) fails the process, with the reason, when Redis refuses its credentials
(`NOAUTH`, `WRONGPASS`, `NOPERM`), when TLS verification fails, or when the server closes the
connection three times in a row before answering (what a TLS-only Redis does to a plaintext client);
an **unreachable** Redis is not an error there, because the gateway must still start during an
outage to serve its fail policy. Because a TLS handshake takes longer than the 200 ms the gateway
allows a plain connect, `DialTimeout` is raised to 1 s when TLS is on (connections are pooled, so this
is off the hot path). The password is never logged; the start-up line says only *how* the connection
is protected (`redis access: user ocs (password from a file), TLS with a client certificate`) and warns
when it is neither authenticated nor encrypted.

Verified against a real `redis-server` with `requirepass`, a named ACL user with the default user
disabled, TLS-only listeners and `tls-auth-clients yes`: no password, wrong password and wrong user
are refused; a plaintext client cannot talk to a TLS server; a server certificate from another CA, an
untrusted CA and a wrong server name are refused; a client certificate from another CA is refused;
a rotated password file is picked up; a service-level test per service proves it uses the settings and
refuses to start otherwise (mutation-checked: skipping the settings makes each fail). Also run through
the Docker Compose stack, where Redis requires a password and TLS, replication runs over TLS and the
replica acknowledgements the gateway waits for still arrive.

## Startup check

The gateway reads `INFO replication` and **refuses to start** if fewer than `OCS_REDIS_REPLICAS` (default 1) replicas are connected, unless `OCS_ALLOW_NO_REPLICA=true` (development only, logs a warning). `OCS_REDIS_REPLICAS=0` turns waiting off (logs a warning).

## Measured (this sandbox, loopback, serial, not a benchmark environment)

`Charge` update path: ~132 µs without waiting, ~241 µs with one replica acknowledging, i.e. about **+110 µs per commit** here. Across availability zones the replica round trip is larger; the 10 ms P99 budget in doc 01 allocates 3.9 ms of margin, which this fits, but it must be re-measured on the real network.

## Verified

- A frozen replica process (SIGSTOP) makes a commit report `Short` after the timeout; after SIGCONT and catch-up the commit is `Acked` and the replica holds the same balance as the primary. Ignoring `WAIT`'s result makes three tests fail (mutation check).
- Under load on the local stack (`dev/chaos.sh`, [14](14-load-test.md)): a replica frozen for 8 s caused no failed charging sessions and no latency change beyond one 0.2 s blip, while about 3,100 commits skipped the wait during suspension (`ocs_durability_suspended_total`); every balance reconciled afterwards. A SIGKILLed Redis primary, restarted, lost no acknowledged commit in 4 runs (not a proof; there is no failover in the stack).
- A killed replica: shortfalls, then suspension (commits fast again), then a probe after the suspension. Business results are unaffected throughout.
- Honest limit of the tests: on loopback a replica is practically never behind even without `WAIT` (measured 0 of 300 commits), so a "the replica already has it" assertion cannot distinguish `WAIT` from no `WAIT`. The frozen-replica test is the one that does.
- Startup behaviour of the built binary: refuses without a replica, starts with the override (warning), starts with a replica (logs the wait).

## Not covered

1. **Redis Cluster**: `WithDurability` returns `ErrClusterWait`. go-redis's cluster client cannot pin a keyless `WAIT` to the primary that owns the slot, so a different approach (per-node clients) is needed before Cluster is used.
2. **Not a consensus guarantee.** `WAIT` shows the replica received the write, not that it fsynced it, and a failover can still promote a replica that is behind if the acknowledging replica is not the one chosen. Use `min-replicas-to-write`, sentinel/failover settings that prefer the most up-to-date replica, and AOF on the replicas.
3. The fail-policy journal and the audit stream have their own durability story (doc 02); this change covers balances and sessions only.
4. Cross-region replication (doc 01 §6) is asynchronous by design and is not covered by this wait.
