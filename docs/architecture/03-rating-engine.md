# Rating Engine & Atomic Reservation Scripts

Code: `pkg/datalayer` (Lua + Go client), `pkg/canonical`, `pkg/engine` (shared so the gateway can run it in-process).

## What is implemented

| Piece | Where | Notes |
|---|---|---|
| Atomic charge script | `pkg/datalayer/lua/charge.lua` | One call = settle usage, refund unused, reserve next slice, session state, stored answer, ledger intent |
| Expiry sweep | `lua/sweep.lua` + opportunistic sweep at the top of every charge | Refunds reservations of silently dropped sessions in full |
| Go client | `pkg/datalayer/client.go` | Bounds validation, pooled argument slices, reply decoding |
| Canonical model | `pkg/canonical` | Pooled `Request` / `Response` |
| Engine | `internal/engine` | Subscriber-hash partitioned workers, per-worker scratch buffers, load shedding, timing wheel for sweeps, fail-policy integration |

Not implemented yet: durable audit/journal drains, Redis Cluster deployment, the provisioning-side balance commands.

## Charge script semantics

Modes: `I` initial, `U` update, `T` terminate, `E` event.

1. **Sweep** expired reservations of this subscriber (refund in full).
2. **Idempotency.** `(session, request number)` mutates state at most once. A retransmit returns the stored answer (`code=1`) with no state change. A lower number than the last processed is `stale`; an update/terminate for an unknown session, **or for one that has already been terminated**, is `unknown session` (a terminate marks the session closed, so a later request with a higher number can no longer reserve quota on it).
3. **Per rating group:**
   - *Settle* the open reservation: if `used < reserved`, refund the unused tail (newest debit first, exact inverse of the rate); if `used > reserved` (or the reservation had already expired), charge the extra from the buckets and report anything unchargeable as `shortfall`.
   - *Reserve* the next slice (`I`/`U`): consume buckets in `P:ord` order (promo, then bundle, then cash by convention of the `Order` score), skipping buckets of the wrong unit kind or past expiry. Partial grants set `final`.
   - *Event* (`E`) is all-or-nothing: a partial debit is rolled back.
4. A terminate with no usage items releases every open reservation of the session.
5. Store the answer, set the session TTL, append a ledger-intent entry to a capped per-subscriber stream.

Pricing is `cost = ceil(units × num / den)` per bucket class, so a 1.5/unit cash rate rounds in the operator's favour and refunds invert exactly.

## Numeric limits (important)

Redis Lua numbers are IEEE doubles. The client enforces: units ≤ 2^40, rate numerator ≤ 2^20, denominator ≤ 2^12, so every intermediate product stays below 2^52 and is exact. Promo/bundle (1:1) take a fast path with no multiplication. Balances above 2^53 minor units are not representable.

## Concurrency model

- All requests for a subscriber hash to one worker goroutine, so the process holds no subscriber lock and a subscriber's requests are processed in arrival order within one engine.
- Correctness across processes/regions does **not** rely on that: the Lua script is the atomicity boundary, and the concurrency tests hammer one subscriber from many goroutines.
- Workers are partitioned by channel; they are not pinned to OS threads or cores (Go does not offer that without `LockOSThread` + affinity; revisit if profiling shows it matters).
- A full worker queue returns `ErrOverloaded` immediately (gateway maps it to 3004 / 429). Queues never grow.

## Reservation lifetime

Each granted reservation is registered in the worker's timing wheel (1 s slots) to be swept after `Validity + Grace + 1 s`. The sweep is idempotent and re-armed if Redis errors or more than 64 are due. If the engine restarts, the wheel is lost; the opportunistic sweep at the start of the subscriber's next charge call recovers the funds, but until then they stay reserved. A global background sweeper (for subscribers that never return) needs a per-shard index of open reservations and is still open.

## Fail policy integration

The engine wraps the data-tier call in `failpolicy.Breaker`. When the call fails or the breaker is open, requests go to `failpolicy.Engine` (see doc 02): prepaid are denied (`ResultUnavailable`), postpaid get a capped, journalled, short grant (`Degraded=true`).

### Usage reported while the data tier is down, and its reconciler

A **terminate** (Gy CCR-T, Nchf release) that arrives during an outage is still acknowledged, so the network element can free the session. An **update** is refused (prepaid fail-closed). Both carried usage ("since the last measurement") that cannot be settled now, and that the network element will not report again: the reservation would expire and be refunded and the usage would never be charged. The failure-injection runs in [14](14-load-test.md) measured it (151 terminates' usage lost in one 8 s Redis stop under load), so it is handled:

1. **Record.** Before answering, the engine hands the request to an `UnsettledSink` (`pkg/unsettled.Spool`): a local append-only log, fsynced per record, in the directory `OCS_UNSETTLED_SPOOL`. The data tier cannot hold the record, since it is the thing that is down. Only requests that reported usage are recorded; a retransmission is the same record. For a refused update the record also says whether the call **provably never reached Redis** (the breaker was open, or the connection could not be made) or its **outcome is unknown** (a call was made and failed). If no sink is configured or the write fails, the usage is counted as **lost** (`ocs_engine_unsettled_unrecorded_total`).
2. **Replay.** `unsettled.Reconciler` (in every gateway) calls `Engine.Settle` for the oldest records first, every 2 s, and stops at the first one the data tier cannot take (it respects the circuit breaker and never answers by the fail policy).
   - A **terminate** is resubmitted as it was, **with the same session and request number**.
   - A **refused update** is settled after the fact without reserving anything (`datalayer.ModeLate` / `ModeLateUnapplied` in `charge.lua`): the open reservation of the rating group is settled against the reported usage, as an update would, and the answer stored for that request number is a refusal.
3. **Why that cannot charge twice.** The charge script makes `(session, request number)` idempotent. For a terminate: if it had in fact reached Redis (an unknown outcome) the replay is answered from its stored result and charges nothing; if not, it settles the reservation or, when the reservation was swept meanwhile, charges the usage fresh against the buckets. For a refused update, a marker key per `(session, request number)` makes a second submission a no-op, and:
   - **never sent** (`N`): charged whatever request number the session has reached since. The usage was reported once, to us, and nowhere else. This is the case when the breaker was open, which is nearly every refused update of an outage.
   - **outcome unknown** (`L`): charged only if the session has not reached that request number. If it is *at* that number the original was applied (nothing to charge); if it moved *past* it, the usage may have been reported again, so **nothing is charged** and the record is kept as a dead letter marked *ambiguous* (`ocs_unsettled_ambiguous_total`, alert `OcsUnsettledUsageAmbiguous`). A person decides: charging could charge twice, and the project prefers a missed charge to a double one.
   - Either way the late settlement **advances the session to that request number** with a refusal as the stored answer, so a network element that retries the very request it was refused gets the same refusal and the usage is not charged a second time. Both cases are tested against a real Redis, with four deliberate breakages each failing a test (`pkg/engine/settle_test.go`).
4. **Dead letters.** If the session no longer exists (its state expired) the usage cannot be charged through the session. The record is moved to the dead letters, kept in the log with the reason, counted (`ocs_unsettled_dead_total`) and alerted on (`OcsUnsettledUsageLost`). Nobody consumes dead letters automatically.

Choice and operation: `OCS_UNSETTLED_SPOOL` is required, a directory or `off` (usage is then lost, with a loud warning); the chart makes it a pod `emptyDir` (`gateway.unsettledSpool.mode`), compose a named volume. Metrics: `ocs_unsettled_pending`, `ocs_unsettled_oldest_age_seconds`, `ocs_unsettled_dead_letters`, `ocs_unsettled_settled_total`, `ocs_unsettled_dead_total`, `ocs_unsettled_ambiguous_total`, `ocs_unsettled_deferred_total`, `ocs_engine_unsettled_recorded_total`, `ocs_engine_unsettled_unrecorded_total`, `ocs_engine_unsettled_terminates_total`, `ocs_engine_unsettled_updates_total`. Alerts: `OcsUsageNotSettled` (warning: it happened), `OcsUnsettledUsageBacklog` (critical: not draining), `OcsUnsettledUsageLost` (critical), `OcsUnsettledUsageAmbiguous` (warning: a person decides).

Limits, stated plainly:

- **The spool is node-local.** It survives a process or container restart on the same volume (tested, and in the `redis-and-gateway` chaos scenario), **not the loss of the pod's `emptyDir` or of the node**; usage recorded there and not yet replayed is lost with it. A persistent volume per gateway would need a StatefulSet; the chart does not offer it.
- A request that arrives while the gateway itself is down never reaches it; the network element's own retry is what recovers that.
- The replay is rated with the **tariff in effect when it is replayed**, not when the usage happened.
- The usage is charged **after** the fact: a subscriber can briefly hold more than the balance shows, and over-use beyond the balance is reported as a shortfall like any over-use, not blocked.
- **The "never sent" rule rests on an assumption about the network element**: that after a refused update it does not report the same usage again. For a Gy peer that retries the same CC-Request-Number it holds (the late settlement then answers the retry with the original refusal); for one that sends the usage again under a *new* number, the usage would be charged twice. Not verified against real peers.
- **Unknown-outcome errors are common**: only a failed *dial* proves a call was never sent. A connection that dies mid-call (a stopped Redis closing pooled connections) is "unknown", so the first few refused updates of an outage may end as ambiguous dead letters.
- Dead letters, ambiguous or not, need a human.

## Measured numbers (this sandbox, not a benchmark environment)

Single-thread loopback Redis 7.0.15, one subscriber, `Charge` update path: ~110 µs/op, 24 allocs/op (all from go-redis; the Lua call itself is a small part of that). Engine path with a stubbed data tier: ~1.8 µs/op, 0 allocs/op (asserted by a test). These do not demonstrate 50k TPS or P99 < 10 ms; that needs the load-test harness against a real cluster.

## Known limitations / follow-ups

1. **Redis Cluster key declaration.** The script derives keys from `KEYS[1]` (the hash-tagged prefix) instead of declaring each one. This works because they share a slot, but it breaks the strict "declare all keys" contract; verify against the target Redis version/proxy before deploying to Cluster.
2. **Durability** is whatever the Redis persistence config provides (risk R1 in doc 01). Commits wait for replica acknowledgement (`WAIT`, doc 07), which narrows this but is not a consensus guarantee.
3. **Allocations in the data client** (go-redis) mean the end-to-end path is not zero-alloc; a custom RESP client is an option if GC profiling demands it.
4. **Deleted buckets.** Refunds to a bucket that no longer exists are parked in `P:orph` rather than dropped; nothing reads that hash yet.
