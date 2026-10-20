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

The engine wraps the data-tier call in `failpolicy.Breaker`. When the call fails or the breaker is open, requests go to `failpolicy.Engine` (see doc 02): prepaid are denied (`ResultUnavailable`), postpaid get a capped, journalled, short grant (`Degraded=true`). A terminate during an outage is acknowledged and emits `AuditUnsettledUsage`; the reservation expires and is refunded, so **usage reported only in that terminate is not charged until reconciliation consumes the audit event**. That reconciler does not exist yet.

## Measured numbers (this sandbox, not a benchmark environment)

Single-thread loopback Redis 7.0.15, one subscriber, `Charge` update path: ~110 µs/op, 24 allocs/op (all from go-redis; the Lua call itself is a small part of that). Engine path with a stubbed data tier: ~1.8 µs/op, 0 allocs/op (asserted by a test). These do not demonstrate 50k TPS or P99 < 10 ms; that needs the load-test harness against a real cluster.

## Known limitations / follow-ups

1. **Redis Cluster key declaration.** The script derives keys from `KEYS[1]` (the hash-tagged prefix) instead of declaring each one. This works because they share a slot, but it breaks the strict "declare all keys" contract; verify against the target Redis version/proxy before deploying to Cluster.
2. **Durability** is whatever the Redis persistence config provides (risk R1 in doc 01). Commits wait for replica acknowledgement (`WAIT`, doc 07), which narrows this but is not a consensus guarantee.
3. **Allocations in the data client** (go-redis) mean the end-to-end path is not zero-alloc; a custom RESP client is an option if GC profiling demands it.
4. **Deleted buckets.** Refunds to a bucket that no longer exists are parked in `P:orph` rather than dropped; nothing reads that hash yet.
