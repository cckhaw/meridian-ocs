# Load test and balance reconciliation

`tools/ocs-loadgen` is a load generator for the running system. It does two jobs that are
usually done by two tools: it drives **charging traffic** at a chosen rate and records latency, and it
**reconciles every balance afterwards** against what it did, so a run proves "no money was created or
lost" and not only "it was fast enough".

```sh
make stack-up                       # the local stack (13)
make loadtest                       # 200 sessions/s for 20 s against it
make loadtest LOAD_ARGS="-rate 500 -subscribers 2000 -duration 30s -slo-p99 10ms -slo-completed 0.999 -json out.json"
cd tools/ocs-loadgen && go run . -h # every flag
```

`dev/chaos.sh` (`make chaos`) runs it while a component of the local stack is broken; see
[Failure injection](#failure-injection).

Exit status: `0` the run passed, `1` an objective was missed or a balance did not reconcile, `2` the
run could not be carried out (bad flags, seeding failed, a service unreachable).

## What it does

1. **Seeds** `--subscribers` new prepaid subscribers through the provisioning REST API (so the commands
   go through Kafka and the workers like any real change), attaches `--offering`, and reads each one's
   starting balance of `--bucket`. Each run uses fresh subscribers, so nothing needs cleaning up and runs
   do not disturb each other. Up to 99,999 per run.
2. **Drives load** over Nchf (HTTP/2, plain or TLS) at a **constant arrival rate** (`--rate` sessions per
   second): a session is *create* → `--updates` × *update* → *release*, each reporting `--used-percent` of
   the grant as used. Arrivals are open-loop: a slow answer does not delay the next arrival, and arrivals
   that find `--max-inflight` sessions already running are counted as **dropped**, not hidden. A
   `--warmup` period runs at the same rate and is left out of the results.
3. **Reconciles** every subscriber afterwards by reading the bucket back through provisioning.
4. **Reports** achieved rate, outcomes, latency per request type, a cross-check of the client's request
   count against the gateway's own counter (`--gateway-admin`), and the verdict of the reconciliation.

Latency is reported per request type (`create`, `update`, `release`, from sending the request) and as
`createFromDue`, which counts from the moment the arrival was *due* — the measure that does not hide
queueing when the generator or the system falls behind (coordinated omission). Quantiles come from a
histogram with under 1.6 % relative error and are rounded **up**, so they are never optimistic.

## The reconciliation

For each subscriber the generator keeps what it knows for certain and what it does not:

- `used`: usage reported in an update or release that the gateway answered `200`/`204` to.
- `held`: grants whose session was not confirmed released (an update or release failed or was shed).
- `unsure`: an upper bound on what requests with an unknown outcome (timeouts, 5xx) may have moved.

The balance must then lie in `[initial − used − held − unsure, initial − used]`. The top edge is exact:
nothing can return more than was taken. Verdicts: **exact** (no uncertainty and the balance is exactly
`initial − used`), **within** (uncertainty exists and the balance is inside the bounds), and the failures
**created** (the balance is above the top edge: value appeared), **overspend** (below the bottom edge:
money was taken twice), **negative**, **unreadable**. Any failure makes the run fail.

Two cases the client cannot see are handled explicitly rather than assumed away:

- An **update answered "denied"** (`END_USER_SERVICE_DENIED`): with the data tier down nothing was settled and
  the previous reservation stays; for a barred subscriber the usage was settled. The answer does not say which,
  so the usage goes to `unsure` and the grant to `held`, never to `used`.
- A **release answered `204`** carries no body, so the client cannot tell a settled release from a terminate
  the gateway acknowledged while the data tier was down ([03](03-rating-engine.md)). The gateway counts those
  (`ocs_engine_unsettled_terminates_total`; only terminates that reported usage). With `--gateway-admin`, the
  generator allows total deviation outside the bounds of at most *that many requests' worth of bytes* and
  reports it as **unsettled**; any deviation beyond that budget, or any deviation when the gateway admitted
  nothing, is a violation. (Mutation-checked: a fake that hides the same fault from its counter fails the run.)

The check is limited, and says so: it covers the one bucket named by `--bucket`; a subscriber who spends the
whole bundle is reported separately because usage beyond it is charged to other buckets, which are not
checked. It does not check the cash bucket, events, or the Kafka event trail.

### Look-alike sessions

The gateway derives the charging reference from the create request (subscriber, consumer, timestamp to
the millisecond, PDU session id) so that a retransmitted create is *replayed*, never reserved twice
([05](05-nchf-adapter.md)). Two sessions of one subscriber that send an identical create within one
millisecond are therefore, by design, **one** session. The first version of this tool let that happen
under bursts and reported a balance 1.6 MB above its bounds; the per-subscriber audit trail (`--audit`)
showed the same reference returned for two creates. The gateway was right and the generator's bookkeeping
was wrong. Now every session of a subscriber gets its own PDU session id, the generator counts any
reference it receives twice, and a run with even one such collision is **invalid** and fails with an
explanation instead of blaming the system. Mutation-checked: with the identifier fixed again the test
fails.

## What was verified

- Unit and end-to-end tests run the generator against in-process fakes of the gateway and provisioning
  (`go test ./tools/ocs-loadgen`, race detector on): a correct system passes with every balance exact and
  the gateway counter equal to the client's count; a system that grants without debiting fails as
  *created*; one that debits twice fails as *overspend*; timeouts widen the bounds and still pass; a
  spent bundle gives denials and stays non-negative; falling behind is reported as dropped and misses an
  objective; look-alike sessions invalidate the run; JSON output; flag validation; the histogram against
  sorted data.
- Against the **real stack** (`docker compose`, 13) the generator ran at 100 to 1,000 sessions/s with 3 to
  2,000 subscribers, including many overlapping sessions on 3 subscribers and a bundle spent on purpose.
  After the fix above, eight such runs produced no violation, no reference collision and no unreadable
  balance. CI runs a short pass on every pull request (below).

## Failure injection

`dev/chaos.sh replica-freeze|primary-stop|primary-kill|gateway-kill|all` (needs the local stack, Docker and
Go) starts the generator at 150 sessions/s over 300 subscribers, lets it run healthy for 10 s, breaks one
thing for 8 s, restores it, keeps loading for 25 s more, and reconciles every balance. `--timeline` prints
what happened in each second. It stops, freezes and kills containers of the stack: development only, and not
part of CI (it takes about a minute per scenario).

| Fault (8 s, at 150 sessions/s) | Sessions failed | Seconds with failures | What the system did |
|---|---|---|---|
| Redis **replica frozen** (`docker pause`) | 0 | none | Charging unaffected, tail latency unchanged apart from one 0.2 s blip. After 5 consecutive shortfalls the gateway suspends waiting for 5 s ([07](07-durability.md)): about 3,100 commits per run skipped the replica wait (`ocs_durability_suspended_total`). **Availability is kept; durability is not, by design.** |
| Redis **primary stopped** (graceful) | about 1,250 of 6,450 (19 %) | the 8 s of the fault and the 1 s after | Creates hang until the client's 3 s timeout in most runs; in one run the breaker opened at once and creates were refused (fail-closed). Back to normal within about 1 s of the restart. |
| Redis **primary killed** (SIGKILL), restarted | about 1,200 of 6,450 | the same | Same as above. No acknowledged commit was lost in 4 kills, which is evidence, not proof (see gaps). |
| **Gateway killed**, restarted | about 1,650 of 6,450 (26 %) | the 8 s of the fault and about 3 s after | In-flight sessions fail; sessions continue after the restart because their state is in Redis. About 3 s from `start` to serving. |

Across the 10 runs with the final generator and gateway (all four scenarios once, then the two Redis
outages three more times each) **every balance reconciled**: no money created, none taken twice, none
negative. One of the 10 showed the gateway's *unsettled terminate* counter at 1 and a deviation of 200,000 bytes
inside the 1,000,000-byte budget it allows.

### What the failure injection found

1. **Usage was lost, silently, when Redis was down during a terminate.** The first Redis-stop run failed the
   reconciliation: one session's release had been acknowledged with `204` after a 0.6 s stall but never
   settled; its reservation expired 36 s later and the usage (800 KB) was never charged. Redis's own ledger
   for the subscriber showed 22 sessions the client had completed, 21 with two settlements and one with
   one. This is the documented behaviour of the engine ([03](03-rating-engine.md): a terminate during an
   outage is acknowledged so the network can release the session, and the usage "is not charged until
   reconciliation consumes the audit event"), and the reconciler **does not exist**. What was missing was any
   way to see it, and a test that sizes it. Now: `ocs_engine_unsettled_terminates_total`, the chart alert
   `OcsUsageNotSettled`, and the generator's budgeted check above. It is revenue leakage (the subscriber is
   charged less), not a double-spend, but it is real, and a reconciler remains the thing to build.
2. **The first version of the check blamed the system for a gap in its own model**: it assumed a `204` meant
   settled. The fix above makes the model state what it does not know.
3. **Behaviour during a Redis outage is not uniform** (hanging until the client times out versus an
   immediate refusal), probably depending on whether the breaker has opened by the time a request arrives. Not
   investigated.

## What was measured, and what it does not mean

One run on the development sandbox: 4 vCPUs, 16 GB, **the generator and the whole stack on the same
machine** (Redis primary + replica with TLS, Redpanda, five services, in Docker). The generator competes
with the system under test for the same CPUs, so these numbers say how this laptop-class environment
behaves, **not** what the design can do. Nothing here supports or refutes 50,000 TPS or P99 < 10 ms.

| Offered (sessions/s) | Requests/s achieved | Create P50 / P99 (ms) | Outcome |
|---|---|---|---|
| 100 | 300 | 1.7 / 4.3 | all sessions completed; balances exact |
| 500 | 1,500 | 1.7 / 23 | all completed; balances exact |
| 600 | 1,800 | 3.3 / 745 | all completed; balances exact; a tail of stalls near 0.7 s |
| 800 | 2,300 | 1,300 / 2,600 | 13 % of requests refused `429`; no violations |
| 1,000 | 2,500 | 1,700–2,000 / 2,500 | 30–45 % refused `429`; no violations |

Reading it: on this machine the system keeps up to roughly 1,800 charging requests per second; beyond
that the gateway's admission control refuses work with `429` (it does not queue without bound or lose
money — balances still reconcile, with abandoned reservations accounted as `held`) but the accepted
requests wait for seconds. Why the P99 jumps between 500 and 600 sessions/s (stalls of about 0.7 s while
the median stays at 3 ms) was **not** investigated: candidates are CPU contention with the generator,
Redis persistence, or replica acknowledgement, and none was tested. The measured tail is an open item.

## Gaps

1. **Not a capacity test.** No dedicated load machines, no cluster, no multi-node Redis; the generator
   shares CPUs with the system. 50k TPS and P99 < 10 ms remain unproven.
2. **Nchf only.** There is no Diameter Gy mode: the generator speaks HTTP/2 JSON, not the Diameter
   capabilities exchange and credit-control messages.
3. **Charging only; no mixed load.** Provisioning changes, catalog publishes and mediation are not driven
   concurrently. Failure injection covers four faults, one at a time, for 8 s ([above](#failure-injection)):
   no Kafka outage, network partition, slow or full disk, clock jump, or two faults at once, and no
   **Redis failover** (there is no Sentinel or operator, so a killed primary is simply restarted: a promoted
   replica that lacks an acknowledged write is untested). Whether a SIGKILLed primary can lose commits the
   replica acknowledged depends on AOF settings and was exercised only four times.
4. **Seeding goes through the provisioning API**, about 250 subscribers per second here, and at most 99,999
   per run; nothing exercises a large subscriber population or a skewed (hot-subscriber) distribution
   beyond choosing a small `--subscribers`.
5. **One bucket reconciled** (see above); the Kafka event trail and the ledger are not compared with the
   balances.
6. **Server-side latency is not broken down** (no scrape of the gateway's histograms; only its request
   counter is cross-checked), and the tail seen at 600 sessions/s is unexplained.
7. **No soak.** Runs last under a minute; leaks, slow growth and reservation expiry over hours are untested.
