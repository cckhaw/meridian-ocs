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
   concurrently, and no failure is injected during a run (Redis failover, a frozen replica, a Kafka
   outage): the failure policies ([02](02-decisions-and-failure-policy.md)) are tested in isolation, not under load.
4. **Seeding goes through the provisioning API**, about 250 subscribers per second here, and at most 99,999
   per run; nothing exercises a large subscriber population or a skewed (hot-subscriber) distribution
   beyond choosing a small `--subscribers`.
5. **One bucket reconciled** (see above); the Kafka event trail and the ledger are not compared with the
   balances.
6. **Server-side latency is not broken down** (no scrape of the gateway's histograms; only its request
   counter is cross-checked), and the tail seen at 600 sessions/s is unexplained.
7. **No soak.** Runs last under a minute; leaks, slow growth and reservation expiry over hours are untested.
