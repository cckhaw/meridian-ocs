# Subscriber Identity Mapping

Code: `pkg/identity` (resolver, cache, Redis directory), used by the Gy adapter (`internal/gy`) and the Nchf adapter (`internal/nchf`), wired in `services/ocs-signaling-gateway/main.go`.

## The problem

Gy identifies a subscriber by MSISDN and/or IMSI; Nchf by the SUPI (IMSI or NAI). Keying balances by any one of them is wrong:

- **SIM swap** keeps the MSISDN and changes the IMSI.
- **Number porting / reassignment** changes the MSISDN and keeps the IMSI.
- **5G / IoT** subscribers may have no MSISDN.
- Keying by "MSISDN, else IMSI" (what the adapters did before) puts one person's 4G and 5G usage on two different balances.

## The model

Balances, sessions and reservations live under one opaque **subscriber key** chosen by provisioning (for example an account id). The directory maps every identifier to that key:

```
id:msisdn:491701234567   ->  acct-8812
id:imsi:262011234567890  ->  acct-8812
id:nai:user@example.com  ->  acct-9001
```

(`SET` these by hand until the provisioning service exists. The prefix is configurable. MSISDNs are stored as E.164 digits without `+`; national-format numbers are **not** converted.)

Keys must be 1-128 printable ASCII characters without spaces or braces: they are embedded in a Redis hash tag and in charging-reference URLs.

## Resolution rules (`Resolver.Resolve`)

The adapter passes every identifier the request carries, in precedence order (Gy: MSISDN, IMSI, NAI; Nchf: the SUPI).

1. Each identifier is validated and normalised (MSISDN/IMSI 6-15 digits, a leading `+` dropped; NAI 3-128 printable characters). Malformed → `ErrInvalid`.
2. **All** identifiers are looked up so inconsistencies are noticed. Unknown ones are skipped; if at least one resolves, it wins.
3. If two identifiers resolve to **different** keys → `ErrConflict`, and the request is refused (default `reject`). `prefer-first` keeps the first key and counts the conflict; use it only if availability matters more than the risk of charging the wrong account.
4. If none resolves → `ErrUnknown`.
5. A **directory failure fails the resolution**. It is never treated as "unknown" or guessed around, because a guess could charge the wrong account.

Adapter outcomes (all before the rating engine is called):

| Resolver result | Gy | Nchf |
|---|---|---|
| unknown | 5030 USER_UNKNOWN | 404, cause `USER_UNKNOWN` |
| invalid identifier | 5004 | 400 |
| conflict | 5003 | 400 |
| directory unavailable | 3002 (E bit) | 503 + `Retry-After` |

## Cache

`CachedDirectory` sits between the resolver and Redis (default 30 s positive TTL, 2 s negative TTL, 1 M entries):

- Concurrent misses for one identifier share a single Redis lookup.
- Unknown identifiers are remembered briefly, and the cache is bounded, so a flood of invented identifiers is neither a flood of Redis lookups nor a memory leak (tested with 20,000 distinct unknown identifiers against a 640-entry bound).
- Directory failures are never cached, so recovery is immediate.
- A cache hit allocates nothing, and resolving two cached identifiers allocates nothing (asserted by a test).

**Staleness window:** a binding changed in the directory (SIM swap, number reassignment) is seen after at most the TTL. During that window the old mapping is used. After a SIM swap, a request carrying both the MSISDN and the new IMSI is refused as a conflict once the cache expires, rather than being charged on a guess (covered by the cross-protocol test). Push invalidation from provisioning is not implemented.

## Binding API (`RedisDirectory`)

- `Bind(id, key)`: idempotent for the same key; `ErrConflict` if the identifier already belongs to another subscriber (no silent theft).
- `Rebind(id, key)`: explicit move (SIM swap, reassignment); reports whether it moved.
- `Unbind(id)`.

Binding several identifiers of one subscriber is several single-key writes and is **not atomic**: between them the identifiers can briefly disagree. That is exactly the conflict case above, so it fails closed.

## Configuration (gateway binary)

| Variable | Default | |
|---|---|---|
| `OCS_IDENTITY_MODE` | `directory` | `direct` uses the raw identifier as the key and does **not** unify identities; development only, logs a warning |
| `OCS_IDENTITY_PREFIX` | `id:` | Redis key prefix |
| `OCS_IDENTITY_CACHE_TTL` | `30s` | positive-entry TTL |
| `OCS_IDENTITY_ON_CONFLICT` | `reject` | or `prefer-first` |

Library default: an adapter built without a resolver uses `NewPassthroughResolver` (raw identifier, no validation). That keeps unit tests simple but does not unify identities, so deployments must configure the directory.

## Verified

- `pkg/identity` tests (including against a real Redis) and mutation checks: disabling conflict detection and disabling the cache bound each make tests fail.
- `TestOneSubscriberOneBalanceAcrossGyAndNchf` (gateway package): with one resolver shared by both adapters, an Nchf create by IMSI, a Gy CCR by MSISDN, by IMSI alone and by both together all draw from the same balance; no state is created under the raw identifiers; unknown subscribers are refused on both interfaces; after a SIM swap the IMSI charges the new subscriber and a request mixing the old MSISDN with the moved IMSI is refused.
- The built gateway binary against a real Redis, with the directory seeded by hand (`SET id:msisdn:491700 acct-9`, `SET id:imsi:262011234567890 acct-9`): Nchf by IMSI and Gy by MSISDN both reserved from `acct-9`; an unknown IMSI got 404.

## Not implemented / known gaps

1. The provisioning service populates the directory when it creates a subscriber (doc 08); there is no bulk import and no command to move an identifier to another subscriber.
2. No cache invalidation push; changes are seen after the TTL.
3. An in-flight session keeps the key it was created with. Gy resolves on every CCR, so a binding that changes **mid-session** makes later CCRs of that session resolve to a different key and fail as unknown session. Nchf is unaffected (the key is in the reference).
4. Only MSISDN, IMSI and NAI. GPSI-based Nchf identification and SIP-URI / private Gy identifiers are not handled.
5. No number normalisation beyond dropping `+` (no national to E.164 conversion).
6. Resolver statistics are exported by the gateway as `ocs_identity_*` (doc 11); the directory cache's own hit/miss counters are not.

## Other users of the directory

The provisioning workers write bindings (doc 08); the offline mediation pipeline (doc 10) resolves IMSI/MSISDN of CDRs through the same cached `Resolver` with `ConflictReject`, and sends unknown or conflicting identities to a suspense output instead of guessing.
