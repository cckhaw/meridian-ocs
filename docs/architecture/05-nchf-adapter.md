# 5G Nchf_ConvergedCharging Adapter (TS 32.291)

Code: `services/ocs-signaling-gateway/internal/nchf` (handler, mapping, encoding, HTTP/2 + TLS server), wired in `services/ocs-signaling-gateway/main.go` next to the Gy adapter. Both adapters share one engine and one admission budget (`internal/admission`).

Field names, required properties, status codes and enum values below were checked against the Rel-17 OpenAPI definition (`TS32291_Nchf_ConvergedCharging.yaml`, version 3.1.7). Where the OpenAPI is silent, or the data type lives in a file I could not read, the text says so.

## Resources

| Operation | Request | Success | Notes |
|---|---|---|---|
| Create | `POST {root}/chargingdata` | `201 Created` + `Location` | `Location` is an absolute URI built from the request's scheme and Host |
| Update | `POST {root}/chargingdata/{ref}/update` | `200 OK` + `ChargingDataResponse` | |
| Release | `POST {root}/chargingdata/{ref}/release` | `204 No Content` | |

`{root}` is `/nchf-convergedcharging/v3` (the Rel-17 server URL) **and** `/nchf-convergedcharging/v1` (the path in the original brief). Both are served; the list is configurable (`Config.APIPrefixes`). Other methods get 405 with `Allow: POST`; other paths 404; a body that is not `application/json` gets 415; bodies over 64 KiB get 413.

The OpenAPI file declares no `Location` header on the 201. It is sent anyway because the create answer has to give the consumer the reference; confirm this against the TS text for your SMF.

## Charging data reference

`<region>.<base64url(subscriber key)>.<16 hex>`. It is opaque to the SMF and does three jobs:

1. **Routing without a lookup.** Update and release do not have to repeat the subscriber (and `subscriberIdentifier` is optional there), but the subscriber key is the shard key. It is recovered from the reference. If the body does repeat it, it must agree (else 400).
2. **Regional homing.** A reference from another region is redirected with 307 to `RegionBaseURLs[region]` + the original URI, or answered 404 if that region is unknown.
3. **Idempotent create.** The last part is a SHA-256 of the create request: subscriber, `nodeFunctionality`, `nFName`, `nFIPv4Address`, `nFIPv6Address`, `invocationTimeStamp`, the PDU session id when present, and the sequence number for events. A retransmitted create therefore maps to the same reference and the charge script replays its stored answer instead of reserving twice. Two creates for the same subscriber from the same NF with the same timestamp and **no** PDU session id are indistinguishable and are treated as a retransmission.

`invocationSequenceNumber` is used directly as the idempotency key of the charge script (initial, update and release each need a higher number than the last; an equal number is a retransmit and gets the stored answer).

## Request mapping

| TS 32.291 | Canonical |
|---|---|
| `subscriberIdentifier` (SUPI `imsi-…` or `nai-…`) | `SubscriberID`: the IMSI / NAI is resolved to the subscriber key through the identity resolver (doc 06). Unknown → 404 (`USER_UNKNOWN`), malformed → 400, directory unavailable → 503. Other types (e.g. `msisdn-`) → 400 |
| `invocationTimeStamp` (required) | `EventTime` (RFC 3339) |
| `invocationSequenceNumber` (required) | `RequestNumber` |
| `oneTimeEvent` + `oneTimeEventType: IEC` | `TypeEvent`. `PEC` → 501; missing type → 400 |
| `multipleUnitUsage[].ratingGroup` (required) | `RatingGroup` |
| `requestedUnit.totalVolume`, or `uplinkVolume` + `downlinkVolume` | bytes `Requested` (total wins) |
| `requestedUnit.time` / `serviceSpecificUnits` | seconds / events `Requested` |
| `requestedUnit: {}` | server default (10 MiB / 300 s / 1) |
| `usedUnitContainer[]` (summed; `serviceId` kept) | `Used` |
| no `requestedUnit` | `Requested = 0` (usage report only, never refused) |
| `pDUSessionChargingInformation.pduSessionInformation.ratType` | `RATType` |
| `…networkSlicingInfo.sNSSAI` (`sst`, `sd`) | `Slice` as `sst` or `sst-sd` |

`nfConsumerIdentification.nodeFunctionality` is required, as in the spec. Unknown properties are ignored. Mixed unit kinds in one group, a missing rating group, no `multipleUnitUsage` on create/update, or more than 16 groups → 400.

## Response mapping

Quota outcomes are reported **per rating group** in `multipleUnitInformation[].resultCode`; the HTTP status describes only the request.

| Outcome | HTTP | `resultCode` |
|---|---|---|
| Granted / partial | 201 or 200 | `SUCCESS`; `grantedUnit` (`totalVolume` / `time` / `serviceSpecificUnits`), `validityTime`; partial adds `finalUnitIndication.finalUnitAction = TERMINATE` |
| Credit limit reached | 201 or 200 | `QUOTA_LIMIT_REACHED` |
| Nothing eligible to charge | 201 or 200 | `RATING_FAILED` |
| Fail policy denied (data tier down) | 201 or 200 | `END_USER_SERVICE_DENIED` |
| Unknown or terminated reference | 404 | |
| Stale `invocationSequenceNumber` | 400 | |
| Admission refused, worker queue full | 429 + `Retry-After: 1` | |
| Engine closed or failing | 503 + `Retry-After: 1` | |

Errors are `application/problem+json` with `title`, `status`, `detail` and, for 400s, a `cause`. The `cause` strings (`MANDATORY_IE_MISSING`, `MANDATORY_IE_INCORRECT`, `INVALID_MSG_FORMAT`, `SYSTEM_FAILURE`) are from TS 29.500 as I remember it; the OpenAPI file only references `ProblemDetails` from a file I could not read, so **verify the cause strings against TS 29.500 before interop testing**.

## Transport and security

- `NewServer` serves cleartext HTTP/2 with prior knowledge (h2c, as behind a mesh sidecar) and HTTP/1.1; with a `tls.Config` it negotiates HTTP/2 through ALPN. Max 1000 concurrent streams per connection.
- `TLSConfig` loads a certificate (TLS ≥ 1.2) and, if a client CA file is given, requires and verifies client certificates (mutual TLS). Covered by a test with generated certificates: a client without a certificate is refused at the handshake.
- **OAuth2 access tokens** are validated on every request (next section), in addition to TLS / mutual TLS.
- The rating call runs on a context detached from the client request (`context.WithoutCancel`): if the SMF hangs up after the charge was applied, cancelling would only trigger the degraded path. Redis client timeouts bound the call instead.

## Access tokens (TS 33.501 §13.4, TS 29.510)

Every request must carry `Authorization: Bearer <JWS>` issued by the NRF (`internal/oauth`). Tokens are parsed and verified with `golang-jwt/jwt/v5`; the policy around it is ours:

| Check | Rule |
|---|---|
| Algorithm | allow-list ES256, ES384, RS256, PS256. `none`, HMAC and every other algorithm are refused; the key type must match the declared algorithm family, so a token cannot choose how it is verified |
| Keys | the NRF's certificate(s) or public key(s) from a PEM file (`OCS_NCHF_OAUTH_KEYS`). ECDSA P-256/P-384, RSA ≥ 2048 bits. Expired or not-yet-valid certificates in a rotation bundle are skipped (logged); a file with no usable key is an error |
| Time | `exp` is required; `exp` / `nbf` honoured with 30 s leeway |
| Audience | `aud` must contain this producer: `CHF` and/or its NF instance id (`OCS_NF_INSTANCE_ID`) |
| Scope | `scope` (space separated) must contain `nchf-convergedcharging` as a whole word (a prefix or suffix of it does not count) |
| Issuer | optional allow-list (`OCS_NCHF_OAUTH_ISSUERS`) |

Signature, format and time failures answer **401** with `WWW-Authenticate: Bearer realm="nchf-convergedcharging", error="invalid_token"` (no token: the bare realm). A correctly signed token that is not meant for this service (audience, scope, issuer) answers **403** with `error="insufficient_scope"`. The body never says which check failed.

Other design points:
- **No default.** The gateway will not start the Nchf listener until `OCS_NCHF_OAUTH` is `required` or `off`; `off` logs a warning. In the library, `NewHandler` fails unless an `Authenticator` or `AllowUnauthenticated` is given.
- **Authentication runs before the body is read and before admission control**, so a flood of forged tokens neither costs a body parse nor uses up the request budget real traffic needs (both tested). It does cost CPU for the signature check; see below.
- **Verified-token cache**, keyed by the SHA-256 of the token, bounded (10 000), never extending a token past its `exp` plus leeway, and invalidated at once when the key set changes. Forged tokens are never cached.
- **Key rotation without restart:** the key file is watched (30 s). A new bundle takes effect immediately and drops cached tokens from removed keys; an unreadable or empty file is logged and the old keys stay in force rather than locking every consumer out.
- Tokens are bearer credentials: the gateway warns when OAuth is on without TLS.

Measured here (sandbox): a cached token costs about 0.56 µs and 1 allocation; a full ES256 signature verification about 89 µs and 72 allocations (mostly inside the JWT library). An SMF reuses one token for many requests, so the cache carries the load; an attacker sending distinct forged tokens costs one signature check each, which the mesh / network policy (and mutual TLS) should absorb: per-source throttling of failed authentication is not implemented.

Verified with independent tooling: the built gateway behind `curl --http2-prior-knowledge`: no token 401, forged token 401, wrong scope 403, valid token 201, and only the valid call reserved quota. Unit tests include the classic attacks (unsigned `alg: none`, HS256 keyed with the public key, RS256 against an EC key set, tampered payload, truncated signature) and four deliberate mutations (no audience check, substring scope match, optional expiry, cache ignoring rotation) that each make tests fail.

## Measured (this sandbox, not a benchmark environment)

Handler cost for an update (JSON decode, mapping, response encode, stubbed rating call, no sockets): ~4.6 µs and 25 allocations per request, mostly `encoding/json` and string conversions. That is several times the Gy adapter (~0.66 µs, 2 allocations). A hand-written JSON scanner for the request would close most of the gap if profiling at load shows it matters. The response encoder is already allocation-light.

Verified with independent tooling: the built gateway binary, driven over HTTP/2 with `curl --http2-prior-knowledge` against a real Redis: 201 → 200 → 204 → 404 after release, bundle balance changed by exactly the used units.

## Bug found while writing the end-to-end test

The charge script accepted an update with a higher request number **after** the session had been terminated and reserved quota again on it. It now marks a terminated session closed: later update/terminate requests get "unknown session" (404 here, 5002 on Gy) and a retransmitted terminate still gets its stored answer. This also fixes the Gy path. Regression test: `TestTerminatedSessionCannotBeReopenedByALaterRequest`.

## Not implemented / known gaps

1. **Identity directory contents.** Subscribers resolve through the identity directory (doc 06); the provisioning service (doc 08) now writes the bindings when it creates a subscriber, and they can still be written with `RedisDirectory.Bind` or by hand (`SET id:imsi:<digits> <key>`) until the provisioning service exists.
2. **PEC (pending event charging)** and the reservation/confirmation flow it needs.
3. **`retransmissionIndicator`**, `notifyUri` / charging notifications (the callback), `triggers`, `quotaHoldingTime`, thresholds, `announcementInformation`, `invocationResult.failureHandling`, `sessionFailover`.
4. Only `pduSessionInformation.ratType`, `pduSessionID` and S-NSSAI are read from the PDU session charging information; other 5G charging information blocks (SMS, MMTel, NEF, …) are ignored. No monetary units.
5. **Feature negotiation** (`supportedFeatures`) is ignored.
6. Per-consumer rate limits and throttling of failed authentication; fine-grained scopes (per resource / operation) are not interpreted, only the service-level scope; EdDSA tokens are not accepted; tokens are not checked against revocation (they are short-lived and NRF-issued).
7. The gateway binary has no fail-policy profile cache (see doc 04): with Redis down every request is denied.
