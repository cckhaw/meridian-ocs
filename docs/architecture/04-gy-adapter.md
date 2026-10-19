# 4G Gy/Ro Adapter (Diameter Credit-Control)

Code: `pkg/diameter` (codec), `services/ocs-signaling-gateway/internal/gy` (server, decode, encode, admission), `services/ocs-signaling-gateway/main.go` (co-located wiring).

The gateway terminates Diameter over TCP, decodes CCRs into `canonical.Request`, calls the rating engine in-process and encodes the CCA. Rating, balances and idempotency live behind the engine (doc 03); the adapter keeps no session state.

## Peer handling

| Event | Behaviour |
|---|---|
| Anything but CER before the peer is open | Connection closed (RFC 6733) |
| CER | CEA 2001 with Host-IP, Vendor-Id, Product-Name, `Auth-Application-Id 4`, `Supported-Vendor-Id 10415`. Missing Origin-Host/Realm → 5005 and close; no Credit-Control application (also accepted inside Vendor-Specific-Application-Id) → 5010 and close |
| DWR | DWA 2001, hop-by-hop / end-to-end echoed |
| DPR | DPA 2001, then close |
| Unknown request command | Answer 3001 with the E bit |
| Silent peer | Server sends DWR after one watchdog interval of silence (default 30 s); the read deadline drops the peer after three |
| Message larger than `MaxMessage` (64 KiB) or bad header | Connection closed |

## CCR decoding

| Diameter | Canonical |
|---|---|
| Session-Id | `SessionID` |
| CC-Request-Type 1/2/3/4 | Initial / Update / Terminate / Event |
| CC-Request-Number | `RequestNumber` (the idempotency key in the charge script) |
| Subscription-Id | `SubscriberID`: END_USER_E164 (MSISDN), else IMSI, else the first other type |
| Event-Timestamp | `EventTime` (NTP → Unix); now if absent |
| Service-Information > PS-Information > 3GPP-RAT-Type | `RATType` (decimal string) |
| MSCC: Rating-Group, Service-Identifier | `RatingGroup`, `ServiceID` |
| RSU: CC-Total-Octets, or CC-Input + CC-Output-Octets | bytes `Requested` (Total wins if both) |
| RSU: CC-Time / CC-Service-Specific-Units | seconds / events `Requested` |
| Empty RSU | server default (`DefaultRequestedBytes/Seconds/Events`: 10 MiB / 300 s / 1) |
| USU (several are summed) | `Used` |
| MSCC without RSU | `Requested = 0` (usage report only; never refused) |

Rejected with a protocol answer (the connection stays up): missing Session-Id / CC-Request-Type / CC-Request-Number / subscriber / (non-terminate) MSCC → 5005; bad type or mixed unit kinds → 5004; bad AVP length → 5014; more than 16 MSCC → 5004; other application id → 3007. **Events must carry `Requested-Action = DIRECT_DEBITING`**; refunds, balance checks and price enquiries are refused with 5012 rather than being rated as a debit.

Values above 2^40 stay above the data layer's limit and are refused as invalid instead of wrapping.

## CCA encoding

| Canonical result | Result-Code | Notes |
|---|---|---|
| Granted / PartialFinal | 2001 | One MSCC per unit: GSU (kind-specific AVP), Rating-Group, Validity-Time, Final-Unit-Indication (TERMINATE) on the last slice, per-MSCC Result-Code for a refused unit |
| CreditLimitReached | 4012 | No MSCC |
| NoBucket | 5031 | |
| UnknownSession | 5002 | |
| StaleRequest | 5012 | |
| Unavailable (fail policy denied) | 4010 | |
| Engine overloaded / admission refused / per-peer limit | 3004 with E bit | Session-Id echoed |
| Engine invalid | 5004 | |
| Engine closed / cancelled | 3002 | |

CCA-Terminate carries no units. Degraded (fail-open) grants look like normal grants on the wire.

## Load shedding

Three independent layers, all answering 3004 immediately instead of queueing: per-peer in-flight cap (default 256), global admission (`Admission`: lock-free GCRA token bucket plus an in-flight ceiling, both adjustable at runtime for the AI Ops control plane), and the engine's bounded worker queues (`ErrOverloaded`). CPU/memory/P99-driven adaptive limits are not implemented; `Admission.SetRate` / `SetMaxInflight` are the hooks for them.

## Co-located wiring (`main.go`)

Gateway + engine + Redis client in one process. Configuration is by environment variable (see the file header). There is no profile cache yet, so the hybrid fail policy is not wired in the binary: with Redis down every request is denied. `OCS_CASH_BYTES_RATE` is a placeholder until the catalog exists.

## Measured (this sandbox, not a benchmark environment)

Decode + encode of a two-MSCC CCR-Update (excluding socket I/O and rating): ~660 ns, 2 allocations per request (the subscriber-id and session-id strings). The codec itself parses without allocating (asserted by a test). The decoder was fuzzed for 10 s (1.2 M inputs) without a panic; the fuzz target is `FuzzParse` in `pkg/diameter`.

Verified with an independent client: the built binary against a real Redis, driven by a separate Python Diameter client (CER, CCR-I/U/T → 2001, balance changed by exactly the used units).

## Not implemented / known gaps

1. **Transport:** TCP only. No SCTP (needs a third-party library or cgo; `Serve` takes any `net.Listener`, so it can be added) and no TLS/IPsec negotiation (Inband-Security-Id is ignored).
2. **Routing:** Destination-Realm/Host are not validated; no relay, redirect or proxy-agent behaviour; no peer table or outbound connections.
3. **Server-initiated messages:** no Re-Auth-Request or Abort-Session-Request.
4. **AVPs ignored:** Reporting-Reason, Trigger, Tariff-Change-Usage, Time-Of-Change, Quota-Holding-Time / Quota-Consumption-Time, Credit-Control-Failure-Handling, cell id / User-Location-Info. Only RAT type is extracted from PS-Information.
5. **Unit coverage:** monetary units (Cost-Information, CC-Money) are not supported.
6. **Gateway state:** no hop-by-hop/session tracking ring buffer. Answers echo the request's identifiers; the only gateway-originated requests are DWRs.
7. **Ordering:** each CCR is handled in its own goroutine. Diameter allows one outstanding CCR per session, so same-session ordering holds for conforming peers; a non-conforming peer pipelining two CCRs of the same session could see them race (the charge script still rejects stale numbers).
8. **Fail policy in the binary** (see above) and the remote-engine (gRPC) mode.
