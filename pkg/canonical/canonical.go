// Package canonical is the protocol-independent charging event model that the
// signaling gateway produces and the rating engine consumes. It mirrors
// api/proto/ocs/v1/canonical.proto; the Go structs are used on the hot path so
// they can be pooled and reused without protobuf allocation.
package canonical

import (
	"sync"
	"time"
)

type Source uint8

const (
	SourceUnspecified Source = iota
	SourceDiameterGy
	SourceNchf
	SourceREST
)

type RequestType uint8

const (
	TypeUnspecified RequestType = iota
	TypeInitial
	TypeUpdate
	TypeTerminate
	TypeEvent
)

type UnitKind uint8

const (
	UnitBytes UnitKind = iota
	UnitSeconds
	UnitEvents
)

func (k UnitKind) String() string {
	switch k {
	case UnitBytes:
		return "bytes"
	case UnitSeconds:
		return "seconds"
	case UnitEvents:
		return "events"
	}
	return "unknown"
}

// UnitRequest is one rating group / multiple-unit-usage entry.
type UnitRequest struct {
	RatingGroup uint32
	ServiceID   uint32
	Kind        UnitKind
	Requested   int64 // units asked for (Requested-Service-Unit / requestedUnit)
	Used        int64 // units consumed since the last request (Used-Service-Unit / usedUnitContainer)
}

// Request is the canonical charging request. Instances are pooled; use
// AcquireRequest / ReleaseRequest and never retain one after release.
type Request struct {
	Source        Source
	Type          RequestType
	SubscriberID  string // MSISDN or SUPI: the shard key
	SessionID     string // Diameter Session-Id or Nchf chargingSessionId (event id for TypeEvent)
	RequestNumber uint32 // CC-Request-Number / invocationSequenceNumber: idempotency key
	EventTime     time.Time
	RATType       string
	CellID        string
	Slice         string
	Units         []UnitRequest
	// AcceptUnknownSession asks the data tier to accept an update or terminate for a session it does not know, as the
	// first request of a new one, and charge its usage (3GPP TS 32.290 5.5.1.2). Only the Nchf adapter sets it; for
	// Gy an unknown session is DIAMETER_UNKNOWN_SESSION_ID (RFC 4006).
	AcceptUnknownSession bool
}

// Reset clears the request for reuse while keeping slice capacity.
func (r *Request) Reset() {
	units := r.Units[:0]
	*r = Request{Units: units}
}

// Result is the outcome for a unit (and, aggregated, for the request).
type Result uint8

const (
	ResultUnspecified Result = iota
	ResultGranted
	ResultPartialFinal       // granted less than asked; Final-Unit-Indication set
	ResultCreditLimitReached // eligible buckets exist but are empty
	ResultNoBucket           // nothing eligible to charge
	ResultUnknownSession
	ResultStaleRequest // request number older than the last processed one
	ResultUnavailable  // data tier down and the fail policy denied
	ResultTooBusy
	ResultBarred // the subscriber is suspended or terminated
)

// UnitGrant is the answer for one rating group.
type UnitGrant struct {
	RatingGroup     uint32
	Result          Result
	Granted         int64
	ValiditySeconds uint32
	FinalUnit       bool
	Degraded        bool  // granted by the fail-open policy without an authoritative balance
	Shortfall       int64 // used units that could not be charged (needs adjustment)
}

// Response is the canonical charging response. Pooled like Request.
type Response struct {
	Result    Result
	SessionID string
	Grants    []UnitGrant
}

// Reset clears the response for reuse while keeping slice capacity.
func (r *Response) Reset() {
	grants := r.Grants[:0]
	*r = Response{Grants: grants}
}

var (
	requestPool  = sync.Pool{New: func() any { return &Request{Units: make([]UnitRequest, 0, 4)} }}
	responsePool = sync.Pool{New: func() any { return &Response{Grants: make([]UnitGrant, 0, 4)} }}
)

func AcquireRequest() *Request { return requestPool.Get().(*Request) }

func ReleaseRequest(r *Request) {
	r.Reset()
	requestPool.Put(r)
}

func AcquireResponse() *Response { return responsePool.Get().(*Response) }

func ReleaseResponse(r *Response) {
	r.Reset()
	responsePool.Put(r)
}
