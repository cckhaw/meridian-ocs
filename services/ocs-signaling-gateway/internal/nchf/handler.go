// Package nchf implements the 5G Nchf_ConvergedCharging service (3GPP
// TS 32.291) as an http.Handler: charging data create / update / release over
// HTTP/2 (or HTTP/1.1), mapped onto canonical charging requests for the rating
// engine.
//
// Resources (relative to {apiRoot}/nchf-convergedcharging/{version}):
//
//	POST /chargingdata                      create  -> 201 Created + Location
//	POST /chargingdata/{ref}/update         update  -> 200 OK
//	POST /chargingdata/{ref}/release        release -> 204 No Content
//
// The ChargingDataRef is generated here and is self-describing:
// "<region>.<base64url(subscriber)>.<16 hex>". Update and release carry no
// mandatory subscriber identifier, so the subscriber (the shard key) is
// recovered from the reference with no lookup, and a request that arrives in
// the wrong region can be redirected. The last part is a hash of the create
// request, so a retransmitted create maps to the same reference and the
// charge script replays its stored answer instead of reserving twice.
package nchf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/oauth"
)

// Rater is the rating engine as seen by the adapter (*engine.Engine).
type Rater interface {
	Rate(ctx context.Context, req *canonical.Request, resp *canonical.Response) error
}

// Config configures the handler. All fields are optional.
type Config struct {
	// Region is embedded in every ChargingDataRef (default "r1").
	Region string
	// RegionBaseURLs maps a region to its base URL (scheme://host). A request
	// whose reference belongs to another region is redirected there with 307
	// when the region is known, and answered 404 otherwise.
	RegionBaseURLs map[string]string
	// APIPrefixes are the accepted URI prefixes. Default: the Rel-17 version
	// ("/nchf-convergedcharging/v3") and "/nchf-convergedcharging/v1".
	APIPrefixes []string

	MaxBodyBytes int64 // default 64 KiB
	MaxUnits     int   // multipleUnitUsage entries per request, default 16

	// Amounts granted when a requestedUnit is present but empty.
	DefaultRequestedBytes   int64 // default 10 MiB
	DefaultRequestedSeconds int64 // default 300
	DefaultRequestedEvents  int64 // default 1

	// Auth validates the OAuth2 access token of every request (TS 33.501
	// §13.4). It is required unless AllowUnauthenticated is set.
	Auth Authenticator
	// AllowUnauthenticated serves requests without checking an access token.
	// For development and tests only.
	AllowUnauthenticated bool

	// Identity maps the SUPI (IMSI or NAI) to the subscriber key balances are
	// stored under. It must be the same resolver the Gy adapter uses, or one
	// subscriber gets two balances. nil uses identity.NewPassthroughResolver,
	// which keys balances by the raw IMSI/NAI.
	Identity *identity.Resolver

	Now func() time.Time
}

func (c *Config) setDefaults() {
	if c.Region == "" {
		c.Region = "r1"
	}
	if len(c.APIPrefixes) == 0 {
		c.APIPrefixes = []string{"/nchf-convergedcharging/v3", "/nchf-convergedcharging/v1"}
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 64 << 10
	}
	if c.MaxUnits <= 0 {
		c.MaxUnits = 16
	}
	if c.DefaultRequestedBytes <= 0 {
		c.DefaultRequestedBytes = 10 << 20
	}
	if c.DefaultRequestedSeconds <= 0 {
		c.DefaultRequestedSeconds = 300
	}
	if c.DefaultRequestedEvents <= 0 {
		c.DefaultRequestedEvents = 1
	}
	if c.Identity == nil {
		c.Identity = identity.NewPassthroughResolver()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Authenticator validates a bearer access token (*oauth.Verifier).
type Authenticator interface {
	Verify(token string) (*oauth.Claims, error)
}

// Stats are cumulative counters.
type Stats struct {
	Requests     uint64
	Shed         uint64 // answered 429
	Rejected     uint64 // answered 4xx for malformed or unsupported input
	Unauthorized uint64 // answered 401
	Forbidden    uint64 // answered 403
}

// Handler serves the Nchf_ConvergedCharging API.
type Handler struct {
	cfg   Config
	rater Rater
	adm   *admission.Admission

	requests, shed, rejected, unauthorized, forbidden atomic.Uint64
}

// NewHandler builds a Handler. adm may be nil (no admission control).
func NewHandler(cfg Config, rater Rater, adm *admission.Admission) (*Handler, error) {
	if rater == nil {
		return nil, errors.New("nchf: rater is required")
	}
	cfg.setDefaults()
	if cfg.Auth == nil && !cfg.AllowUnauthenticated {
		return nil, errors.New("nchf: an Authenticator is required (set AllowUnauthenticated only for development)")
	}
	if strings.ContainsAny(cfg.Region, ".") || cfg.Region == "" {
		return nil, errors.New("nchf: Region must not contain '.'")
	}
	if adm == nil {
		adm = admission.New(0, 0, 0)
	}
	return &Handler{cfg: cfg, rater: rater, adm: adm}, nil
}

// Stats returns a snapshot of the counters.
func (h *Handler) Stats() Stats {
	return Stats{h.requests.Load(), h.shed.Load(), h.rejected.Load(), h.unauthorized.Load(), h.forbidden.Load()}
}

// Admission returns the admission controller (for runtime tuning).
func (h *Handler) Admission() *admission.Admission { return h.adm }

type operation uint8

const (
	opCreate operation = iota + 1
	opUpdate
	opRelease
)

// problem is a ProblemDetails answer. Values are shared constants: never
// mutate them. Causes follow TS 29.500 §5.2.7.2.
type problem struct {
	status int
	cause  string
	detail string
}

var (
	errNotFound     = &problem{404, "", "unknown resource"}
	errRefNotFound  = &problem{404, "", "charging data reference not found"}
	errBadRef       = &problem{404, "", "malformed charging data reference"}
	errMethod       = &problem{405, "", "only POST is supported"}
	errMediaType    = &problem{415, "", "Content-Type must be application/json"}
	errTooLarge     = &problem{413, "", "request body too large"}
	errBadJSON      = &problem{400, "INVALID_MSG_FORMAT", "body is not a valid ChargingDataRequest"}
	errNoNF         = &problem{400, "MANDATORY_IE_MISSING", "nfConsumerIdentification.nodeFunctionality is required"}
	errNoTimestamp  = &problem{400, "MANDATORY_IE_MISSING", "invocationTimeStamp is required"}
	errBadTimestamp = &problem{400, "MANDATORY_IE_INCORRECT", "invocationTimeStamp is not a date-time"}
	errNoSeq        = &problem{400, "MANDATORY_IE_MISSING", "invocationSequenceNumber is required"}
	errNoSub        = &problem{400, "MANDATORY_IE_MISSING", "subscriberIdentifier is required"}
	errBadSub       = &problem{400, "MANDATORY_IE_INCORRECT", "subscriberIdentifier must be an imsi- or nai- SUPI"}
	errUserUnknown  = &problem{404, "USER_UNKNOWN", "subscriber unknown"}
	errSubMismatch  = &problem{400, "MANDATORY_IE_INCORRECT", "subscriberIdentifier does not match the charging data reference"}
	errNoUnits      = &problem{400, "MANDATORY_IE_MISSING", "multipleUnitUsage is required"}
	errNoRG         = &problem{400, "MANDATORY_IE_MISSING", "multipleUnitUsage.ratingGroup is required"}
	errBadUnits     = &problem{400, "MANDATORY_IE_INCORRECT", "mixed or invalid unit types in one unit group"}
	errTooManyUnits = &problem{400, "MANDATORY_IE_INCORRECT", "too many multipleUnitUsage entries"}
	errNoEventType  = &problem{400, "MANDATORY_IE_MISSING", "oneTimeEventType is required when oneTimeEvent is true"}
	errPEC          = &problem{501, "", "only immediate event charging (IEC) is supported"}
	errStale        = &problem{400, "MANDATORY_IE_INCORRECT", "invocationSequenceNumber is older than the last processed request"}
	errUnauthorized = &problem{401, "", "access token missing or invalid"}
	errForbidden    = &problem{403, "", "the access token does not grant access to this service"}
	errBusy         = &problem{429, "", "overloaded"}
	errUnavailable  = &problem{503, "SYSTEM_FAILURE", "charging is temporarily unavailable"}
)

func (h *Handler) route(path string) (op operation, ref string, prefix string, ok bool) {
	for _, p := range h.cfg.APIPrefixes {
		if !strings.HasPrefix(path, p) {
			continue
		}
		rest := path[len(p):]
		switch {
		case rest == "/chargingdata":
			return opCreate, "", p, true
		case strings.HasPrefix(rest, "/chargingdata/"):
			tail := rest[len("/chargingdata/"):]
			i := strings.IndexByte(tail, '/')
			if i <= 0 {
				return 0, "", "", false
			}
			switch tail[i+1:] {
			case "update":
				return opUpdate, tail[:i], p, true
			case "release":
				return opRelease, tail[:i], p, true
			}
		}
	}
	return 0, "", "", false
}

var (
	reqPool = sync.Pool{New: func() any { return &chargingDataRequest{} }}
	bufPool = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, 2048)) }}
	outPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

	ctJSON    = []string{"application/json"}
	ctProblem = []string{"application/problem+json"}
	noStore   = []string{"no-store"}
	allowPOST = []string{http.MethodPost}
	retry1    = []string{"1"}
)

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.requests.Add(1)
	op, ref, prefix, ok := h.route(r.URL.Path)
	if !ok {
		h.fail(w, errNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header()["Allow"] = allowPOST
		h.fail(w, errMethod)
		return
	}
	if ct := r.Header.Get("Content-Type"); len(ct) < 16 || !strings.EqualFold(ct[:16], "application/json") {
		h.fail(w, errMediaType)
		return
	}

	if h.cfg.Auth != nil {
		if p := h.authenticate(w, r); p != nil {
			h.fail(w, p)
			return
		}
	}

	// A reference minted in another region is that region's business.
	var sub string
	if op != opCreate {
		region, s, perr := parseRef(ref)
		if perr != nil {
			h.fail(w, perr)
			return
		}
		if region != h.cfg.Region {
			if base, ok := h.cfg.RegionBaseURLs[region]; ok {
				w.Header().Set("Location", base+r.URL.RequestURI())
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			h.fail(w, errRefNotFound)
			return
		}
		sub = s
	}

	if !h.adm.Acquire() {
		h.shed.Add(1)
		h.fail(w, errBusy)
		return
	}
	defer h.adm.Release()

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if _, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes)); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			h.fail(w, errTooLarge)
		} else {
			h.fail(w, errBadJSON)
		}
		return
	}

	in := reqPool.Get().(*chargingDataRequest)
	defer func() { in.reset(); reqPool.Put(in) }()
	if err := json.Unmarshal(buf.Bytes(), in); err != nil {
		h.fail(w, errBadJSON)
		return
	}

	req := canonical.AcquireRequest()
	resp := canonical.AcquireResponse()
	defer canonical.ReleaseRequest(req)
	defer canonical.ReleaseResponse(resp)

	// The identity lookup and the data-tier call must not be abandoned because
	// the consumer hung up: the charge may already have been applied and the
	// retransmission would replay it. Redis client timeouts bound them instead.
	ctx := context.WithoutCancel(r.Context())
	if p := h.fill(ctx, op, sub, ref, in, req); p != nil {
		h.fail(w, p)
		return
	}

	switch err := h.rater.Rate(ctx, req, resp); {
	case err == nil:
	case errors.Is(err, engine.ErrOverloaded):
		h.shed.Add(1)
		h.fail(w, errBusy)
		return
	case errors.Is(err, engine.ErrInvalid):
		h.fail(w, errBadJSON)
		return
	default:
		h.fail(w, errUnavailable)
		return
	}

	switch resp.Result {
	case canonical.ResultUnknownSession:
		h.fail(w, errRefNotFound)
		return
	case canonical.ResultStaleRequest:
		h.fail(w, errStale)
		return
	case canonical.ResultTooBusy:
		h.shed.Add(1)
		h.fail(w, errBusy)
		return
	}

	hdr := w.Header()
	hdr["Cache-Control"] = noStore
	if op == opRelease {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	hdr["Content-Type"] = ctJSON
	status := http.StatusOK
	if op == opCreate {
		status = http.StatusCreated
		scheme := "http://"
		if r.TLS != nil {
			scheme = "https://"
		}
		hdr["Location"] = []string{scheme + r.Host + prefix + "/chargingdata/" + req.SessionID}
	}
	op2 := outPool.Get().(*[]byte)
	out := encodeResponse((*op2)[:0], h.cfg.Now(), *in.InvocationSequenceNumber, req, resp)
	w.WriteHeader(status)
	_, _ = w.Write(out)
	*op2 = out[:0]
	outPool.Put(op2)
}

const realm = `Bearer realm="nchf-convergedcharging"`

// authenticate checks the bearer token. On failure it sets WWW-Authenticate
// (RFC 6750) and returns the problem to answer with. The client is told
// nothing about which check failed.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) *problem {
	vals := r.Header["Authorization"]
	if len(vals) == 0 {
		h.unauthorized.Add(1)
		w.Header().Set("WWW-Authenticate", realm)
		return errUnauthorized
	}
	var token string
	if len(vals) == 1 && len(vals[0]) > 7 && strings.EqualFold(vals[0][:7], "Bearer ") {
		token = strings.TrimSpace(vals[0][7:])
	}
	_, err := h.cfg.Auth.Verify(token)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, oauth.ErrForbidden):
		h.forbidden.Add(1)
		w.Header().Set("WWW-Authenticate", realm+`, error="insufficient_scope", scope="`+oauth.DefaultScope+`"`)
		return errForbidden
	}
	h.unauthorized.Add(1)
	w.Header().Set("WWW-Authenticate", realm+`, error="invalid_token"`)
	return errUnauthorized
}

// fail writes a ProblemDetails answer.
func (h *Handler) fail(w http.ResponseWriter, p *problem) {
	if p.status >= 400 && p.status < 500 && p.status != http.StatusTooManyRequests &&
		p.status != http.StatusUnauthorized && p.status != http.StatusForbidden {
		h.rejected.Add(1)
	}
	hdr := w.Header()
	hdr["Content-Type"] = ctProblem
	hdr["Cache-Control"] = noStore
	if p.status == http.StatusTooManyRequests || p.status == http.StatusServiceUnavailable {
		hdr["Retry-After"] = retry1
	}
	w.WriteHeader(p.status)
	op := outPool.Get().(*[]byte)
	b := append((*op)[:0], `{"title":"`...)
	b = append(b, http.StatusText(p.status)...)
	b = append(b, `","status":`...)
	b = strconv.AppendInt(b, int64(p.status), 10)
	if p.cause != "" {
		b = append(b, `,"cause":"`...)
		b = append(b, p.cause...)
		b = append(b, '"')
	}
	b = append(b, `,"detail":"`...)
	b = append(b, p.detail...)
	b = append(b, `"}`...)
	_, _ = w.Write(b)
	*op = b[:0]
	outPool.Put(op)
}

// parseRef splits "<region>.<b64 subscriber>.<hex>".
func parseRef(ref string) (region, sub string, p *problem) {
	i := strings.IndexByte(ref, '.')
	if i <= 0 {
		return "", "", errBadRef
	}
	rest := ref[i+1:]
	j := strings.IndexByte(rest, '.')
	if j <= 0 || len(rest)-j-1 != 16 {
		return "", "", errBadRef
	}
	raw, err := base64.RawURLEncoding.DecodeString(rest[:j])
	if err != nil || len(raw) == 0 {
		return "", "", errBadRef
	}
	return ref[:i], string(raw), nil
}

// mintRef derives the reference of a create request.
func mintRef(region, sub string, in *chargingDataRequest) string {
	var tmp [384]byte
	b := append(tmp[:0], sub...)
	for _, s := range [...]string{in.NFConsumer.NodeFunctionality, in.NFConsumer.NFName,
		in.NFConsumer.NFIPv4Address, in.NFConsumer.NFIPv6Address, in.InvocationTimeStamp} {
		b = append(b, 0)
		b = append(b, s...)
	}
	if pc := in.PDUSessionCharging; pc != nil && pc.PDUSessionInformation != nil && pc.PDUSessionInformation.PDUSessionID != nil {
		b = append(b, 0)
		b = strconv.AppendUint(b, uint64(*pc.PDUSessionInformation.PDUSessionID), 10)
	}
	if in.OneTimeEvent {
		b = append(b, 0, 'e')
		b = strconv.AppendUint(b, uint64(*in.InvocationSequenceNumber), 10)
	}
	sum := sha256.Sum256(b)
	var sb strings.Builder
	sb.Grow(len(region) + 2 + base64.RawURLEncoding.EncodedLen(len(sub)) + 16)
	sb.WriteString(region)
	sb.WriteByte('.')
	sb.WriteString(base64.RawURLEncoding.EncodeToString([]byte(sub)))
	sb.WriteByte('.')
	var hx [16]byte
	hex.Encode(hx[:], sum[:8])
	sb.Write(hx[:])
	return sb.String()
}

// resolve maps a SUPI identity to the subscriber key.
func (h *Handler) resolve(ctx context.Context, id identity.Identity) (string, *problem) {
	var ids [1]identity.Identity
	ids[0] = id
	key, err := h.cfg.Identity.Resolve(ctx, ids[:])
	switch {
	case err == nil:
		return key, nil
	case errors.Is(err, identity.ErrUnknown):
		return "", errUserUnknown
	case errors.Is(err, identity.ErrInvalid), errors.Is(err, identity.ErrConflict), errors.Is(err, identity.ErrNoIdentity):
		return "", errBadSub
	}
	return "", errUnavailable
}

func (h *Handler) defaultRequested(k canonical.UnitKind) int64 {
	switch k {
	case canonical.UnitSeconds:
		return h.cfg.DefaultRequestedSeconds
	case canonical.UnitEvents:
		return h.cfg.DefaultRequestedEvents
	}
	return h.cfg.DefaultRequestedBytes
}

// fill validates the request and maps it onto the canonical model.
func (h *Handler) fill(ctx context.Context, op operation, refSub, ref string, in *chargingDataRequest, out *canonical.Request) *problem {
	switch {
	case in.NFConsumer == nil || in.NFConsumer.NodeFunctionality == "":
		return errNoNF
	case in.InvocationTimeStamp == "":
		return errNoTimestamp
	case in.InvocationSequenceNumber == nil:
		return errNoSeq
	}
	ts, err := time.Parse(time.RFC3339, in.InvocationTimeStamp)
	if err != nil {
		return errBadTimestamp
	}

	// Subscriber: mandatory on create; on update/release it comes from the
	// reference and, if repeated in the body, must agree.
	if in.SubscriberIdentifier != "" || op == opCreate {
		if in.SubscriberIdentifier == "" {
			return errNoSub
		}
		sid, ok := parseSUPI(in.SubscriberIdentifier)
		if !ok {
			return errBadSub
		}
		key, perr := h.resolve(ctx, sid)
		if perr != nil {
			return perr
		}
		if op == opCreate {
			refSub = key
		} else if key != refSub {
			return errSubMismatch
		}
	}

	switch op {
	case opCreate:
		if in.OneTimeEvent {
			switch in.OneTimeEventType {
			case "IEC":
				out.Type = canonical.TypeEvent
			case "":
				return errNoEventType
			default:
				return errPEC
			}
		} else {
			out.Type = canonical.TypeInitial
		}
		ref = mintRef(h.cfg.Region, refSub, in)
	case opUpdate:
		out.Type = canonical.TypeUpdate
	case opRelease:
		out.Type = canonical.TypeTerminate
	}

	if len(in.MultipleUnitUsage) > h.cfg.MaxUnits {
		return errTooManyUnits
	}
	for i := range in.MultipleUnitUsage {
		m := &in.MultipleUnitUsage[i]
		if m.RatingGroup == nil {
			return errNoRG
		}
		u := canonical.UnitRequest{RatingGroup: *m.RatingGroup}
		var req, used unitValue
		var ok bool
		if m.RequestedUnit != nil {
			r := m.RequestedUnit
			if req, ok = units(r.Time, r.TotalVolume, r.UplinkVolume, r.DownlinkVolume, r.ServiceSpecificUnits); !ok {
				return errBadUnits
			}
		}
		for j := range m.UsedUnitContainer {
			c := &m.UsedUnitContainer[j]
			v, ok := units(c.Time, c.TotalVolume, c.UplinkVolume, c.DownlinkVolume, c.ServiceSpecificUnits)
			if !ok {
				return errBadUnits
			}
			if c.ServiceID != nil && u.ServiceID == 0 {
				u.ServiceID = *c.ServiceID
			}
			if !v.any {
				continue
			}
			if used.any && used.kind != v.kind {
				return errBadUnits
			}
			sum := used.value + v.value
			if sum > datalayer.MaxUnits {
				sum = datalayer.MaxUnits + 1
			}
			used = unitValue{kind: v.kind, value: sum, any: true}
		}
		switch {
		case req.any && used.any && req.kind != used.kind:
			return errBadUnits
		case req.any:
			u.Kind = req.kind
		case used.any:
			u.Kind = used.kind
		}
		u.Used = used.value
		switch {
		case req.any:
			u.Requested = req.value
		case m.RequestedUnit != nil: // empty requestedUnit: the server picks the amount
			u.Requested = h.defaultRequested(u.Kind)
		}
		out.Units = append(out.Units, u)
	}
	if op != opRelease && len(out.Units) == 0 {
		return errNoUnits
	}

	out.Source = canonical.SourceNchf
	out.SubscriberID = refSub
	out.SessionID = ref
	out.RequestNumber = *in.InvocationSequenceNumber
	out.EventTime = ts
	if pc := in.PDUSessionCharging; pc != nil && pc.PDUSessionInformation != nil {
		pi := pc.PDUSessionInformation
		out.RATType = pi.RatType
		if ni := pi.NetworkSlicingInfo; ni != nil && ni.SNSSAI != nil && ni.SNSSAI.SST != nil {
			out.Slice = strconv.FormatUint(uint64(*ni.SNSSAI.SST), 10)
			if ni.SNSSAI.SD != "" {
				out.Slice += "-" + ni.SNSSAI.SD
			}
		}
	}
	return nil
}
