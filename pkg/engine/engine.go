// Package engine is the real-time rating and credit-control engine.
//
// Requests are partitioned onto a fixed set of worker goroutines by hashing the
// subscriber id, so all requests for one subscriber are processed serially by
// one goroutine: there is no lock on subscriber state in this process, and the
// authoritative balance mutation is a single atomic Lua script in the data
// layer (see pkg/datalayer). Each worker owns reusable scratch buffers, so the
// engine's own path allocates nothing per request once warm.
//
// Pipeline per request: identify subscriber -> price units (catalog) ->
// atomic charge (settle, refund, reserve, session state) -> schedule expiry
// sweep -> emit audit event. When the data tier is unavailable the request
// is handed to the hybrid fail policy instead (pkg/failpolicy).
package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
)

var (
	// ErrOverloaded means the subscriber's worker queue is full; the gateway
	// should answer DIAMETER_TOO_BUSY (3004) / HTTP 429.
	ErrOverloaded = errors.New("engine: overloaded")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("engine: closed")
	// ErrInvalid marks a malformed canonical request.
	ErrInvalid = errors.New("engine: invalid request")

	errBreakerOpen = errors.New("engine: data tier breaker open")
)

// Store is the data-tier surface the engine needs (*datalayer.Client).
type Store interface {
	Charge(ctx context.Context, req *datalayer.Request, resp *datalayer.Response) error
	SweepExpired(ctx context.Context, subscriberID string, now time.Time) (int, error)
}

// UnsettledSink durably records a terminate that is about to be acknowledged without being settled, so its
// usage can be charged later (see package unsettled). Record must not return until the record is safe; an
// error means it is not, and the usage is counted as unrecorded.
type UnsettledSink interface {
	Record(req *canonical.Request) error
}

// AuditKind classifies an audit event.
type AuditKind uint8

const (
	AuditCharged AuditKind = iota
	AuditDegradedGrant
	// AuditUnsettledUsage: usage was reported while the data tier was down and
	// could not be settled; it must be reconciled later.
	AuditUnsettledUsage
)

// AuditEvent is emitted (best effort, non-blocking) for every processed
// request. The authoritative record is the ledger stream in the data tier;
// this feeds Kafka and reconciliation.
type AuditEvent struct {
	Kind         AuditKind
	Type         canonical.RequestType
	SubscriberID string
	SessionID    string
	ReqNo        uint32
	Result       canonical.Result
	Requested    int64
	Used         int64
	Granted      int64
	At           time.Time
}

// Config tunes the engine. Zero values select defaults.
type Config struct {
	Workers    int           // default GOMAXPROCS
	QueueDepth int           // per worker, default 1024
	Validity   time.Duration // grant validity, whole seconds, default 30s
	Grace      time.Duration // extra reservation lifetime beyond validity, default 5s
	Now        func() time.Time
	// PeerMayRepeatUsage says the network elements may report the usage of an update they were refused again
	// (in a later request of the same session, under a new request number). The reconciler then charges a refused
	// update only if the session has not moved past it, and otherwise keeps it for a person, so usage that was
	// repeated is never charged twice at the price of sometimes not charging usage that was not. False (the
	// default) charges it unless the data tier applied that very request, which it can tell exactly for the last 8
	// request numbers of a session; RFC 4006 §9.1 and its client state machine support that for Gy (doc 03).
	PeerMayRepeatUsage bool
}

// Deps are the engine's collaborators. Breaker, Policy and Audit are optional.
type Deps struct {
	Store   Store
	Tariff  Tariff
	Breaker *failpolicy.Breaker
	Policy  *failpolicy.Engine
	Audit   chan<- AuditEvent
	// Unsettled, when set, receives every terminate that reported usage and was acknowledged while the data
	// tier was unavailable. Without it that usage is lost (and counted as unrecorded).
	Unsettled UnsettledSink
	// Latency, when set, records how long each Rate call took (queueing plus
	// processing), including rejected ones.
	Latency *telemetry.Histogram
}

// Stats are cumulative counters.
type Stats struct {
	Requests     uint64
	Degraded     uint64 // answered without the data tier
	Overloaded   uint64
	AuditDropped uint64
	// Unreplicated counts committed steps that finished with fewer replica
	// acknowledgements than required (see datalayer.WithDurability).
	Unreplicated uint64
	// Unpriced counts units the tariff could not price (barred by a policy
	// filter, no matching rule, unknown plan). They are sent with no rates.
	Unpriced uint64
	// UnsettledTerminates counts terminates that reported usage and were acknowledged while the data tier was
	// unavailable: the session is released but the usage it reported was not
	// charged (its reservation expires and is refunded). Each one is revenue
	// that only a reconciler could recover; see AuditUnsettledUsage.
	UnsettledTerminates uint64
	// UnsettledRecorded / UnsettledUnrecorded split UnsettledTerminates: those saved for reconciliation, and
	// those whose usage is lost because no sink was configured or writing the record failed.
	UnsettledRecorded   uint64
	UnsettledUnrecorded uint64
	// UnsettledUpdates counts updates that reported usage and were refused because the data tier was unavailable:
	// that usage was not charged either. They are recorded (or not) exactly like the terminates above and are
	// included in UnsettledRecorded / UnsettledUnrecorded.
	UnsettledUpdates uint64
}

type job struct {
	ctx    context.Context
	req    *canonical.Request
	resp   *canonical.Response
	err    error
	replay bool // a reconciliation replay: never answered by the fail policy, errors go back to the caller
	done   chan struct{}
}

var jobPool = sync.Pool{New: func() any { return &job{done: make(chan struct{}, 1)} }}

// Engine is safe for concurrent use.
type Engine struct {
	cfg     Config
	deps    Deps
	workers []*worker

	stop        chan struct{}
	workersDone chan struct{}
	closed      atomic.Bool
	wg          sync.WaitGroup

	requests, degraded, overloaded, auditDropped, unreplicated, unpriced, unsettled, unsettledRecorded, unsettledUnrecorded, unsettledUpdates atomic.Uint64
}

// New builds and starts an engine.
func New(cfg Config, deps Deps) (*Engine, error) {
	if deps.Store == nil || deps.Tariff == nil {
		return nil, errors.New("engine: Store and Tariff are required")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 1024
	}
	if cfg.Validity <= 0 {
		cfg.Validity = 30 * time.Second
	}
	cfg.Validity = cfg.Validity.Truncate(time.Second)
	if cfg.Grace <= 0 {
		cfg.Grace = 5 * time.Second
	}
	if cfg.Validity > 30*time.Minute {
		return nil, errors.New("engine: Validity must be <= 30m")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	e := &Engine{cfg: cfg, deps: deps, stop: make(chan struct{}), workersDone: make(chan struct{})}
	span := 2*(cfg.Validity+cfg.Grace) + 10*time.Second
	for i := 0; i < cfg.Workers; i++ {
		w := &worker{e: e, in: make(chan *job, cfg.QueueDepth), wheel: newWheel(span, cfg.Now())}
		e.workers = append(e.workers, w)
		e.wg.Add(1)
		go w.run()
	}
	go func() { e.wg.Wait(); close(e.workersDone) }()
	return e, nil
}

// Close stops the workers. Queued requests fail with ErrClosed.
func (e *Engine) Close() {
	if e.closed.CompareAndSwap(false, true) {
		close(e.stop)
	}
	<-e.workersDone
}

// Stats returns a snapshot of the counters.
func (e *Engine) Stats() Stats {
	return Stats{e.requests.Load(), e.degraded.Load(), e.overloaded.Load(), e.auditDropped.Load(), e.unreplicated.Load(), e.unpriced.Load(), e.unsettled.Load(), e.unsettledRecorded.Load(), e.unsettledUnrecorded.Load(), e.unsettledUpdates.Load()}
}

// Rate processes one request and fills resp. It blocks until the answer is
// ready; ctx bounds the data-tier call. req and resp are only touched by the
// engine until Rate returns, so callers may pool them.
//
// Errors: ErrOverloaded (shed it), ErrClosed, ErrInvalid. Data-tier trouble is
// not an error: it is answered by the fail policy (resp.Result tells which).
func (e *Engine) Rate(ctx context.Context, req *canonical.Request, resp *canonical.Response) error {
	if h := e.deps.Latency; h != nil {
		start := time.Now()
		defer func() { h.Observe(time.Since(start)) }()
	}
	if e.closed.Load() {
		return ErrClosed
	}
	j := jobPool.Get().(*job)
	j.ctx, j.req, j.resp, j.err = ctx, req, resp, nil
	w := e.workers[fnv32(req.SubscriberID)%uint32(len(e.workers))]
	select {
	case w.in <- j:
	default:
		e.overloaded.Add(1)
		jobPool.Put(j)
		return ErrOverloaded
	}
	select {
	case <-j.done:
	case <-e.workersDone:
		select {
		case <-j.done:
		default:
			return ErrClosed // job abandoned with the workers; do not recycle
		}
	}
	err := j.err
	j.ctx, j.req, j.resp = nil, nil, nil
	jobPool.Put(j)
	return err
}

// Settle submits a terminate, or an update that was refused, again, as the unsettled-usage reconciler does after
// the data tier was down when it first arrived. An update is settled after the fact without reserving anything
// (datalayer.ModeLateExact, or ModeLateStrict with PeerMayRepeatUsage), and never when the data tier already applied it. Unlike Rate it is never answered by the fail policy: when the data tier cannot take the
// request it returns the error and the caller keeps the record. The charge is idempotent per session and
// request number, so a terminate that did reach the data tier the first time is answered from its stored
// result and charges nothing twice. The result is ResultUnknownSession or ResultStaleRequest when the session
// can no longer take the usage; any other result means it was settled.
func (e *Engine) Settle(ctx context.Context, req *canonical.Request) (canonical.Result, error) {
	if req.Type != canonical.TypeTerminate && req.Type != canonical.TypeUpdate {
		return 0, ErrInvalid
	}
	if e.closed.Load() {
		return 0, ErrClosed
	}
	resp := canonical.AcquireResponse()
	defer canonical.ReleaseResponse(resp)
	j := jobPool.Get().(*job)
	j.ctx, j.req, j.resp, j.err, j.replay = ctx, req, resp, nil, true
	w := e.workers[fnv32(req.SubscriberID)%uint32(len(e.workers))]
	select {
	case w.in <- j:
	default:
		j.replay = false
		jobPool.Put(j)
		return 0, ErrOverloaded
	}
	select {
	case <-j.done:
	case <-e.workersDone:
		select {
		case <-j.done:
		default:
			return 0, ErrClosed
		}
	}
	err := j.err
	j.ctx, j.req, j.resp, j.replay = nil, nil, nil, false
	jobPool.Put(j)
	return resp.Result, err
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

type worker struct {
	e     *Engine
	in    chan *job
	wheel *wheel

	// Scratch reused across requests; only this goroutine touches them.
	dreq    datalayer.Request
	dres    datalayer.Response
	rateBuf []datalayer.Rate
	offs    []int
}

func (w *worker) run() {
	defer w.e.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case j := <-w.in:
			w.handle(j)
		case <-tick.C:
			w.sweepDue()
		case <-w.e.stop:
			for {
				select {
				case j := <-w.in:
					j.err = ErrClosed
					j.done <- struct{}{}
				default:
					return
				}
			}
		}
	}
}

func modeOf(t canonical.RequestType) (datalayer.Mode, bool) {
	switch t {
	case canonical.TypeInitial:
		return datalayer.ModeInitial, true
	case canonical.TypeUpdate:
		return datalayer.ModeUpdate, true
	case canonical.TypeTerminate:
		return datalayer.ModeTerminate, true
	case canonical.TypeEvent:
		return datalayer.ModeEvent, true
	}
	return 0, false
}

func (w *worker) handle(j *job) {
	defer func() { j.done <- struct{}{} }()
	e := w.e
	e.requests.Add(1)
	req, resp := j.req, j.resp
	resp.Reset()
	resp.SessionID = req.SessionID

	mode, ok := modeOf(req.Type)
	if j.replay && req.Type == canonical.TypeUpdate {
		mode = datalayer.ModeLateExact
		if e.cfg.PeerMayRepeatUsage {
			mode = datalayer.ModeLateStrict
		}
	}
	if !ok || req.SubscriberID == "" || req.SessionID == "" ||
		(len(req.Units) == 0 && req.Type != canonical.TypeTerminate) {
		j.err = ErrInvalid
		return
	}
	now := e.cfg.Now()
	if err := w.build(req, mode, now); err != nil {
		j.err = err
		return
	}

	var err error
	br := e.deps.Breaker
	if j.replay {
		if br != nil && !br.AllowDataTier() {
			j.err = errBreakerOpen
			return
		}
		err = e.deps.Store.Charge(j.ctx, &w.dreq, &w.dres)
		if br != nil && j.ctx.Err() == nil {
			br.Record(err)
		}
		if errors.Is(err, datalayer.ErrInvalid) {
			j.err = ErrInvalid
			return
		}
		if err != nil {
			j.err = err
			return
		}
		w.fill(req, resp)
		return
	}
	if br == nil || br.AllowDataTier() {
		err = e.deps.Store.Charge(j.ctx, &w.dreq, &w.dres)
		if errors.Is(err, datalayer.ErrInvalid) {
			j.err = ErrInvalid
			return
		}
		if br != nil && j.ctx.Err() == nil {
			br.Record(err)
		}
	} else {
		err = errBreakerOpen
	}

	if err != nil {
		// Never sent: the breaker kept the call from being made, or the connection could not be established.
		w.degrade(req, resp, now)
		return
	}
	if w.dres.Replication == datalayer.ReplicationShort {
		e.unreplicated.Add(1)
	}
	w.fill(req, resp)
	if mode == datalayer.ModeInitial || mode == datalayer.ModeUpdate {
		for i := range resp.Grants {
			if resp.Grants[i].Granted > 0 && w.dres.Code == datalayer.CodeOK {
				w.wheel.add(req.SubscriberID, now.Add(e.cfg.Validity+e.cfg.Grace+time.Second))
				break
			}
		}
	}
	w.audit(AuditCharged, req, resp, now)
}

// build fills w.dreq from the canonical request, pricing each unit.
func (w *worker) build(req *canonical.Request, mode datalayer.Mode, now time.Time) error {
	w.rateBuf = w.rateBuf[:0]
	w.offs = w.offs[:0]
	for i := range req.Units {
		w.offs = append(w.offs, len(w.rateBuf))
		var err error
		w.rateBuf, err = w.e.deps.Tariff.Rates(req, &req.Units[i], w.rateBuf)
		if errors.Is(err, ErrNoTariff) {
			// An unpriceable unit gets no rates rather than failing the whole
			// request: nothing new can be reserved for it, but usage already
			// reserved is still settled and any excess is reported as a
			// shortfall, so a terminate can never lose its settlement.
			w.rateBuf = w.rateBuf[:w.offs[i]]
			w.e.unpriced.Add(1)
			continue
		}
		if err != nil {
			return err
		}
	}
	w.offs = append(w.offs, len(w.rateBuf))

	d := &w.dreq
	d.SubscriberID, d.SessionID, d.Mode, d.ReqNo, d.Now = req.SubscriberID, req.SessionID, mode, req.RequestNumber, now
	d.Validity, d.Grace = w.e.cfg.Validity, w.e.cfg.Grace
	d.AcceptUnknownSession = req.AcceptUnknownSession && mode != datalayer.ModeInitial && mode != datalayer.ModeEvent
	d.Items = d.Items[:0]
	for i := range req.Units {
		u := &req.Units[i]
		lo, hi := w.offs[i], w.offs[i+1]
		d.Items = append(d.Items, datalayer.Item{
			RatingGroup: u.RatingGroup, Kind: u.Kind,
			Requested: u.Requested, Used: u.Used,
			Rates: w.rateBuf[lo:hi:hi],
		})
	}
	return nil
}

var itemResult = [...]canonical.Result{
	datalayer.ItemGranted:      canonical.ResultGranted,
	datalayer.ItemPartialFinal: canonical.ResultPartialFinal,
	datalayer.ItemLimitReached: canonical.ResultCreditLimitReached,
	datalayer.ItemNoBucket:     canonical.ResultNoBucket,
	datalayer.ItemBarred:       canonical.ResultBarred,
}

// fill maps the data-layer answer onto the canonical response.
func (w *worker) fill(req *canonical.Request, resp *canonical.Response) {
	switch w.dres.Code {
	case datalayer.CodeUnknownSession:
		resp.Result = canonical.ResultUnknownSession
		return
	case datalayer.CodeStale:
		resp.Result = canonical.ResultStaleRequest
		return
	}
	anyGranted, allGranted := false, true
	first := canonical.ResultUnspecified
	for i, it := range w.dres.Items {
		r := canonical.ResultNoBucket
		if int(it.Code) < len(itemResult) {
			r = itemResult[it.Code]
		}
		// A unit that asked for nothing (e.g. an update that only reports
		// usage) has nothing to be refused.
		if i < len(req.Units) && req.Units[i].Requested == 0 && req.Type != canonical.TypeTerminate && it.Granted == 0 {
			r = canonical.ResultGranted
		}
		resp.Grants = append(resp.Grants, canonical.UnitGrant{
			RatingGroup: it.RatingGroup, Result: r, Granted: it.Granted,
			ValiditySeconds: it.ValidityS, FinalUnit: it.Final, Shortfall: it.Shortfall,
		})
		switch {
		case it.Granted > 0:
			anyGranted = true
		case r == canonical.ResultGranted: // asked for nothing
		default:
			allGranted = false
			if first == canonical.ResultUnspecified {
				first = r
			}
		}
	}
	switch req.Type {
	case canonical.TypeTerminate:
		resp.Result = canonical.ResultGranted
	case canonical.TypeEvent:
		if allGranted {
			resp.Result = canonical.ResultGranted
		} else {
			resp.Result = first
		}
	default:
		if anyGranted || first == canonical.ResultUnspecified {
			resp.Result = canonical.ResultGranted
		} else {
			resp.Result = first
		}
	}
}

func policyKind(k canonical.UnitKind) failpolicy.UnitKind {
	switch k {
	case canonical.UnitSeconds:
		return failpolicy.UnitSeconds
	case canonical.UnitEvents:
		return failpolicy.UnitEvents
	}
	return failpolicy.UnitBytes
}

// degrade answers a request without the data tier, via the hybrid fail policy:
// prepaid (all of it) fails closed; postpaid gets a capped, journalled grant.
func (w *worker) degrade(req *canonical.Request, resp *canonical.Response, now time.Time) {
	e := w.e
	e.degraded.Add(1)
	resp.Result = canonical.ResultUnavailable
	if req.Type == canonical.TypeUpdate {
		w.saveUnsettledUpdate(req)
	}

	if req.Type == canonical.TypeTerminate {
		// Nothing can be settled now. Acknowledge so the network element can
		// release the session, and flag the usage for reconciliation.
		resp.Result = canonical.ResultGranted
		for i := range req.Units {
			u := &req.Units[i]
			resp.Grants = append(resp.Grants, canonical.UnitGrant{RatingGroup: u.RatingGroup, Result: canonical.ResultGranted, Degraded: true})
		}
		for i := range req.Units {
			if req.Units[i].Used > 0 { // a terminate that reported no usage loses nothing
				e.unsettled.Add(1)
				if sink := e.deps.Unsettled; sink != nil && sink.Record(req) == nil {
					e.unsettledRecorded.Add(1)
				} else {
					e.unsettledUnrecorded.Add(1)
				}
				break
			}
		}
		w.audit(AuditUnsettledUsage, req, resp, now)
		return
	}

	anyGranted := false
	for i := range req.Units {
		u := &req.Units[i]
		g := canonical.UnitGrant{RatingGroup: u.RatingGroup, Result: canonical.ResultUnavailable, Degraded: true}
		if e.deps.Policy != nil {
			d := e.deps.Policy.Decide(failpolicy.Request{
				SubscriberID: req.SubscriberID, SessionID: req.SessionID,
				Kind: policyKind(u.Kind), Requested: u.Requested,
			})
			// Events are all-or-nothing: a partial fail-open grant is useless.
			if d.Allowed() && (req.Type != canonical.TypeEvent || d.Granted == u.Requested) {
				g.Result, g.Granted = canonical.ResultGranted, d.Granted
				g.ValiditySeconds = uint32(d.Validity / time.Second)
				g.FinalUnit = d.FinalUnit
				if d.Granted < u.Requested {
					g.Result = canonical.ResultPartialFinal
					g.FinalUnit = true
				}
				anyGranted = true
			}
		}
		resp.Grants = append(resp.Grants, g)
	}
	if anyGranted {
		resp.Result = canonical.ResultGranted
		w.audit(AuditDegradedGrant, req, resp, now)
	}
}

// saveUnsettledUpdate records the usage of an update that is about to be refused for want of the data tier: the
// network element reports usage since its last measurement and will not report it again, so without a record
// it is never charged.
func (w *worker) saveUnsettledUpdate(req *canonical.Request) {
	e := w.e
	for i := range req.Units {
		if req.Units[i].Used > 0 {
			e.unsettledUpdates.Add(1)
			if sink := e.deps.Unsettled; sink != nil && sink.Record(req) == nil {
				e.unsettledRecorded.Add(1)
			} else {
				e.unsettledUnrecorded.Add(1)
			}
			return
		}
	}
}

func (w *worker) audit(kind AuditKind, req *canonical.Request, resp *canonical.Response, now time.Time) {
	ch := w.e.deps.Audit
	if ch == nil {
		return
	}
	ev := AuditEvent{Kind: kind, Type: req.Type, SubscriberID: req.SubscriberID,
		SessionID: req.SessionID, ReqNo: req.RequestNumber, Result: resp.Result, At: now}
	for i := range req.Units {
		ev.Requested += req.Units[i].Requested
		ev.Used += req.Units[i].Used
	}
	for i := range resp.Grants {
		ev.Granted += resp.Grants[i].Granted
	}
	select {
	case ch <- ev:
	default:
		w.e.auditDropped.Add(1)
	}
}

// sweepDue refunds reservations that expired because the network element
// silently dropped the session.
func (w *worker) sweepDue() {
	now := w.e.cfg.Now()
	w.wheel.advance(now, func(sub string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		n, err := w.e.deps.Store.SweepExpired(ctx, sub, now)
		switch {
		case err != nil:
			w.wheel.add(sub, now.Add(5*time.Second)) // retry later
		case n >= 64:
			w.wheel.add(sub, now.Add(time.Second)) // more are due
		}
	})
}
