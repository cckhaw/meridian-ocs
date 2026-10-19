package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
)

var tariff = StaticTariff{Cash: map[canonical.UnitKind]Frac{canonical.UnitBytes: {Num: 2, Den: 1}}}

func newReq(typ canonical.RequestType, sub, sid string, n uint32, requested, used int64) *canonical.Request {
	r := canonical.AcquireRequest()
	r.Type, r.SubscriberID, r.SessionID, r.RequestNumber = typ, sub, sid, n
	r.Units = append(r.Units, canonical.UnitRequest{RatingGroup: 100, Kind: canonical.UnitBytes, Requested: requested, Used: used})
	return r
}

func rate(t testing.TB, e *Engine, r *canonical.Request) *canonical.Response {
	t.Helper()
	resp := canonical.AcquireResponse()
	if err := e.Rate(context.Background(), r, resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestEndToEndGyLifecycle(t *testing.T) {
	rdb := redistest.Start(t)
	store := datalayer.New(rdb)
	ctx := context.Background()
	if err := store.PutBucket(ctx, "u1", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1}); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Workers: 4}, Deps{Store: store, Tariff: tariff})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	r := rate(t, e, newReq(canonical.TypeInitial, "u1", "s", 0, 1000, 0))
	if r.Result != canonical.ResultGranted || r.Grants[0].Granted != 1000 || r.Grants[0].ValiditySeconds != 30 {
		t.Fatalf("%+v", r)
	}
	r = rate(t, e, newReq(canonical.TypeUpdate, "u1", "s", 1, 1000, 400))
	if r.Result != canonical.ResultGranted {
		t.Fatalf("%+v", r)
	}
	r = rate(t, e, newReq(canonical.TypeTerminate, "u1", "s", 2, 0, 300))
	if r.Result != canonical.ResultGranted {
		t.Fatalf("%+v", r)
	}
	if got, _ := store.Balance(ctx, "u1", "bundle"); got != 5000-700 {
		t.Fatalf("balance %d", got)
	}

	r = rate(t, e, newReq(canonical.TypeUpdate, "u1", "ghost", 1, 10, 0))
	if r.Result != canonical.ResultUnknownSession {
		t.Fatalf("%+v", r)
	}
	r = rate(t, e, newReq(canonical.TypeInitial, "u1", "s", 0, 10, 0))
	if r.Result != canonical.ResultStaleRequest {
		t.Fatalf("reusing a closed session id must be stale, got %+v", r)
	}
}

func TestCreditLimitAndPartialGrant(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "u2", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 300, Order: 1})
	e, _ := New(Config{Workers: 2}, Deps{Store: store, Tariff: tariff})
	defer e.Close()

	r := rate(t, e, newReq(canonical.TypeInitial, "u2", "a", 0, 1000, 0))
	if g := r.Grants[0]; r.Result != canonical.ResultGranted || g.Granted != 300 || !g.FinalUnit || g.Result != canonical.ResultPartialFinal {
		t.Fatalf("%+v", r)
	}
	r = rate(t, e, newReq(canonical.TypeInitial, "u2", "b", 0, 1000, 0))
	if r.Result != canonical.ResultCreditLimitReached {
		t.Fatalf("%+v", r)
	}
}

func TestInvalidRequests(t *testing.T) {
	e, _ := New(Config{Workers: 1}, Deps{Store: &stubStore{}, Tariff: tariff})
	defer e.Close()
	for name, r := range map[string]*canonical.Request{
		"no type":    {SubscriberID: "u", SessionID: "s", Units: []canonical.UnitRequest{{Requested: 1}}},
		"no sub":     {Type: canonical.TypeInitial, SessionID: "s", Units: []canonical.UnitRequest{{Requested: 1}}},
		"no units":   {Type: canonical.TypeInitial, SubscriberID: "u", SessionID: "s"},
		"no session": {Type: canonical.TypeInitial, SubscriberID: "u", Units: []canonical.UnitRequest{{Requested: 1}}},
	} {
		if err := e.Rate(context.Background(), r, &canonical.Response{}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// stubStore lets tests script the data tier without Redis.
type stubStore struct {
	mu      sync.Mutex
	calls   atomic.Int64
	err     error
	block   chan struct{}
	started chan struct{}
	sweeps  atomic.Int64
	result  func(*datalayer.Request, *datalayer.Response)
	replace bool // result builds the whole answer instead of adding to the default one
}

func (s *stubStore) Charge(_ context.Context, req *datalayer.Request, resp *datalayer.Response) error {
	s.calls.Add(1)
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	resp.Code = datalayer.CodeOK
	resp.Replication = datalayer.ReplicationNotRequested
	if s.result != nil && !s.replace {
		s.result(req, resp)
	}
	resp.Items = resp.Items[:0]
	if s.replace && s.result != nil {
		s.result(req, resp)
		return nil
	}
	for i := range req.Items {
		resp.Items = append(resp.Items, datalayer.ItemResult{RatingGroup: req.Items[i].RatingGroup, Granted: req.Items[i].Requested, ValidityS: 30})
	}
	return nil
}

func (s *stubStore) SweepExpired(context.Context, string, time.Time) (int, error) {
	s.sweeps.Add(1)
	return 0, nil
}

func (s *stubStore) setErr(err error) { s.mu.Lock(); s.err = err; s.mu.Unlock() }

func TestOverloadShedsInsteadOfQueueing(t *testing.T) {
	st := &stubStore{block: make(chan struct{}), started: make(chan struct{}, 1)}
	e, _ := New(Config{Workers: 1, QueueDepth: 1}, Deps{Store: st, Tariff: tariff})
	defer func() { close(st.block); e.Close() }()

	var wg sync.WaitGroup
	call := func() error {
		return e.Rate(context.Background(), newReq(canonical.TypeInitial, "u", "s", 0, 1, 0), &canonical.Response{})
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = call() }()
	<-st.started // worker is now stuck inside Charge
	wg.Add(1)
	go func() { defer wg.Done(); _ = call() }() // fills the queue
	deadline := time.Now().Add(2 * time.Second)
	for len(e.workers[0].in) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := call(); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("want ErrOverloaded, got %v", err)
	}
	if e.Stats().Overloaded != 1 {
		t.Fatalf("%+v", e.Stats())
	}
	st.block <- struct{}{}
	st.block <- struct{}{}
	wg.Wait()
}

type profiles map[string]failpolicy.Profile

func (p profiles) Lookup(id string) (failpolicy.Profile, bool) { v, ok := p[id]; return v, ok }

func TestDataTierDownUsesHybridPolicy(t *testing.T) {
	st := &stubStore{}
	st.setErr(errors.New("redis down"))
	journal := failpolicy.NewChanJournal(16)
	pol := failpolicy.NewEngine(profiles{
		"prepaid":  {Payment: failpolicy.Prepaid, Status: failpolicy.StatusActive},
		"postpaid": {Payment: failpolicy.Postpaid, Status: failpolicy.StatusActive},
	}, journal, nil)
	var pe *failpolicy.Engine = pol
	br := failpolicy.NewBreaker(1, time.Hour, pe.BeginOutage, nil)
	audit := make(chan AuditEvent, 16)
	e, _ := New(Config{Workers: 2}, Deps{Store: st, Tariff: tariff, Breaker: br, Policy: pol, Audit: audit})
	defer e.Close()

	r := rate(t, e, newReq(canonical.TypeInitial, "prepaid", "a", 0, 1000, 0))
	if r.Result != canonical.ResultUnavailable || r.Grants[0].Granted != 0 || !r.Grants[0].Degraded {
		t.Fatalf("prepaid must fail closed: %+v", r)
	}
	if !br.Open() {
		t.Fatal("breaker should have opened")
	}
	callsAfterOpen := st.calls.Load()

	r = rate(t, e, newReq(canonical.TypeInitial, "postpaid", "b", 0, 1000, 0))
	g := r.Grants[0]
	if r.Result != canonical.ResultGranted || !g.Degraded || g.Granted != 1000 || g.ValiditySeconds != 30 {
		t.Fatalf("postpaid must fail open with a cap: %+v", r)
	}
	if st.calls.Load() != callsAfterOpen {
		t.Fatal("open breaker must not call the data tier")
	}
	if gr := <-journal.C; gr.SubscriberID != "postpaid" || gr.Units != 1000 {
		t.Fatalf("grant not journalled: %+v", gr)
	}

	// Unknown subscribers are denied.
	r = rate(t, e, newReq(canonical.TypeInitial, "stranger", "c", 0, 10, 0))
	if r.Result != canonical.ResultUnavailable {
		t.Fatalf("%+v", r)
	}

	// Terminate during the outage is acknowledged and flagged for reconciliation.
	r = rate(t, e, newReq(canonical.TypeTerminate, "postpaid", "b", 1, 0, 700))
	if r.Result != canonical.ResultGranted {
		t.Fatalf("%+v", r)
	}
	var kinds []AuditKind
	for len(audit) > 0 {
		kinds = append(kinds, (<-audit).Kind)
	}
	want := []AuditKind{AuditDegradedGrant, AuditUnsettledUsage}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("audit kinds %v, want %v", kinds, want)
	}
	if e.Stats().Degraded != 4 {
		t.Fatalf("%+v", e.Stats())
	}
}

func TestBreakerRecoversAfterProbe(t *testing.T) {
	st := &stubStore{}
	st.setErr(errors.New("down"))
	br := failpolicy.NewBreaker(1, 20*time.Millisecond, nil, nil)
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tariff, Breaker: br})
	defer e.Close()

	rate(t, e, newReq(canonical.TypeInitial, "u", "a", 0, 10, 0))
	if !br.Open() {
		t.Fatal("expected open")
	}
	st.setErr(nil)
	time.Sleep(40 * time.Millisecond)
	r := rate(t, e, newReq(canonical.TypeInitial, "u", "b", 0, 10, 0))
	if br.Open() || r.Result != canonical.ResultGranted || r.Grants[0].Degraded {
		t.Fatalf("open=%v %+v", br.Open(), r)
	}
}

func TestSilentlyDroppedSessionIsRefundedByWheel(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "u3", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	e, _ := New(Config{Workers: 1, Validity: time.Second, Grace: time.Millisecond}, Deps{Store: store, Tariff: tariff})
	defer e.Close()

	rate(t, e, newReq(canonical.TypeInitial, "u3", "dead", 0, 800, 0))
	if got, _ := store.Balance(ctx, "u3", "b"); got != 200 {
		t.Fatal(got)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := store.Balance(ctx, "u3", "b"); got == 1000 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	got, _ := store.Balance(ctx, "u3", "b")
	t.Fatalf("reservation was never refunded: balance %d", got)
}

func TestConcurrentSubscribersRace(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	const subs = 20
	for i := 0; i < subs; i++ {
		_ = store.PutBucket(ctx, fmt.Sprintf("c%d", i), datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1_000_000, Order: 1})
	}
	e, _ := New(Config{Workers: 4}, Deps{Store: store, Tariff: tariff})
	defer e.Close()

	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub, sid := fmt.Sprintf("c%d", i), "s"
			resp := canonical.AcquireResponse()
			do := func(r *canonical.Request) {
				if err := e.Rate(ctx, r, resp); err != nil {
					t.Error(err)
				}
				canonical.ReleaseRequest(r)
			}
			do(newReq(canonical.TypeInitial, sub, sid, 0, 100, 0))
			for n := uint32(1); n <= 50; n++ {
				do(newReq(canonical.TypeUpdate, sub, sid, n, 100, 100))
			}
			do(newReq(canonical.TypeTerminate, sub, sid, 51, 0, 100))
		}(i)
	}
	wg.Wait()
	for i := 0; i < subs; i++ {
		if got, _ := store.Balance(ctx, fmt.Sprintf("c%d", i), "b"); got != 1_000_000-51*100 {
			t.Errorf("c%d balance %d", i, got)
		}
	}
}

func TestCloseFailsPendingAndFutureRequests(t *testing.T) {
	e, _ := New(Config{Workers: 1}, Deps{Store: &stubStore{}, Tariff: tariff})
	e.Close()
	if err := e.Rate(context.Background(), newReq(canonical.TypeInitial, "u", "s", 0, 1, 0), &canonical.Response{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestWheel(t *testing.T) {
	t0 := time.Unix(1000, 0)
	w := newWheel(60*time.Second, t0)
	w.add("a", t0.Add(5*time.Second))
	w.add("b", t0.Add(-time.Second)) // past: next tick
	w.add("c", t0.Add(time.Hour))    // beyond span: clamped
	var got []string
	run := func(at time.Time) { w.advance(at, func(s string) { got = append(got, s) }) }
	run(t0.Add(time.Second))
	if fmt.Sprint(got) != "[b]" {
		t.Fatalf("%v", got)
	}
	run(t0.Add(4 * time.Second))
	if len(got) != 1 {
		t.Fatalf("a fired early: %v", got)
	}
	run(t0.Add(5 * time.Second))
	if fmt.Sprint(got) != "[b a]" {
		t.Fatalf("%v", got)
	}
	run(t0.Add(10 * time.Minute)) // long pause: clamped entry fires
	if fmt.Sprint(got) != "[b a c]" {
		t.Fatalf("%v", got)
	}
}

// The engine's own path (everything except the data-tier call) must not allocate.
func TestEnginePathDoesNotAllocate(t *testing.T) {
	st := &stubStore{}
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tariff})
	defer e.Close()
	req := newReq(canonical.TypeUpdate, "u", "s", 1, 100, 100)
	resp := canonical.AcquireResponse()
	ctx := context.Background()
	for i := 0; i < 100; i++ { // warm pools and scratch buffers
		_ = e.Rate(ctx, req, resp)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		_ = e.Rate(ctx, req, resp)
	})
	if allocs > 0 {
		t.Fatalf("engine path allocates %.1f times per request", allocs)
	}
}

func BenchmarkRateStub(b *testing.B) {
	e, _ := New(Config{Workers: 4}, Deps{Store: &stubStore{}, Tariff: tariff})
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		req := newReq(canonical.TypeUpdate, "u", "s", 1, 100, 100)
		resp := canonical.AcquireResponse()
		for pb.Next() {
			_ = e.Rate(ctx, req, resp)
		}
	})
}

func TestUsageOnlyUpdateIsNotRefused(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "u9", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	e, _ := New(Config{Workers: 1}, Deps{Store: store, Tariff: tariff})
	defer e.Close()

	rate(t, e, newReq(canonical.TypeInitial, "u9", "s", 0, 400, 0))
	// Reports 100 used but asks for no more quota.
	r := rate(t, e, newReq(canonical.TypeUpdate, "u9", "s", 1, 0, 100))
	if r.Result != canonical.ResultGranted || r.Grants[0].Result != canonical.ResultGranted || r.Grants[0].Granted != 0 {
		t.Fatalf("%+v", r)
	}
	if got, _ := store.Balance(ctx, "u9", "b"); got != 1000-100 {
		t.Fatalf("balance %d", got)
	}
}

func TestUnreplicatedCommitsAreCounted(t *testing.T) {
	st := &stubStore{}
	st.result = func(_ *datalayer.Request, resp *datalayer.Response) { resp.Replication = datalayer.ReplicationShort }
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tariff})
	defer e.Close()
	rate(t, e, newReq(canonical.TypeUpdate, "u", "s", 1, 10, 10))
	rate(t, e, newReq(canonical.TypeUpdate, "u", "s", 2, 10, 10))
	if got := e.Stats().Unreplicated; got != 2 {
		t.Fatalf("%d", got)
	}
}

func TestBarredSubscribersAreReportedAsSuch(t *testing.T) {
	st := &stubStore{}
	st.result = func(req *datalayer.Request, resp *datalayer.Response) {
		for i := range req.Items {
			resp.Items = append(resp.Items, datalayer.ItemResult{RatingGroup: req.Items[i].RatingGroup, Code: datalayer.ItemBarred})
		}
	}
	// The stub appends the barred items after its default ones; use only ours.
	st.replace = true
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tariff})
	defer e.Close()
	r := rate(t, e, newReq(canonical.TypeInitial, "u", "s", 0, 100, 0))
	if r.Result != canonical.ResultBarred || r.Grants[0].Result != canonical.ResultBarred || r.Grants[0].Granted != 0 {
		t.Fatalf("%+v", r)
	}
	// Terminating is still acknowledged.
	if r = rate(t, e, newReq(canonical.TypeTerminate, "u", "s", 1, 0, 50)); r.Result != canonical.ResultGranted {
		t.Fatalf("%+v", r)
	}
}

type tariffFunc func(*canonical.Request, *canonical.UnitRequest, []datalayer.Rate) ([]datalayer.Rate, error)

func (f tariffFunc) Rates(r *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, error) {
	return f(r, u, dst)
}

// A unit the tariff cannot price must not fail the request: the other units
// are priced normally and the unpriced one reaches the data tier with no rates,
// so a terminate still settles what was already reserved.
func TestUnpricedUnitDoesNotFailTheRequest(t *testing.T) {
	var seen [][]int
	st := &stubStore{}
	st.result = func(req *datalayer.Request, _ *datalayer.Response) {
		var lens []int
		for _, it := range req.Items {
			lens = append(lens, len(it.Rates))
		}
		seen = append(seen, lens)
	}
	tf := tariffFunc(func(r *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, error) {
		if u.RatingGroup == 200 {
			// A tariff may have appended before failing; the engine must drop it.
			return append(dst, datalayer.Rate{Class: datalayer.ClassPromo, Num: 1, Den: 1}), fmt.Errorf("%w: barred", ErrNoTariff)
		}
		return tariff.Rates(r, u, dst)
	})
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tf})
	defer e.Close()

	r := newReq(canonical.TypeUpdate, "u", "s", 1, 10, 10)
	r.Units = append(r.Units,
		canonical.UnitRequest{RatingGroup: 200, Kind: canonical.UnitBytes, Requested: 10, Used: 10},
		canonical.UnitRequest{RatingGroup: 300, Kind: canonical.UnitBytes, Requested: 10, Used: 10})
	rate(t, e, r)

	if len(seen) != 1 || len(seen[0]) != 3 || seen[0][0] != 3 || seen[0][1] != 0 || seen[0][2] != 3 {
		t.Fatalf("rates per item: %v, want [[3 0 3]]", seen)
	}
	if got := e.Stats().Unpriced; got != 1 {
		t.Fatalf("unpriced %d", got)
	}
}

func TestOtherTariffErrorsStillFailTheRequest(t *testing.T) {
	boom := errors.New("boom")
	e, _ := New(Config{Workers: 1}, Deps{Store: &stubStore{}, Tariff: tariffFunc(func(*canonical.Request, *canonical.UnitRequest, []datalayer.Rate) ([]datalayer.Rate, error) {
		return nil, boom
	})})
	defer e.Close()
	resp := canonical.AcquireResponse()
	if err := e.Rate(context.Background(), newReq(canonical.TypeUpdate, "u", "s", 1, 10, 0), resp); !errors.Is(err, boom) {
		t.Fatalf("%v", err)
	}
}
