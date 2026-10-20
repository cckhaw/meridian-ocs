package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
	"github.com/cckhaw/meridian-ocs/pkg/unsettled"
)

// outage wraps the real data tier and fails every call while down is set.
type outage struct {
	*datalayer.Client
	down atomic.Bool
}

func (o *outage) Charge(ctx context.Context, req *datalayer.Request, resp *datalayer.Response) error {
	if o.down.Load() {
		return errors.New("redis unreachable")
	}
	return o.Client.Charge(ctx, req, resp)
}

type settleRig struct {
	t      *testing.T
	store  *outage
	spool  *unsettled.Spool
	eng    *Engine
	rec    *unsettled.Reconciler
	ctx    context.Context
	client *datalayer.Client
}

func newSettleRig(t *testing.T, balance int64) *settleRig {
	t.Helper()
	client := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	if err := client.PutBucket(ctx, "r1", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: balance, Order: 1}); err != nil {
		t.Fatal(err)
	}
	spool, err := unsettled.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	st := &outage{Client: client}
	br := failpolicy.NewBreaker(1, 10*time.Millisecond, nil, nil)
	e, err := New(Config{Workers: 2}, Deps{Store: st, Tariff: tariff, Breaker: br, Unsettled: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return &settleRig{t: t, store: st, spool: spool, eng: e, rec: &unsettled.Reconciler{Spool: spool, Settler: e}, ctx: ctx, client: client}
}

func (r *settleRig) balance() int64 {
	r.t.Helper()
	b, err := r.client.Balance(r.ctx, "r1", "b")
	if err != nil {
		r.t.Fatal(err)
	}
	return b
}

func (r *settleRig) rate(req *canonical.Request) *canonical.Response {
	r.t.Helper()
	resp := canonical.AcquireResponse()
	if err := r.eng.Rate(r.ctx, req, resp); err != nil {
		r.t.Fatal(err)
	}
	canonical.ReleaseRequest(req)
	return resp
}

// pass runs one reconciliation pass, waiting out the breaker's probe interval first.
func (r *settleRig) pass() (int, error) {
	time.Sleep(30 * time.Millisecond)
	return r.rec.Pass(r.ctx)
}

func TestUsageReleasedDuringAnOutageIsChargedWhenTheDataTierReturns(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	if got := r.balance(); got != 999_000 {
		t.Fatalf("the grant should be reserved: %d", got)
	}

	r.store.down.Store(true)
	resp := r.rate(newReq(canonical.TypeTerminate, "r1", "s", 1, 0, 700))
	if resp.Result != canonical.ResultGranted || !resp.Grants[0].Degraded {
		t.Fatalf("the terminate must still be acknowledged: %+v", resp)
	}
	st := r.eng.Stats()
	if st.UnsettledTerminates != 1 || st.UnsettledRecorded != 1 || st.UnsettledUnrecorded != 0 || r.spool.Stats().Pending != 1 {
		t.Fatalf("the usage must be on record: %+v pending %d", st, r.spool.Stats().Pending)
	}

	// Still down: nothing is charged, the record stays.
	if n, err := r.pass(); err == nil || n != 0 || r.spool.Stats().Pending != 1 {
		t.Fatalf("while the data tier is down the record must stay pending: n=%d err=%v", n, err)
	}

	r.store.down.Store(false)
	n, err := r.pass()
	if err != nil || n != 1 || r.spool.Stats().Pending != 0 {
		t.Fatalf("n=%d err=%v pending=%d", n, err, r.spool.Stats().Pending)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("700 used of a 1000 reservation: balance %d, want %d", got, 1_000_000-700)
	}
}

func TestReplayingATerminateThatAlreadyReachedTheDataTierChargesNothingTwice(t *testing.T) {
	// The unknown-outcome case: the terminate was applied, the answer was lost, the engine acknowledged it as
	// unsettled and recorded it. Replaying it must be answered from the stored result.
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.rate(newReq(canonical.TypeTerminate, "r1", "s", 1, 0, 700)) // applied for real
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("setup: %d", got)
	}
	dup := newReq(canonical.TypeTerminate, "r1", "s", 1, 0, 700)
	if err := r.spool.Record(dup); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ { // and a reconciler that restarts mid-way and replays again
		_ = r.spool.Record(dup)
		if _, err := r.pass(); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("a replay charged again: balance %d, want %d", got, 1_000_000-700)
	}
}

func TestUsageIsStillChargedWhenTheReservationExpiredMeanwhile(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.rate(newReq(canonical.TypeTerminate, "r1", "s", 1, 0, 700))
	// The outage outlasts the reservation: it expires and is refunded.
	if n, err := r.client.SweepExpired(r.ctx, "r1", time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	if got := r.balance(); got != 1_000_000 {
		t.Fatalf("setup: the reservation should be refunded, balance %d", got)
	}
	r.store.down.Store(false)
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("usage must be charged even though the reservation is gone: balance %d, want %d", got, 1_000_000-700)
	}
}

func TestUsageOfASessionThatNoLongerExistsIsADeadLetterNotSilentlyDropped(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	// The terminate arrived for a session the data tier never heard of (its create was lost, or its state expired).
	r.store.down.Store(true)
	r.rate(newReq(canonical.TypeTerminate, "r1", "ghost", 4, 0, 300))
	r.store.down.Store(false)
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	st := r.spool.Stats()
	recs, why := r.spool.DeadLetters()
	if st.Pending != 0 || st.Dead != 1 || len(recs) != 1 || recs[0].SessionID != "ghost" || why[0] == "" || r.rec.Stats().Dead != 1 {
		t.Fatalf("an unsettleable record must be kept as a dead letter: %+v %v", st, why)
	}
	if got := r.balance(); got != 1_000_000 {
		t.Fatalf("nothing was charged: %d", got)
	}
}

func TestUsageIsCountedAsLostWhenItCannotBeRecorded(t *testing.T) {
	// No sink configured.
	e, _ := New(Config{Workers: 1}, Deps{Store: &stubStore{err: errors.New("down")}, Tariff: tariff})
	defer e.Close()
	rate(t, e, newReq(canonical.TypeTerminate, "u", "s", 1, 0, 100))
	if st := e.Stats(); st.UnsettledTerminates != 1 || st.UnsettledRecorded != 0 || st.UnsettledUnrecorded != 1 {
		t.Fatalf("without a sink the usage is lost and must be counted as such: %+v", st)
	}
	// A sink whose write fails.
	e2, _ := New(Config{Workers: 1}, Deps{Store: &stubStore{err: errors.New("down")}, Tariff: tariff, Unsettled: failingSink{}})
	defer e2.Close()
	resp := rate(t, e2, newReq(canonical.TypeTerminate, "u", "s", 1, 0, 100))
	if resp.Result != canonical.ResultGranted {
		t.Fatalf("the terminate is still acknowledged: %+v", resp)
	}
	if st := e2.Stats(); st.UnsettledUnrecorded != 1 || st.UnsettledRecorded != 0 {
		t.Fatalf("%+v", st)
	}
}

type failingSink struct{}

func (failingSink) Record(*canonical.Request) error { return errors.New("disk full") }

func TestSettleNeverUsesTheFailPolicyAndRespectsTheBreaker(t *testing.T) {
	st := &stubStore{err: errors.New("down")}
	br := failpolicy.NewBreaker(1, time.Hour, nil, nil)
	e, _ := New(Config{Workers: 1}, Deps{Store: st, Tariff: tariff, Breaker: br})
	defer e.Close()

	req := newReq(canonical.TypeTerminate, "u", "s", 1, 0, 100)
	if _, err := e.Settle(context.Background(), req); err == nil {
		t.Fatal("a failing data tier must be returned as an error, not answered by the fail policy")
	}
	if e.Stats().Degraded != 0 || e.Stats().UnsettledTerminates != 0 {
		t.Fatalf("a replay must not count as a new degraded answer: %+v", e.Stats())
	}
	calls := st.calls.Load()
	if _, err := e.Settle(context.Background(), req); err == nil || st.calls.Load() != calls {
		t.Fatal("an open breaker must stop replays from hitting the data tier")
	}
	if _, err := e.Settle(context.Background(), newReq(canonical.TypeUpdate, "u", "s", 1, 10, 10)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("only terminates can be replayed: %v", err)
	}
}
