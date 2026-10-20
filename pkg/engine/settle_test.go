package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

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
	rdb    *redis.Client
	t      *testing.T
	store  *outage
	spool  *unsettled.Spool
	eng    *Engine
	rec    *unsettled.Reconciler
	ctx    context.Context
	client *datalayer.Client
}

func newSettleRig(t *testing.T, balance int64) *settleRig {
	return newSettleRigWith(t, balance, Config{})
}

func newSettleRigWith(t *testing.T, balance int64, cfg Config) *settleRig {
	t.Helper()
	rdb := redistest.Start(t)
	client := datalayer.New(rdb)
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
	cfg.Workers = 2
	e, err := New(cfg, Deps{Store: st, Tariff: tariff, Breaker: br, Unsettled: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return &settleRig{rdb: rdb, t: t, store: st, spool: spool, eng: e, rec: &unsettled.Reconciler{Spool: spool, Settler: e}, ctx: ctx, client: client}
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
	for _, typ := range []canonical.RequestType{canonical.TypeInitial, canonical.TypeEvent} {
		if _, err := e.Settle(context.Background(), newReq(typ, "u", "s", 1, 10, 10)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("only a terminate or a refused update can be replayed (type %d): %v", typ, err)
		}
	}
}

// ---- refused updates ---------------------------------------------------------------------------

// recoverWait lets the circuit breaker's probe interval pass after the data tier comes back.
func (r *settleRig) recoverWait() { time.Sleep(30 * time.Millisecond) }

// openBreaker makes a failing call so the breaker opens: later calls are refused without reaching the data tier.
func (r *settleRig) openBreaker() {
	r.t.Helper()
	r.rate(newReq(canonical.TypeInitial, "r1", "prime", 0, 10, 0))
	if !r.eng.deps.Breaker.Open() {
		r.t.Fatal("the breaker should be open")
	}
}

func (r *settleRig) refusedUpdate(no uint32, used int64) {
	r.t.Helper()
	resp := r.rate(newReq(canonical.TypeUpdate, "r1", "s", no, 1000, used))
	if resp.Result == canonical.ResultGranted && resp.Grants[0].Granted > 0 {
		r.t.Fatalf("the update should have been refused: %+v", resp)
	}
}

func reqFor(typ canonical.RequestType, no uint32, used int64) *canonical.Request {
	return newReq(typ, "r1", "s", no, 1000, used)
}

func TestUsageOfARefusedUpdateIsChargedEvenIfTheSessionMovedOn(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	r.refusedUpdate(1, 700)
	st := r.eng.Stats()
	if st.UnsettledUpdates != 1 || st.UnsettledRecorded != 1 || st.UnsettledUnrecorded != 0 {
		t.Fatalf("the refused update's usage must be on record: %+v", st)
	}
	if recs := r.spool.Pending(0); len(recs) != 1 || !recs[0].IsUpdate() || recs[0].Units[0].Used != 700 {
		t.Fatalf("recorded as an update: %+v", recs)
	}

	// Redis returns. The network element, refused, ends the session without re-reporting what it measured.
	r.store.down.Store(false)
	r.recoverWait()
	r.rate(newReq(canonical.TypeTerminate, "r1", "s", 2, 0, 0))
	if got := r.balance(); got != 1_000_000 {
		t.Fatalf("setup: the unreported 700 has been refunded with the reservation: %d", got)
	}
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("the usage must be charged: balance %d, want %d", got, 1_000_000-700)
	}
	// The reconciler restarting and submitting it again charges nothing more.
	_ = r.spool.Record(reqFor(canonical.TypeUpdate, 1, 700))
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("a second submission charged again: %d", got)
	}
}

// RFC 4006 section 5.7 lets a client that got no usable answer re-send the same request (same CC-Request-Number,
// T-flag). If that re-sent request is processed after the outage, and the session carries on, the data tier has
// applied it; replaying the refused original must not charge it a second time.
func TestARetransmissionAfterTheOutageIsNotChargedTwice(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	r.refusedUpdate(1, 700)
	r.store.down.Store(false)
	r.recoverWait()
	r.rate(reqFor(canonical.TypeUpdate, 1, 700))    // the retransmission: applied, 700 charged, a new 1000 reserved
	r.rate(reqFor(canonical.TypeTerminate, 2, 300)) // the session carries on and ends having used 300 more
	want := 1_000_000 - 700 - 300
	if got := r.balance(); got != int64(want) {
		t.Fatalf("setup: %d, want %d", got, want)
	}
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := r.balance(); got != int64(want) {
		t.Fatalf("the usage of a request the data tier had applied was charged again: %d, want %d", got, want)
	}
}

// The data tier tells exactly whether a request was applied, so an update whose outcome was unknown is no longer
// ambiguous when the session moved on: it was not applied, so it is charged.
func TestAnUnknownOutcomeUpdateTheSessionMovedPastIsChargedWhenItWasNotApplied(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.refusedUpdate(1, 700) // a call was made and failed
	r.store.down.Store(false)
	r.recoverWait()
	r.rate(reqFor(canonical.TypeTerminate, 2, 0))
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if st := r.rec.Stats(); st.Ambiguous != 0 || st.Settled != 1 {
		t.Fatalf("%+v", st)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("balance %d, want %d", got, 1_000_000-700)
	}
}

func TestUsageOfAnUpdateWhoseOutcomeIsUnknownIsChargedAndItsRetryThenChargesNothing(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.refusedUpdate(1, 700)
	r.store.down.Store(false)
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("700 used of the 1000 reserved: balance %d, want %d", got, 1_000_000-700)
	}
	// The network element retries the very request that was refused: it is answered with the refusal it first got.
	resp := r.rate(reqFor(canonical.TypeUpdate, 1, 700))
	if resp.Grants[0].Granted != 0 {
		t.Fatalf("a retry of a request already settled late is answered with its refusal: %+v", resp)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("the retry charged again: %d", got)
	}
}

func TestAnUpdateThatDidReachRedisIsNotChargedTwice(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.rate(reqFor(canonical.TypeUpdate, 1, 700)) // applied for real; its answer is "lost"
	want := r.balance()                          // 700 charged, a new 1000 reserved
	if want != 1_000_000-700-1000 {
		t.Fatalf("setup: %d", want)
	}
	if err := r.spool.Record(reqFor(canonical.TypeUpdate, 1, 700)); err != nil {
		t.Fatal(err)
	}
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := r.balance(); got != want {
		t.Fatalf("an update the data tier had already applied was charged again: %d, want %d", got, want)
	}
}

// Only the last 8 request numbers are remembered; beyond that nobody can tell whether the request was applied.
func TestAnUpdateOlderThanTheRememberedWindowIsLeftToAPerson(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.refusedUpdate(1, 700)
	r.store.down.Store(false)
	r.recoverWait()
	for n := uint32(2); n <= 10; n++ { // the session carries on for nine more requests
		r.rate(reqFor(canonical.TypeUpdate, n, 0))
	}
	before := r.balance()
	if n, err := r.pass(); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	recs, why := r.spool.DeadLetters()
	if len(recs) != 1 || !recs[0].IsUpdate() || !containsFold(why[0], "twice") {
		t.Fatalf("it must be kept, with the reason: %+v %v", recs, why)
	}
	if st := r.rec.Stats(); st.Ambiguous != 1 || st.Dead != 0 || st.Settled != 0 {
		t.Fatalf("%+v", st)
	}
	if got := r.balance(); got != before {
		t.Fatalf("nothing may be charged when it could be charged twice: %d, was %d", got, before)
	}
}

func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestAnUpdateOfASessionThatIsGoneIsADeadLetter(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.store.down.Store(true)
	r.openBreaker()
	r.rate(newReq(canonical.TypeUpdate, "r1", "ghost", 3, 1000, 300))
	r.store.down.Store(false)
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if st := r.rec.Stats(); st.Dead != 1 || st.Ambiguous != 0 || r.spool.Stats().Pending != 0 {
		t.Fatalf("%+v", st)
	}
	if got := r.balance(); got != 1_000_000 {
		t.Fatalf("%d", got)
	}
}

func TestRefusedUpdatesWithNoUsageAreNotRecorded(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.refusedUpdate(1, 0)
	if st := r.eng.Stats(); st.UnsettledUpdates != 0 || r.spool.Stats().Pending != 0 {
		t.Fatalf("an update that reported nothing loses nothing: %+v", st)
	}
}

// With PeerMayRepeatUsage (peers that may report refused usage again under a new request number) a refused update
// is charged only while the session has not moved past it; past it, nothing is charged and a person decides.
func TestWhenPeersMayRepeatUsageAnUpdateTheSessionMovedPastIsLeftToAPerson(t *testing.T) {
	r := newSettleRigWith(t, 1_000_000, Config{PeerMayRepeatUsage: true})
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	r.refusedUpdate(1, 700)
	r.store.down.Store(false)
	r.recoverWait()
	r.rate(reqFor(canonical.TypeTerminate, 2, 0)) // the session carried on and ended
	if n, err := r.pass(); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	recs, why := r.spool.DeadLetters()
	if len(recs) != 1 || !containsFold(why[0], "twice") {
		t.Fatalf("kept for a person, with the reason: %+v %v", recs, why)
	}
	if st := r.rec.Stats(); st.Ambiguous != 1 || st.Settled != 0 {
		t.Fatalf("%+v", st)
	}
	if got := r.balance(); got != 1_000_000 {
		t.Fatalf("nothing may be charged when the peer may have repeated it: %d", got)
	}
}

func TestWhenPeersMayRepeatUsageAnUpdateOfASessionThatStoodStillIsStillCharged(t *testing.T) {
	r := newSettleRigWith(t, 1_000_000, Config{PeerMayRepeatUsage: true})
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	r.refusedUpdate(1, 700)
	r.store.down.Store(false)
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("balance %d, want %d", got, 1_000_000-700)
	}
}

// loseSessionState deletes the session hash of "s" (a failover that lost it, a flush, an expiry): the data tier no
// longer knows the session. The reservation records and the balances stay.
func (r *settleRig) loseSessionState() {
	r.t.Helper()
	if err := r.rdb.Del(r.ctx, "{sub:r1}:s:s").Err(); err != nil {
		r.t.Fatal(err)
	}
}

// For Nchf (TS 32.290 5.5.1.2) a refused update of a session whose state is gone is still charged afterwards.
func TestUsageOfARefusedNchfUpdateIsChargedEvenIfTheSessionWasLostMeanwhile(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	q := reqFor(canonical.TypeUpdate, 1, 700)
	q.AcceptUnknownSession = true // what the Nchf adapter sets
	r.rate(q)
	if recs := r.spool.Pending(0); len(recs) != 1 || !recs[0].AcceptUnknownSession {
		t.Fatalf("the record must remember it is an Nchf request: %+v", recs)
	}
	r.store.down.Store(false)
	r.loseSessionState()
	if n, err := r.pass(); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if st := r.rec.Stats(); st.Dead != 0 || st.Settled != 1 {
		t.Fatalf("%+v", st)
	}
	// The reservation of the lost session is refunded when it expires, as ever; the 700 are charged.
	if n, err := r.client.SweepExpired(r.ctx, "r1", time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	if got := r.balance(); got != 1_000_000-700 {
		t.Fatalf("balance %d, want %d", got, 1_000_000-700)
	}
}

// ... and for Gy an unknown session stays unknown (RFC 4006): the usage is a dead letter, not charged.
func TestUsageOfARefusedGyUpdateOfALostSessionStaysADeadLetter(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.store.down.Store(true)
	r.openBreaker()
	r.rate(reqFor(canonical.TypeUpdate, 1, 700)) // no AcceptUnknownSession
	r.store.down.Store(false)
	r.loseSessionState()
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if st := r.rec.Stats(); st.Dead != 1 || st.Settled != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestATerminateOfALostNchfSessionIsChargedAndOnlyOnce(t *testing.T) {
	r := newSettleRig(t, 1_000_000)
	r.rate(newReq(canonical.TypeInitial, "r1", "s", 0, 1000, 0))
	r.loseSessionState()
	q := reqFor(canonical.TypeTerminate, 1, 300)
	q.AcceptUnknownSession = true
	r.rate(q)
	// 300 charged fresh; the lost session's 1000 reservation comes back at expiry.
	if _, err := r.client.SweepExpired(r.ctx, "r1", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := r.balance(); got != 1_000_000-300 {
		t.Fatalf("balance %d, want %d", got, 1_000_000-300)
	}
	q2 := reqFor(canonical.TypeTerminate, 1, 300) // the retransmission
	q2.AcceptUnknownSession = true
	r.rate(q2)
	if got := r.balance(); got != 1_000_000-300 {
		t.Fatalf("a retransmitted terminate charged again: %d", got)
	}
}
