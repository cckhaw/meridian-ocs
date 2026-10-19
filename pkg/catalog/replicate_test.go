package catalog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublishReachesEveryNodeWithoutRestart(t *testing.T) {
	rdb := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &Replicator{Redis: rdb, Store: NewStore(0)}
	b := &Replicator{Redis: rdb, Store: NewStore(0), Resync: time.Hour} // must rely on pub/sub
	var wg sync.WaitGroup
	for _, r := range []*Replicator{a, b} {
		wg.Add(1)
		go func(r *Replicator) { defer wg.Done(); _ = r.Run(ctx) }(r)
	}
	defer wg.Wait()
	defer cancel()

	sn, _, err := a.Publish(ctx, doc(t0), 1)
	if err != nil || sn.Version != 1 {
		t.Fatal(err)
	}
	waitFor(t, "node b to receive v1", func() bool { return b.Store.LatestVersion() == 1 })

	d2 := doc(t0.Add(time.Hour))
	d2.Plans[0].Rules[1].Cash = &Price{7, 1}
	if _, _, err := b.Publish(ctx, d2, 2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "node a to receive v2", func() bool { return a.Store.LatestVersion() == 2 })

	// Both nodes price identically from the replicated document.
	for _, r := range []*Replicator{a, b} {
		rates, _, err := r.Store.At(t0.Add(2*time.Hour)).Rates("std", t0.Add(2*time.Hour),
			&canonical.Request{RATType: "LTE"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil || rates[2].Num != 7 {
			t.Fatalf("%v %v", rates, err)
		}
	}
}

func TestLateJoinerAndMissedMessageCatchUp(t *testing.T) {
	rdb := redistest.Start(t)
	ctx := context.Background()
	pub := &Replicator{Redis: rdb, Store: NewStore(0)}
	for i := 0; i < 5; i++ {
		if _, _, err := pub.Publish(ctx, doc(t0.Add(time.Duration(i)*time.Hour)), 0); err != nil {
			t.Fatal(err)
		}
	}
	// A node that starts later loads the history.
	late := &Replicator{Redis: rdb, Store: NewStore(0)}
	if err := late.Sync(ctx); err != nil || late.Store.LatestVersion() != 5 || len(late.Store.Versions()) != 5 {
		t.Fatalf("%v v%d", err, late.Store.LatestVersion())
	}
	// With a small retention only the newest versions are loaded.
	small := &Replicator{Redis: rdb, Store: NewStore(2)}
	if err := small.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	vs := small.Store.Versions()
	if len(vs) != 2 || vs[0].Version != 4 || vs[1].Version != 5 {
		t.Fatalf("%d versions, first %d", len(vs), vs[0].Version)
	}
	if small.Store.At(t0.Add(30*time.Minute)) != nil {
		t.Fatal("history older than the retained window must not exist")
	}
	// It can still follow new versions.
	if _, _, err := pub.Publish(ctx, doc(t0.Add(10*time.Hour)), 0); err != nil {
		t.Fatal(err)
	}
	if err := small.Sync(ctx); err != nil || small.Store.LatestVersion() != 6 {
		t.Fatalf("%v v%d", err, small.Store.LatestVersion())
	}

	// A node that never hears the pub/sub message still catches up on resync.
	rs := &Replicator{Redis: rdb, Store: NewStore(0), Resync: 20 * time.Millisecond, Channel: "ocs:catalog:deaf"}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = rs.Run(rctx); close(done) }()
	waitFor(t, "initial sync", func() bool { return rs.Store.LatestVersion() == 6 })
	if _, _, err := pub.Publish(ctx, doc(t0.Add(20*time.Hour)), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "periodic resync", func() bool { return rs.Store.LatestVersion() == 7 })
	cancel()
	<-done
}

func TestTwoEditorsCannotPublishTheSameVersion(t *testing.T) {
	rdb := redistest.Start(t)
	ctx := context.Background()
	var ok, conflict atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := &Replicator{Redis: rdb, Store: NewStore(0)} // each editor is its own node
			_, _, err := r.Publish(ctx, doc(t0.Add(time.Duration(g+1)*time.Hour)), 0)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrVersionConflict), errors.Is(err, ErrNotMonotonic):
				conflict.Add(1)
			default:
				t.Error(err)
			}
		}(g)
	}
	wg.Wait()
	n, _ := rdb.LLen(ctx, "ocs:catalog:versions").Result()
	if ok.Load() != n || ok.Load() < 1 || ok.Load()+conflict.Load() != 8 {
		t.Fatalf("ok=%d conflict=%d stored=%d", ok.Load(), conflict.Load(), n)
	}
	// Whatever order won, the stored timeline is strictly increasing in time.
	check := &Replicator{Redis: rdb, Store: NewStore(0)}
	if err := check.Sync(ctx); err != nil || check.Store.LatestVersion() != n {
		t.Fatalf("%v", err)
	}
}

func TestStaleEditorAndInvalidDocumentLeaveRedisUntouched(t *testing.T) {
	rdb := redistest.Start(t)
	ctx := context.Background()
	r := &Replicator{Redis: rdb, Store: NewStore(0)}
	if _, _, err := r.Publish(ctx, doc(t0), 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Publish(ctx, doc(t0.Add(time.Hour)), 1); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("%v", err)
	}
	bad := doc(t0.Add(time.Hour))
	bad.Plans[0].Rules[0].When = `rat ==`
	var ve *ValidationError
	if _, _, err := r.Publish(ctx, bad, 0); !errors.As(err, &ve) {
		t.Fatalf("%v", err)
	}
	if _, _, err := r.Publish(ctx, doc(t0), 0); !errors.Is(err, ErrNotMonotonic) {
		t.Fatalf("%v", err)
	}
	if n, _ := rdb.LLen(ctx, "ocs:catalog:versions").Result(); n != 1 {
		t.Fatalf("%d versions stored", n)
	}
}

func TestACorruptStoredVersionDoesNotReplaceAGoodCatalog(t *testing.T) {
	rdb := redistest.Start(t)
	ctx := context.Background()
	r := &Replicator{Redis: rdb, Store: NewStore(0)}
	if _, _, err := r.Publish(ctx, doc(t0), 0); err != nil {
		t.Fatal(err)
	}
	rdb.RPush(ctx, "ocs:catalog:versions", `{"effectiveFrom":"2031-01-01T00:00:00Z","plans":[]}`)
	if err := r.Sync(ctx); err == nil {
		t.Fatal("a version that does not compile must be reported")
	}
	if r.Store.LatestVersion() != 1 || r.Store.At(t0) == nil {
		t.Fatal("the node must keep serving the last good catalog")
	}
}

// The catalog drives the real engine: a version dated in the future starts
// pricing at its effective time, with no restart, and an unpriceable unit
// does not stop other units or a terminate from settling.
func TestCatalogDrivesTheEngine(t *testing.T) {
	rdb := redistest.Start(t)
	store := datalayer.New(rdb)
	ctx := context.Background()
	if err := store.PutBucket(ctx, "u1", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: 100_000, Order: 9}); err != nil {
		t.Fatal(err)
	}

	cat := NewStore(0)
	now := t0.Add(time.Hour)
	d1 := doc(t0)
	d2 := doc(t0.Add(24 * time.Hour))
	d2.Plans[0].Rules[1].Cash = &Price{10, 1} // data: 2 -> 10 per byte from day 2
	for _, d := range []Document{d1, d2} {
		if _, _, err := cat.Publish(d, 0); err != nil {
			t.Fatal(err)
		}
	}
	clock := func() time.Time { return now }
	tf := &Tariff{Store: cat, DefaultPlan: "std", Now: clock}
	e, err := engine.New(engine.Config{Workers: 1, Now: clock}, engine.Deps{Store: store, Tariff: tf})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	req := func(typ canonical.RequestType, n uint32, rat string, requested, used int64) *canonical.Request {
		r := canonical.AcquireRequest()
		r.Type, r.SubscriberID, r.SessionID, r.RequestNumber, r.RATType = typ, "u1", "s1", n, rat
		r.Units = append(r.Units, canonical.UnitRequest{RatingGroup: 100, Kind: canonical.UnitBytes, Requested: requested, Used: used})
		return r
	}
	rate := func(r *canonical.Request) *canonical.Response {
		resp := canonical.AcquireResponse()
		if err := e.Rate(ctx, r, resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Day 1, LTE: 2 per byte. 100 bytes reserved = 200.
	if r := rate(req(canonical.TypeInitial, 0, "LTE", 100, 0)); r.Grants[0].Granted != 100 {
		t.Fatalf("%+v", r)
	}
	if got, _ := store.Balance(ctx, "u1", "cash"); got != 100_000-200 {
		t.Fatalf("balance %d", got)
	}

	// Roaming is barred by a policy filter: no new quota, but the engine does
	// not fail and the already reserved 100 bytes are settled (50 used, 50 refunded at 2).
	r := rate(req(canonical.TypeUpdate, 1, "ROAM", 100, 50))
	if r.Grants[0].Granted != 0 || r.Grants[0].Result == canonical.ResultGranted {
		t.Fatalf("barred usage was granted: %+v", r)
	}
	if got, _ := store.Balance(ctx, "u1", "cash"); got != 100_000-100 {
		t.Fatalf("balance %d, want the unused half refunded", got)
	}
	if e.Stats().Unpriced != 1 {
		t.Fatalf("%+v", e.Stats())
	}
	// Terminating while roaming still settles.
	if r := rate(req(canonical.TypeTerminate, 2, "ROAM", 0, 0)); r.Result != canonical.ResultGranted {
		t.Fatalf("%+v", r)
	}

	// Day 2: the future-dated version takes over by itself: 10 per byte.
	now = t0.Add(25 * time.Hour)
	before, _ := store.Balance(ctx, "u1", "cash")
	ev := req(canonical.TypeEvent, 0, "LTE", 10, 0)
	ev.SessionID = "event-1" // s1 was terminated
	if r := rate(ev); len(r.Grants) != 1 || r.Grants[0].Granted != 10 {
		t.Fatalf("%+v", r)
	}
	if got, _ := store.Balance(ctx, "u1", "cash"); before-got != 100 {
		t.Fatalf("event cost %d, want 100", before-got)
	}
}
