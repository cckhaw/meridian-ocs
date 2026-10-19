package datalayer_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
)

var t0 = time.UnixMilli(1_700_000_000_000)

func setup(t testing.TB) (*datalayer.Client, context.Context) {
	t.Helper()
	return datalayer.New(redistest.Start(t)), context.Background()
}

func rates(cash datalayer.Rate) []datalayer.Rate {
	return []datalayer.Rate{
		{Class: datalayer.ClassPromo, Num: 1, Den: 1},
		{Class: datalayer.ClassBundle, Num: 1, Den: 1},
		cash,
	}
}

var cash2 = datalayer.Rate{Class: datalayer.ClassCash, Num: 2, Den: 1}

func put(t testing.TB, c *datalayer.Client, sub string, b datalayer.Bucket) {
	t.Helper()
	if err := c.PutBucket(context.Background(), sub, b); err != nil {
		t.Fatal(err)
	}
}

func bal(t testing.TB, c *datalayer.Client, sub, id string) int64 {
	t.Helper()
	v, err := c.Balance(context.Background(), sub, id)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func req(sub, sid string, mode datalayer.Mode, n uint32, now time.Time, requested, used int64, r []datalayer.Rate) *datalayer.Request {
	return &datalayer.Request{
		SubscriberID: sub, SessionID: sid, Mode: mode, ReqNo: n, Now: now,
		Validity: 30 * time.Second,
		Items:    []datalayer.Item{{RatingGroup: 100, Kind: canonical.UnitBytes, Requested: requested, Used: used, Rates: r}},
	}
}

func charge(t testing.TB, c *datalayer.Client, r *datalayer.Request) *datalayer.Response {
	t.Helper()
	var resp datalayer.Response
	if err := c.Charge(context.Background(), r, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func standardBuckets(t testing.TB, c *datalayer.Client, sub string) {
	put(t, c, sub, datalayer.Bucket{ID: "promo", Class: datalayer.ClassPromo, Kind: canonical.UnitBytes, Balance: 100, Order: 1})
	put(t, c, sub, datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 2})
	put(t, c, sub, datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: 10_000, Order: 3})
}

func TestConsumptionOrderPromoBundleCash(t *testing.T) {
	c, _ := setup(t)
	standardBuckets(t, c, "s1")
	resp := charge(t, c, req("s1", "sess", datalayer.ModeInitial, 0, t0, 1500, 0, rates(cash2)))
	if resp.Code != datalayer.CodeOK || len(resp.Items) != 1 || resp.Items[0].Granted != 1500 || resp.Items[0].Code != datalayer.ItemGranted {
		t.Fatalf("%+v", resp)
	}
	if got := bal(t, c, "s1", "promo"); got != 0 {
		t.Errorf("promo=%d", got)
	}
	if got := bal(t, c, "s1", "bundle"); got != 0 {
		t.Errorf("bundle=%d", got)
	}
	if got := bal(t, c, "s1", "cash"); got != 10_000-2*400 {
		t.Errorf("cash=%d", got)
	}
}

func TestUpdateRefundsUnusedAndTerminateSettles(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s2", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	r := rates(cash2)

	charge(t, c, req("s2", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	if got := bal(t, c, "s2", "bundle"); got != 4000 {
		t.Fatalf("after initial %d", got)
	}
	resp := charge(t, c, req("s2", "x", datalayer.ModeUpdate, 1, t0, 1000, 400, r))
	if resp.Items[0].Granted != 1000 {
		t.Fatalf("%+v", resp)
	}
	if got := bal(t, c, "s2", "bundle"); got != 3600 { // 4000 + 600 refunded - 1000 reserved
		t.Fatalf("after update %d", got)
	}
	charge(t, c, req("s2", "x", datalayer.ModeTerminate, 2, t0, 0, 300, r))
	if got := bal(t, c, "s2", "bundle"); got != 5000-400-300 {
		t.Fatalf("after terminate %d", got)
	}
	if n, _ := c.OpenReservations(context.Background(), "s2"); n != 0 {
		t.Fatalf("open reservations %d", n)
	}
}

func TestPartialGrantSetsFinalUnit(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s3", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 300, Order: 1})
	resp := charge(t, c, req("s3", "x", datalayer.ModeInitial, 0, t0, 1000, 0, rates(cash2)))
	it := resp.Items[0]
	if it.Granted != 300 || !it.Final || it.Code != datalayer.ItemPartialFinal || it.ValidityS != 30 {
		t.Fatalf("%+v", it)
	}
	resp = charge(t, c, req("s3", "y", datalayer.ModeInitial, 0, t0, 10, 0, rates(cash2)))
	if resp.Items[0].Granted != 0 || resp.Items[0].Code != datalayer.ItemLimitReached {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func TestNoEligibleBucket(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s4", datalayer.Bucket{ID: "sec", Class: datalayer.ClassBundle, Kind: canonical.UnitSeconds, Balance: 100, Order: 1})
	resp := charge(t, c, req("s4", "x", datalayer.ModeInitial, 0, t0, 10, 0, rates(cash2)))
	if resp.Items[0].Code != datalayer.ItemNoBucket {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func TestRetransmitIsIdempotent(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s5", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	r := rates(cash2)

	first := charge(t, c, req("s5", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	dup := charge(t, c, req("s5", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	if dup.Code != datalayer.CodeReplayed || dup.Items[0] != first.Items[0] {
		t.Fatalf("first=%+v dup=%+v", first, dup)
	}
	if got := bal(t, c, "s5", "bundle"); got != 4000 {
		t.Fatalf("debited more than once: %d", got)
	}

	u1 := charge(t, c, req("s5", "x", datalayer.ModeUpdate, 1, t0, 1000, 200, r))
	u2 := charge(t, c, req("s5", "x", datalayer.ModeUpdate, 1, t0, 1000, 200, r))
	if u2.Code != datalayer.CodeReplayed || u2.Items[0] != u1.Items[0] {
		t.Fatalf("u1=%+v u2=%+v", u1, u2)
	}
	if got := bal(t, c, "s5", "bundle"); got != 5000-200-1000 {
		t.Fatalf("balance %d", got)
	}
}

func TestStaleAndUnknownSession(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s6", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	r := rates(cash2)

	if resp := charge(t, c, req("s6", "nope", datalayer.ModeUpdate, 1, t0, 10, 0, r)); resp.Code != datalayer.CodeUnknownSession {
		t.Fatalf("%+v", resp)
	}
	charge(t, c, req("s6", "x", datalayer.ModeInitial, 0, t0, 100, 0, r))
	charge(t, c, req("s6", "x", datalayer.ModeUpdate, 2, t0, 100, 10, r))
	before := bal(t, c, "s6", "bundle")
	if resp := charge(t, c, req("s6", "x", datalayer.ModeUpdate, 1, t0, 100, 10, r)); resp.Code != datalayer.CodeStale {
		t.Fatalf("%+v", resp)
	}
	if bal(t, c, "s6", "bundle") != before {
		t.Fatal("stale request changed the balance")
	}
}

func TestOveruseReportsShortfall(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s7", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 100, Order: 1})
	r := rates(cash2)
	charge(t, c, req("s7", "x", datalayer.ModeInitial, 0, t0, 100, 0, r))
	resp := charge(t, c, req("s7", "x", datalayer.ModeTerminate, 1, t0, 0, 250, r))
	if resp.Items[0].Shortfall != 150 {
		t.Fatalf("%+v", resp.Items[0])
	}
	if got := bal(t, c, "s7", "bundle"); got != 0 {
		t.Fatalf("bundle=%d", got)
	}
}

func TestExpiredReservationIsRefundedInFull(t *testing.T) {
	c, ctx := setup(t)
	put(t, c, "s8", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("s8", "x", datalayer.ModeInitial, 0, t0, 600, 0, r))
	if got := bal(t, c, "s8", "bundle"); got != 400 {
		t.Fatal(got)
	}

	if n, _ := c.SweepExpired(ctx, "s8", t0.Add(10*time.Second)); n != 0 {
		t.Fatalf("swept %d before expiry", n)
	}
	if n, err := c.SweepExpired(ctx, "s8", t0.Add(31*time.Second)); err != nil || n != 1 {
		t.Fatalf("swept %d err=%v", n, err)
	}
	if got := bal(t, c, "s8", "bundle"); got != 1000 {
		t.Fatalf("not refunded: %d", got)
	}
	// Sweeping again is a no-op.
	if n, _ := c.SweepExpired(ctx, "s8", t0.Add(40*time.Second)); n != 0 {
		t.Fatal("double sweep")
	}
}

func TestChargeSweepsExpiredReservationsOpportunistically(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s9", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("s9", "dead", datalayer.ModeInitial, 0, t0, 900, 0, r)) // network drops this session
	// A new session 1 minute later can use the full balance again.
	resp := charge(t, c, req("s9", "live", datalayer.ModeInitial, 0, t0.Add(time.Minute), 900, 0, r))
	if resp.Items[0].Granted != 900 {
		t.Fatalf("%+v", resp.Items[0])
	}
	// The late update of the dead session is charged as over-use, not lost.
	late := charge(t, c, req("s9", "dead", datalayer.ModeTerminate, 1, t0.Add(time.Minute), 0, 50, r))
	if late.Items[0].Shortfall != 0 {
		t.Fatalf("%+v", late.Items[0])
	}
	if got := bal(t, c, "s9", "bundle"); got != 1000-900-50 {
		t.Fatalf("bundle=%d", got)
	}
}

func TestEventChargingIsAllOrNothing(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s10", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 50, Order: 1})
	r := rates(cash2)
	if resp := charge(t, c, req("s10", "e1", datalayer.ModeEvent, 0, t0, 100, 0, r)); resp.Items[0].Granted != 0 || resp.Items[0].Code != datalayer.ItemLimitReached {
		t.Fatalf("%+v", resp.Items[0])
	}
	if got := bal(t, c, "s10", "bundle"); got != 50 {
		t.Fatalf("partial event debit leaked: %d", got)
	}
	if resp := charge(t, c, req("s10", "e2", datalayer.ModeEvent, 0, t0, 40, 0, r)); resp.Items[0].Granted != 40 {
		t.Fatalf("%+v", resp.Items[0])
	}
	if got := bal(t, c, "s10", "bundle"); got != 10 {
		t.Fatal(got)
	}
}

func TestFractionalRateRoundsUpAndRefundsExactly(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s11", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: 1000, Order: 1})
	r := rates(datalayer.Rate{Class: datalayer.ClassCash, Num: 3, Den: 2}) // 1.5 per unit
	charge(t, c, req("s11", "x", datalayer.ModeInitial, 0, t0, 5, 0, r))
	if got := bal(t, c, "s11", "cash"); got != 1000-8 { // ceil(7.5)
		t.Fatal(got)
	}
	charge(t, c, req("s11", "x", datalayer.ModeTerminate, 1, t0, 0, 3, r))
	if got := bal(t, c, "s11", "cash"); got != 1000-5 { // ceil(4.5)
		t.Fatal(got)
	}
}

func TestFractionalRateLimitsUnitsByBalance(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s12", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: 10, Order: 1})
	r := rates(datalayer.Rate{Class: datalayer.ClassCash, Num: 3, Den: 2})
	resp := charge(t, c, req("s12", "x", datalayer.ModeInitial, 0, t0, 100, 0, r))
	// 10 units of money at 1.5 per unit buys 6 units (cost 9), never 7 (cost 11).
	if resp.Items[0].Granted != 6 {
		t.Fatalf("%+v", resp.Items[0])
	}
	if got := bal(t, c, "s12", "cash"); got != 1 {
		t.Fatal(got)
	}
}

func TestExpiredBucketIsSkipped(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s13", datalayer.Bucket{ID: "old", Class: datalayer.ClassPromo, Kind: canonical.UnitBytes, Balance: 500, Order: 1, ExpiresAt: t0.Add(-time.Hour)})
	put(t, c, "s13", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 500, Order: 2})
	resp := charge(t, c, req("s13", "x", datalayer.ModeInitial, 0, t0, 300, 0, rates(cash2)))
	if resp.Items[0].Granted != 300 || bal(t, c, "s13", "old") != 500 || bal(t, c, "s13", "bundle") != 200 {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func TestMultipleRatingGroupsInOneRequest(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s14", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	r := req("s14", "x", datalayer.ModeInitial, 0, t0, 0, 0, nil)
	rt := rates(cash2)
	r.Items = []datalayer.Item{
		{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 400, Rates: rt},
		{RatingGroup: 2, Kind: canonical.UnitBytes, Requested: 400, Rates: rt},
		{RatingGroup: 3, Kind: canonical.UnitBytes, Requested: 400, Rates: rt},
	}
	resp := charge(t, c, r)
	g := []int64{resp.Items[0].Granted, resp.Items[1].Granted, resp.Items[2].Granted}
	if g[0] != 400 || g[1] != 400 || g[2] != 200 || !resp.Items[2].Final {
		t.Fatalf("%v %+v", g, resp.Items)
	}
}

func TestValidationRejectsOutOfRange(t *testing.T) {
	c, ctx := setup(t)
	var resp datalayer.Response
	bad := []*datalayer.Request{
		req("a{b}", "x", datalayer.ModeInitial, 0, t0, 1, 0, nil),
		req("a", "", datalayer.ModeInitial, 0, t0, 1, 0, nil),
		req("a", "x", datalayer.Mode('Z'), 0, t0, 1, 0, nil),
		req("a", "x", datalayer.ModeInitial, 0, t0, datalayer.MaxUnits+1, 0, nil),
		req("a", "x", datalayer.ModeInitial, 0, t0, 1, 0, []datalayer.Rate{{Class: datalayer.ClassCash, Num: 1, Den: datalayer.MaxDen + 1}}),
	}
	for i, r := range bad {
		if err := c.Charge(ctx, r, &resp); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

// Money is neither created nor destroyed: after every session is terminated the
// cash balance equals initial - price * (units used - shortfall), for any
// random interleaving of initial / update / terminate across sessions.
func TestConservationUnderRandomTraffic(t *testing.T) {
	c, _ := setup(t)
	const initial = 100_000
	const price = 3
	put(t, c, "p", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: initial, Order: 1})
	r := rates(datalayer.Rate{Class: datalayer.ClassCash, Num: price, Den: 1})

	rng := rand.New(rand.NewSource(42))
	type sess struct {
		id   string
		next uint32
		open bool
	}
	sessions := make([]*sess, 12)
	for i := range sessions {
		sessions[i] = &sess{id: fmt.Sprintf("s%d", i)}
	}
	var used, shortfall int64
	for step := 0; step < 2000; step++ {
		s := sessions[rng.Intn(len(sessions))]
		u := int64(rng.Intn(400))
		var resp *datalayer.Response
		switch {
		case !s.open && s.next == 0:
			resp = charge(t, c, req("p", s.id, datalayer.ModeInitial, 0, t0, int64(rng.Intn(500)), 0, r))
			s.next, s.open = 1, true
		case s.open && rng.Intn(4) == 0:
			resp = charge(t, c, req("p", s.id, datalayer.ModeTerminate, s.next, t0, 0, u, r))
			used += u
			shortfall += resp.Items[0].Shortfall
			s.open, s.next = false, 0
			s.id = fmt.Sprintf("%s-r%d", s.id, step)
			continue
		case s.open:
			resp = charge(t, c, req("p", s.id, datalayer.ModeUpdate, s.next, t0, int64(rng.Intn(500)), u, r))
			used += u
			shortfall += resp.Items[0].Shortfall
			s.next++
		}
	}
	for _, s := range sessions {
		if s.open {
			resp := charge(t, c, req("p", s.id, datalayer.ModeTerminate, s.next, t0, 0, 0, r))
			shortfall += resp.Items[0].Shortfall
		}
	}
	got := bal(t, c, "p", "cash")
	want := int64(initial) - price*(used-shortfall)
	if got != want || got < 0 {
		t.Fatalf("balance %d, want %d (used=%d shortfall=%d)", got, want, used, shortfall)
	}
	if n, _ := c.OpenReservations(context.Background(), "p"); n != 0 {
		t.Fatalf("%d reservations left open", n)
	}
}

// Concurrent sessions on one subscriber never overspend and never lose money.
func TestConcurrentSessionsNeverOverspend(t *testing.T) {
	c, _ := setup(t)
	const initial = 50_000
	put(t, c, "cc", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: initial, Order: 1})
	r := rates(datalayer.Rate{Class: datalayer.ClassCash, Num: 2, Den: 1})

	var mu sync.Mutex
	var used, shortfall int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			sid := fmt.Sprintf("g%d", g)
			var resp datalayer.Response
			do := func(q *datalayer.Request) {
				if err := c.Charge(context.Background(), q, &resp); err != nil {
					t.Error(err)
				}
			}
			do(req("cc", sid, datalayer.ModeInitial, 0, t0, 300, 0, r))
			var u, sf int64
			for n := uint32(1); n <= 40; n++ {
				do(req("cc", sid, datalayer.ModeUpdate, n, t0, 300, 100, r))
				u += 100
				sf += resp.Items[0].Shortfall
			}
			do(req("cc", sid, datalayer.ModeTerminate, 41, t0, 0, 50, r))
			u += 50
			sf += resp.Items[0].Shortfall
			mu.Lock()
			used += u
			shortfall += sf
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	got := bal(t, c, "cc", "cash")
	want := int64(initial) - 2*(used-shortfall)
	if got != want || got < 0 {
		t.Fatalf("balance %d want %d", got, want)
	}
}

func BenchmarkChargeUpdate(b *testing.B) {
	c, ctx := setup(b)
	put(b, c, "bench", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1 << 40, Order: 1})
	r := rates(cash2)
	var resp datalayer.Response
	q := req("bench", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r)
	if err := c.Charge(ctx, q, &resp); err != nil {
		b.Fatal(err)
	}
	q.Mode = datalayer.ModeUpdate
	q.Items[0].Used = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.ReqNo = uint32(i + 1)
		if err := c.Charge(ctx, q, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTerminateWithoutItemsReleasesAllReservations(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "s15", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	r := req("s15", "x", datalayer.ModeInitial, 0, t0, 0, 0, nil)
	rt := rates(cash2)
	r.Items = []datalayer.Item{
		{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 300, Rates: rt},
		{RatingGroup: 2, Kind: canonical.UnitBytes, Requested: 200, Rates: rt},
	}
	charge(t, c, r)
	if got := bal(t, c, "s15", "bundle"); got != 500 {
		t.Fatal(got)
	}
	term := req("s15", "x", datalayer.ModeTerminate, 1, t0, 0, 0, nil)
	term.Items = nil
	if resp := charge(t, c, term); resp.Code != datalayer.CodeOK {
		t.Fatalf("%+v", resp)
	}
	if got := bal(t, c, "s15", "bundle"); got != 1000 {
		t.Fatalf("reservations not released: %d", got)
	}
	if n, _ := c.OpenReservations(context.Background(), "s15"); n != 0 {
		t.Fatalf("open=%d", n)
	}
}
