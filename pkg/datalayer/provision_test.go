package datalayer_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/lifecycle"
)

func prov(t testing.TB, c *datalayer.Client, op datalayer.ProvisionOp) datalayer.ProvisionResult {
	t.Helper()
	if op.Now.IsZero() {
		op.Now = t0
	}
	if op.Fingerprint == "" {
		op.Fingerprint = fmt.Sprintf("%c|%s|%s|%d|%s|%s", op.Op, op.Key, op.BucketID, op.Amount, op.Target, op.Payment)
	}
	res, err := c.Provision(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func create(t testing.TB, c *datalayer.Client, key, payment string) {
	t.Helper()
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: key, Token: "create-" + key, Payment: payment}); r.Code != datalayer.ProvApplied {
		t.Fatalf("create: %+v", r)
	}
}

func TestCreateTopUpAndRead(t *testing.T) {
	c, ctx := setup(t)
	create(t, c, "p1", "prepaid")

	p, err := c.GetProfile(ctx, "p1")
	if err != nil || p.Payment != "prepaid" || p.Status != "active" || p.Version != 1 || !p.CreatedAt.Equal(t0) {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := c.GetProfile(ctx, "nobody"); err != datalayer.ErrNoSubscriber {
		t.Fatalf("%v", err)
	}

	r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p1", Token: "tu1", BucketID: "cash", Amount: 5000, Order: 100})
	if r.Code != datalayer.ProvApplied || r.Balance != 5000 || r.Version != 2 {
		t.Fatalf("%+v", r)
	}
	r = prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p1", Token: "tu2", BucketID: "cash", Amount: 250})
	if r.Balance != 5250 || r.Version != 3 {
		t.Fatalf("%+v", r)
	}
	bs, err := c.ListBuckets(ctx, "p1")
	if err != nil || len(bs) != 1 || bs[0].ID != "cash" || bs[0].Class != datalayer.ClassCash || bs[0].Kind != "any" || bs[0].Balance != 5250 {
		t.Fatalf("%+v %v", bs, err)
	}

	// The balance created by provisioning is the one the charging path uses.
	resp := charge(t, c, req("p1", "s", datalayer.ModeInitial, 0, t0, 1000, 0, rates(cash2)))
	if resp.Items[0].Granted != 1000 || bal(t, c, "p1", "cash") != 5250-2000 {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func TestSameTokenIsAppliedOnce(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p2", "prepaid")
	op := datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p2", Token: "pay-123", BucketID: "cash", Amount: 1000}

	first := prov(t, c, op)
	for i := 0; i < 3; i++ {
		again := prov(t, c, op)
		if again.Code != datalayer.ProvDuplicate || again.OriginalCode != datalayer.ProvApplied || again.Balance != first.Balance || again.Version != first.Version {
			t.Fatalf("redelivery %d: %+v vs %+v", i, again, first)
		}
	}
	if got := bal(t, c, "p2", "cash"); got != 1000 {
		t.Fatalf("a redelivered top-up credited %d", got)
	}

	// The same token for a different request is refused, not silently ignored.
	other := op
	other.Amount = 9999
	other.Fingerprint = "different"
	if r := prov(t, c, other); r.Code != datalayer.ProvTokenMismatch {
		t.Fatalf("%+v", r)
	}
	if got := bal(t, c, "p2", "cash"); got != 1000 {
		t.Fatal(got)
	}
}

func TestConcurrentRedeliveriesApplyExactlyOnce(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p3", "prepaid")
	var applied, dups atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Provision(context.Background(), datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p3", Token: "one-payment",
				Fingerprint: "fp", Now: t0, BucketID: "cash", Amount: 700})
			if err != nil {
				t.Error(err)
				return
			}
			switch r.Code {
			case datalayer.ProvApplied:
				applied.Add(1)
			case datalayer.ProvDuplicate:
				dups.Add(1)
			default:
				t.Errorf("%+v", r)
			}
		}()
	}
	wg.Wait()
	if applied.Load() != 1 || dups.Load() != 31 || bal(t, c, "p3", "cash") != 700 {
		t.Fatalf("applied=%d dups=%d balance=%d", applied.Load(), dups.Load(), bal(t, c, "p3", "cash"))
	}
}

// A failure is remembered too, so a redelivered command cannot flip to
// success because something changed in between.
func TestFailuresAreRememberedUnderTheToken(t *testing.T) {
	c, _ := setup(t)
	op := datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "late", Token: "t-late", BucketID: "cash", Amount: 100}
	if r := prov(t, c, op); r.Code != datalayer.ProvNoSubscriber {
		t.Fatalf("%+v", r)
	}
	create(t, c, "late", "prepaid") // the subscriber appears afterwards
	r := prov(t, c, op)
	if r.Code != datalayer.ProvDuplicate || r.OriginalCode != datalayer.ProvNoSubscriber || r.Effective() != datalayer.ProvNoSubscriber {
		t.Fatalf("a redelivered failure must stay a failure: %+v", r)
	}
	if _, err := c.ListBuckets(context.Background(), "late"); err != nil {
		t.Fatal(err)
	}
	if bs, _ := c.ListBuckets(context.Background(), "late"); len(bs) != 0 {
		t.Fatalf("nothing may have been applied: %+v", bs)
	}
}

func TestCreateConflictsAndNoSubscriber(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p4", "postpaid")
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: "p4", Token: "again", Payment: "prepaid"}); r.Code != datalayer.ProvExists {
		t.Fatalf("%+v", r)
	}
	if p, _ := c.GetProfile(context.Background(), "p4"); p.Payment != "postpaid" {
		t.Fatalf("an existing subscriber must not be overwritten: %+v", p)
	}
	for _, op := range []datalayer.ProvisionOp{
		{Op: datalayer.ProvTopUp, BucketID: "cash", Amount: 1},
		{Op: datalayer.ProvAdjust, BucketID: "cash", Amount: 1},
		{Op: datalayer.ProvLifecycle, Target: "grace"},
		{Op: datalayer.ProvAddBundle, BucketID: "b", Class: datalayer.ClassBundle, Amount: 1},
		{Op: datalayer.ProvRemove, BucketID: "b"},
	} {
		op.Key, op.Token = "ghost", fmt.Sprintf("g-%c", op.Op)
		if r := prov(t, c, op); r.Code != datalayer.ProvNoSubscriber {
			t.Errorf("%c: %+v", op.Op, r)
		}
	}
}

func TestAdjustAndTheFloor(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p5", "prepaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p5", Token: "a", BucketID: "cash", Amount: 1000})

	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "p5", Token: "d1", BucketID: "cash", Amount: -400}); r.Code != 0 || r.Balance != 600 {
		t.Fatalf("%+v", r)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "p5", Token: "d2", BucketID: "cash", Amount: -601}); r.Code != datalayer.ProvInsufficient || r.Balance != 600 {
		t.Fatalf("a debit below zero needs allow-negative: %+v", r)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "p5", Token: "d3", BucketID: "cash", Amount: -601, AllowNegative: true}); r.Code != 0 || r.Balance != -1 {
		t.Fatalf("%+v", r)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "p5", Token: "d4", BucketID: "missing", Amount: -1}); r.Code != datalayer.ProvNoBucket {
		t.Fatalf("%+v", r)
	}
	if got := bal(t, c, "p5", "cash"); got != -1 {
		t.Fatal(got)
	}
}

func TestBalanceOverflowIsRefused(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p6", "prepaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p6", Token: "a", BucketID: "cash", Amount: datalayer.MaxBalance - 10})
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p6", Token: "b", BucketID: "cash", Amount: 11}); r.Code != datalayer.ProvOverflow {
		t.Fatalf("%+v", r)
	}
}

func TestBundlesReloadAndRemove(t *testing.T) {
	c, ctx := setup(t)
	create(t, c, "p7", "postpaid")
	exp := t0.Add(30 * 24 * time.Hour)

	add := datalayer.ProvisionOp{Op: datalayer.ProvAddBundle, Key: "p7", Token: "b1", BucketID: "data", Class: datalayer.ClassBundle,
		Unit: canonical.UnitBytes, Amount: 1_000_000, ExpiresAt: exp, Order: 20}
	if r := prov(t, c, add); r.Code != 0 || r.Balance != 1_000_000 {
		t.Fatalf("%+v", r)
	}
	add.Token = "b2"
	if r := prov(t, c, add); r.Code != datalayer.ProvExists {
		t.Fatalf("an existing bucket must not be replaced by an add: %+v", r)
	}
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAddBundle, Key: "p7", Token: "b3", BucketID: "promo", Class: datalayer.ClassPromo,
		Unit: canonical.UnitBytes, Amount: 500, Order: 10})
	bs, _ := c.ListBuckets(ctx, "p7")
	if len(bs) != 2 || bs[0].ID != "promo" || bs[1].ID != "data" || !bs[1].ExpiresAt.Equal(exp) || bs[1].Kind != "bytes" {
		t.Fatalf("consumption order or contents wrong: %+v", bs)
	}

	// Charging consumes promo, then the bundle.
	resp := charge(t, c, req("p7", "s", datalayer.ModeInitial, 0, t0, 800, 0, rates(cash2)))
	if resp.Items[0].Granted != 800 || bal(t, c, "p7", "promo") != 0 || bal(t, c, "p7", "data") != 1_000_000-300 {
		t.Fatalf("%+v", resp.Items[0])
	}

	// Reload resets the allowance and keeps expiry / order when asked to.
	r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvReload, Key: "p7", Token: "r1", BucketID: "data", Amount: 2_000_000, KeepExpiry: true, KeepOrder: true})
	if r.Code != 0 || r.Balance != 2_000_000 || r.OldBalance != 1_000_000-300 {
		t.Fatalf("%+v", r)
	}
	bs, _ = c.ListBuckets(ctx, "p7")
	if !bs[1].ExpiresAt.Equal(exp) || bs[1].Order != 20 {
		t.Fatalf("%+v", bs[1])
	}
	newExp := t0.Add(60 * 24 * time.Hour)
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvReload, Key: "p7", Token: "r2", BucketID: "data", Amount: 5, ExpiresAt: newExp, Order: 5})
	bs, _ = c.ListBuckets(ctx, "p7")
	if !bs[0].ExpiresAt.Equal(newExp) || bs[0].ID != "data" {
		t.Fatalf("reload with new expiry and order: %+v", bs)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvReload, Key: "p7", Token: "r3", BucketID: "nope", Amount: 1, KeepExpiry: true, KeepOrder: true}); r.Code != datalayer.ProvNoBucket {
		t.Fatalf("%+v", r)
	}

	// Removing a bucket that still holds units needs force.
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvRemove, Key: "p7", Token: "x1", BucketID: "data"}); r.Code != datalayer.ProvBucketNotEmpty || r.Balance != 5 {
		t.Fatalf("%+v", r)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvRemove, Key: "p7", Token: "x2", BucketID: "data", Force: true}); r.Code != 0 || r.Balance != 5 {
		t.Fatalf("%+v", r)
	}
	if bs, _ = c.ListBuckets(ctx, "p7"); len(bs) != 1 {
		t.Fatalf("%+v", bs)
	}
}

func TestTopUpOnlyTargetsCashBuckets(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p8", "prepaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAddBundle, Key: "p8", Token: "b", BucketID: "data", Class: datalayer.ClassBundle, Unit: canonical.UnitBytes, Amount: 10})
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "p8", Token: "t", BucketID: "data", Amount: 100}); r.Code != datalayer.ProvNotCashBucket {
		t.Fatalf("money must not be added to a megabyte allowance: %+v", r)
	}
	// Care may still correct an allowance by hand.
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "p8", Token: "a", BucketID: "data", Amount: 100}); r.Code != 0 || r.Balance != 110 {
		t.Fatalf("%+v", r)
	}
}

// The Lua copy of the lifecycle rules must agree with pkg/lifecycle for every
// pair of states.
func TestLifecycleScriptMatchesTheGoTable(t *testing.T) {
	c, _ := setup(t)
	n := 0
	for from := lifecycle.Active; from <= lifecycle.Terminated; from++ {
		for to := lifecycle.Active; to <= lifecycle.Terminated; to++ {
			key := fmt.Sprintf("lc%d%d", from, to)
			prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: key, Token: "c", Payment: "prepaid"})
			// Walk to the starting state through allowed steps.
			for _, step := range path(from) {
				n++
				if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: key, Token: fmt.Sprintf("w%d", n), Target: step.String()}); r.Code != 0 {
					t.Fatalf("walking to %v via %v: %+v", from, step, r)
				}
			}
			r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: key, Token: "go", Target: to.String()})
			want := lifecycle.CanTransition(from, to)
			if (r.Code == datalayer.ProvApplied) != want {
				t.Errorf("%v -> %v: script says %v, Go table says %v", from, to, r.Code, want)
			}
			if want && (r.From != from.String() || r.To != to.String()) {
				t.Errorf("%+v", r)
			}
			if !want && (r.Code != datalayer.ProvInvalidTransition || r.From != from.String() || r.To != to.String()) {
				t.Errorf("%v -> %v: %+v", from, to, r)
			}
		}
	}
}

func path(to lifecycle.State) []lifecycle.State {
	switch to {
	case lifecycle.Grace:
		return []lifecycle.State{lifecycle.Grace}
	case lifecycle.Suspended:
		return []lifecycle.State{lifecycle.Suspended}
	case lifecycle.Terminated:
		return []lifecycle.State{lifecycle.Terminated}
	}
	return nil
}

func TestLifecycleGraceDeadlineAndVersions(t *testing.T) {
	c, ctx := setup(t)
	create(t, c, "p9", "prepaid")
	until := t0.Add(72 * time.Hour)
	r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "p9", Token: "g", Target: "grace", Until: until})
	if r.Code != 0 || r.From != "active" || r.To != "grace" || r.Version != 2 {
		t.Fatalf("%+v", r)
	}
	p, _ := c.GetProfile(ctx, "p9")
	if p.Status != "grace" || !p.GraceUntil.Equal(until) {
		t.Fatalf("%+v", p)
	}
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "p9", Token: "a", Target: "active"})
	if p, _ = c.GetProfile(ctx, "p9"); p.GraceUntil != (time.Time{}) || p.Version != 3 {
		t.Fatalf("leaving grace clears the deadline: %+v", p)
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "p9", Token: "same", Target: "active"}); r.Code != datalayer.ProvInvalidTransition {
		t.Fatalf("staying in a state is not a transition: %+v", r)
	}
}

func TestTerminatedSubscribersCannotBeCredited(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "p10", "prepaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "p10", Token: "t", Target: "terminated"})
	for i, op := range []datalayer.ProvisionOp{
		{Op: datalayer.ProvTopUp, BucketID: "cash", Amount: 1},
		{Op: datalayer.ProvAdjust, BucketID: "cash", Amount: 1},
		{Op: datalayer.ProvAddBundle, BucketID: "b", Class: datalayer.ClassBundle, Amount: 1},
	} {
		op.Key, op.Token = "p10", fmt.Sprintf("x%d", i)
		if r := prov(t, c, op); r.Code != datalayer.ProvTerminated {
			t.Errorf("%c: %+v", op.Op, r)
		}
	}
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "p10", Token: "back", Target: "active"}); r.Code != datalayer.ProvInvalidTransition {
		t.Fatalf("%+v", r)
	}
}

func TestProvisionValidation(t *testing.T) {
	c, _ := setup(t)
	ctx := context.Background()
	good := datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "v", Token: "t", Fingerprint: "f", Now: t0, BucketID: "cash", Amount: 5}
	for name, mod := range map[string]func(*datalayer.ProvisionOp){
		"no token":          func(o *datalayer.ProvisionOp) { o.Token = "" },
		"comma in token":    func(o *datalayer.ProvisionOp) { o.Token = "a,b" },
		"no fingerprint":    func(o *datalayer.ProvisionOp) { o.Fingerprint = "" },
		"braces in key":     func(o *datalayer.ProvisionOp) { o.Key = "a{b}" },
		"zero amount":       func(o *datalayer.ProvisionOp) { o.Amount = 0 },
		"negative top-up":   func(o *datalayer.ProvisionOp) { o.Amount = -1 },
		"amount over limit": func(o *datalayer.ProvisionOp) { o.Amount = datalayer.MaxBalance + 1 },
		"no bucket":         func(o *datalayer.ProvisionOp) { o.BucketID = "" },
		"bad bucket id":     func(o *datalayer.ProvisionOp) { o.BucketID = "a|b" },
		"unknown op":        func(o *datalayer.ProvisionOp) { o.Op = 'Z' },
		"bad payment":       func(o *datalayer.ProvisionOp) { o.Op, o.Payment = datalayer.ProvCreate, "maybe" },
		"bad target":        func(o *datalayer.ProvisionOp) { o.Op, o.Target = datalayer.ProvLifecycle, "dormant" },
		"cash as a bundle":  func(o *datalayer.ProvisionOp) { o.Op, o.Class = datalayer.ProvAddBundle, datalayer.ClassCash },
	} {
		op := good
		mod(&op)
		if _, err := c.Provision(ctx, op); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestProvisioningWritesWaitForTheReplica(t *testing.T) {
	primary := redistest.Start(t)
	replica, _ := redistest.StartReplica(t, primary)
	c, _ := datalayer.New(primary).WithDurability(datalayer.Durability{Replicas: 1, Timeout: time.Second})
	create(t, c, "rep", "prepaid")
	r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "rep", Token: "t", BucketID: "cash", Amount: 4200})
	if r.Replication != datalayer.ReplicationAcked {
		t.Fatalf("%v", r.Replication)
	}
	if got := replicaBalance(t, replica, "rep", "cash"); got != "4200" {
		t.Fatalf("replica has %s", got)
	}
}

// ---- charging path honours the lifecycle ------------------------------------

func TestSuspendedAndTerminatedSubscribersGetNoNewQuota(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "s1", "prepaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "s1", Token: "t", BucketID: "cash", Amount: 100_000})
	r := rates(cash2)

	// An active subscriber with an open session.
	charge(t, c, req("s1", "sess", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	if got := bal(t, c, "s1", "cash"); got != 100_000-2000 {
		t.Fatal(got)
	}

	// Barred: the update still settles what was used, but reserves nothing new.
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "s1", Token: "susp", Target: "suspended"})
	resp := charge(t, c, req("s1", "sess", datalayer.ModeUpdate, 1, t0, 1000, 300, r))
	if resp.Items[0].Code != datalayer.ItemBarred || resp.Items[0].Granted != 0 {
		t.Fatalf("%+v", resp.Items[0])
	}
	if got := bal(t, c, "s1", "cash"); got != 100_000-600 { // 300 used units at 2 each; the rest of the slice refunded
		t.Fatalf("usage must still be settled: %d", got)
	}
	if n, _ := c.OpenReservations(context.Background(), "s1"); n != 0 {
		t.Fatalf("no reservation may be left for a barred subscriber: %d", n)
	}
	for _, mode := range []datalayer.Mode{datalayer.ModeInitial, datalayer.ModeEvent} {
		resp = charge(t, c, req("s1", "new-"+string(rune(mode)), mode, 0, t0, 10, 0, r))
		if resp.Items[0].Code != datalayer.ItemBarred || resp.Items[0].Granted != 0 {
			t.Fatalf("mode %c: %+v", mode, resp.Items[0])
		}
	}
	if resp = charge(t, c, req("s1", "sess", datalayer.ModeTerminate, 2, t0, 0, 0, r)); resp.Code != datalayer.CodeOK {
		t.Fatalf("terminating a session is always allowed: %+v", resp)
	}
	if got := bal(t, c, "s1", "cash"); got != 100_000-600 {
		t.Fatalf("a barred subscriber was charged: %d", got)
	}

	// Reinstated: service resumes.
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "s1", Token: "back", Target: "active"})
	if resp = charge(t, c, req("s1", "again", datalayer.ModeInitial, 0, t0, 100, 0, r)); resp.Items[0].Granted != 100 {
		t.Fatalf("%+v", resp.Items[0])
	}

	// Terminated: barred for good.
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "s1", Token: "end", Target: "terminated"})
	if resp = charge(t, c, req("s1", "after", datalayer.ModeInitial, 0, t0, 100, 0, r)); resp.Items[0].Code != datalayer.ItemBarred {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func TestGraceStillServesAndNoProfileMeansServed(t *testing.T) {
	c, _ := setup(t)
	create(t, c, "s2", "postpaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "s2", Token: "t", BucketID: "cash", Amount: 10_000})
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "s2", Token: "g", Target: "grace"})
	if resp := charge(t, c, req("s2", "x", datalayer.ModeInitial, 0, t0, 100, 0, rates(cash2))); resp.Items[0].Granted != 100 {
		t.Fatalf("grace period exists to keep the subscriber served: %+v", resp.Items[0])
	}
	// Buckets seeded without a profile (older data, other tests) are served.
	put(t, c, "legacy", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 500, Order: 1})
	if resp := charge(t, c, req("legacy", "x", datalayer.ModeInitial, 0, t0, 100, 0, rates(cash2))); resp.Items[0].Granted != 100 {
		t.Fatalf("%+v", resp.Items[0])
	}
}

func gb(id string, amount int64) datalayer.Allowance {
	return datalayer.Allowance{BucketID: id, Class: datalayer.ClassBundle, Unit: canonical.UnitBytes, Amount: amount, Order: 100}
}

func TestAttachOfferingIsAllOrNothing(t *testing.T) {
	c, ctx := setup(t)
	create(t, c, "o1", "prepaid")
	if off, err := c.Offering(ctx, "o1"); err != nil || off != "" {
		t.Fatalf("%q %v", off, err)
	}
	if _, err := c.Offering(ctx, "ghost"); err != datalayer.ErrNoSubscriber {
		t.Fatalf("%v", err)
	}

	r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o1", Token: "a1", Offering: "gold",
		Allowances: []datalayer.Allowance{gb("data", 1000), {BucketID: "sms", Class: datalayer.ClassPromo, Unit: canonical.UnitEvents, Amount: 50, Order: 10}}})
	if r.Code != datalayer.ProvApplied || r.Offering != "gold" || r.PrevOffering != "" || r.Buckets != 2 || r.Version != 2 {
		t.Fatalf("%+v", r)
	}
	if off, _ := c.Offering(ctx, "o1"); off != "gold" {
		t.Fatalf("%q", off)
	}
	if p, _ := c.GetProfile(ctx, "o1"); p.Offering != "gold" {
		t.Fatalf("%+v", p)
	}
	bs, _ := c.ListBuckets(ctx, "o1")
	if len(bs) != 2 || bs[0].ID != "sms" || bs[1].ID != "data" || bs[1].Balance != 1000 {
		t.Fatalf("%+v", bs)
	}

	// Attaching again without renew conflicts, and writes nothing: not even the
	// allowance that did not exist yet, nor the offering id.
	r = prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o1", Token: "a2", Offering: "platinum",
		Allowances: []datalayer.Allowance{gb("extra", 5), gb("data", 9999)}})
	if r.Code != datalayer.ProvAllowanceExists || r.Conflict != "data" {
		t.Fatalf("%+v", r)
	}
	if off, _ := c.Offering(ctx, "o1"); off != "gold" {
		t.Fatalf("a refused attach changed the offering to %q", off)
	}
	if bs, _ = c.ListBuckets(ctx, "o1"); len(bs) != 2 || bs[1].Balance != 1000 {
		t.Fatalf("a refused attach changed buckets: %+v", bs)
	}

	// Renew resets existing allowances to their full amount and switches offering.
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvAdjust, Key: "o1", Token: "use", BucketID: "data", Amount: -400})
	r = prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o1", Token: "a3", Offering: "gold", Renew: true,
		Allowances: []datalayer.Allowance{gb("data", 1000)}})
	if r.Code != datalayer.ProvApplied || r.PrevOffering != "gold" || r.Buckets != 1 {
		t.Fatalf("%+v", r)
	}
	if bal, _ := c.Balance(ctx, "o1", "data"); bal != 1000 {
		t.Fatalf("renewed balance %d", bal)
	}

	// A bucket id that exists with another class is a conflict even on renew.
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "o1", Token: "tu", BucketID: "cash", Amount: 10, Order: 1000})
	r = prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o1", Token: "a4", Offering: "x", Renew: true,
		Allowances: []datalayer.Allowance{gb("cash", 1)}})
	if r.Code != datalayer.ProvAllowanceExists {
		t.Fatalf("%+v", r)
	}

	// Redelivery of the first attach returns its stored outcome and does not apply again.
	r = prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o1", Token: "a1", Offering: "gold",
		Allowances: []datalayer.Allowance{gb("data", 1000), {BucketID: "sms", Class: datalayer.ClassPromo, Unit: canonical.UnitEvents, Amount: 50, Order: 10}}})
	if r.Code != datalayer.ProvDuplicate || r.OriginalCode != datalayer.ProvApplied || r.Offering != "gold" {
		t.Fatalf("%+v", r)
	}
}

func TestAttachOfferingGuards(t *testing.T) {
	c, ctx := setup(t)
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "none", Token: "t", Offering: "gold"}); r.Code != datalayer.ProvNoSubscriber {
		t.Fatalf("%+v", r)
	}
	create(t, c, "o2", "postpaid")
	prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "o2", Token: "kill", Target: "terminated"})
	if r := prov(t, c, datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "o2", Token: "t2", Offering: "gold"}); r.Code != datalayer.ProvTerminated {
		t.Fatalf("%+v", r)
	}
	for name, op := range map[string]datalayer.ProvisionOp{
		"no offering":  {Offering: ""},
		"comma in id":  {Offering: "a,b"},
		"bad class":    {Offering: "g", Allowances: []datalayer.Allowance{{BucketID: "x", Class: datalayer.ClassCash, Amount: 1}}},
		"zero amount":  {Offering: "g", Allowances: []datalayer.Allowance{gb("x", 0)}},
		"duplicate id": {Offering: "g", Allowances: []datalayer.Allowance{gb("x", 1), gb("x", 2)}},
		"too many": {Offering: "g", Allowances: func() (a []datalayer.Allowance) {
			for i := 0; i <= datalayer.MaxAllowances; i++ {
				a = append(a, gb(fmt.Sprintf("b%d", i), 1))
			}
			return
		}()},
	} {
		op.Op, op.Key, op.Token, op.Fingerprint, op.Now = datalayer.ProvOffering, "o2", "g-"+name, "fp", t0
		if _, err := c.Provision(ctx, op); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
