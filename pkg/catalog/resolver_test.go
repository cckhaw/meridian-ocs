package catalog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

type fakeSource struct {
	calls atomic.Int64
	offs  map[string]string
	err   error
}

func (f *fakeSource) Offering(_ context.Context, key string) (string, error) {
	f.calls.Add(1)
	if f.err != nil {
		return "", f.err
	}
	o, ok := f.offs[key]
	if !ok {
		return "", datalayer.ErrNoSubscriber
	}
	return o, nil
}

func twoPlanStore(t *testing.T) *Store {
	t.Helper()
	d := Document{EffectiveFrom: t0, Plans: []PricePlan{
		{ID: "basic", Rules: []ChargeRule{{ID: "r", Cash: &Price{Num: 4, Den: 1}}}},
		{ID: "gold", Rules: []ChargeRule{{ID: "r", Cash: &Price{Num: 1, Den: 1}}}},
	}, Offerings: []ProductOffering{{ID: "gold-offer", PlanID: "gold"}}}
	s := NewStore(0)
	if _, _, err := s.Publish(d, 0); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProfileResolverMapsOfferingToPlanAndCaches(t *testing.T) {
	src := &fakeSource{offs: map[string]string{"alice": "gold-offer", "bob": "", "carol": "retired-offer"}}
	r := NewProfileResolver(src, twoPlanStore(t))
	clock := t0.Add(time.Hour)
	r.now = func() time.Time { return clock }
	at := t0.Add(time.Hour)

	if p, err := r.PlanFor("alice", at); err != nil || p != "gold" {
		t.Fatalf("%q %v", p, err)
	}
	for i := 0; i < 5; i++ {
		r.PlanFor("alice", at)
	}
	if src.calls.Load() != 1 {
		t.Fatalf("%d profile reads for one subscriber within the TTL", src.calls.Load())
	}
	clock = clock.Add(31 * time.Second)
	r.PlanFor("alice", at)
	if src.calls.Load() != 2 {
		t.Fatalf("an expired entry was not re-read: %d", src.calls.Load())
	}

	if _, err := r.PlanFor("bob", at); !errors.Is(err, ErrNoPlan) {
		t.Fatalf("no offering: %v", err)
	}
	if _, err := r.PlanFor("nobody", at); !errors.Is(err, ErrNoPlan) {
		t.Fatalf("no profile: %v", err)
	}
	if _, err := r.PlanFor("carol", at); !errors.Is(err, ErrUnknownOffering) {
		t.Fatalf("offering gone from the catalog: %v", err)
	}
	if _, err := r.PlanFor("alice", t0.Add(-time.Hour)); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("before the first catalog version: %v", err)
	}
}

func TestLookupFailureIsNeverTheDefaultPlan(t *testing.T) {
	src := &fakeSource{err: errors.New("redis down")}
	r := NewProfileResolver(src, twoPlanStore(t))
	tf := &Tariff{Store: r.Store, Resolver: r, DefaultPlan: "basic", Now: func() time.Time { return t0.Add(time.Hour) }}
	_, err := tf.Rates(&canonical.Request{SubscriberID: "alice"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
	if !errors.Is(err, engine.ErrNoTariff) || errors.Is(err, ErrNoPlan) {
		t.Fatalf("%v", err)
	}
	// And the failure is not cached: the next call asks again and succeeds.
	src.err, src.offs = nil, map[string]string{"alice": "gold-offer"}
	rates, err := tf.Rates(&canonical.Request{SubscriberID: "alice"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
	if err != nil || rates[len(rates)-1].Num != 1 {
		t.Fatalf("%v %v", rates, err)
	}
}

func TestTariffPricesEachSubscriberOnItsOwnPlan(t *testing.T) {
	src := &fakeSource{offs: map[string]string{"alice": "gold-offer"}}
	r := NewProfileResolver(src, twoPlanStore(t))
	tf := &Tariff{Store: r.Store, Resolver: r, DefaultPlan: "basic", Now: func() time.Time { return t0.Add(time.Hour) }}
	cash := func(sub string) int64 {
		rates, err := tf.Rates(&canonical.Request{SubscriberID: sub}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rates[len(rates)-1].Num
	}
	if cash("alice") != 1 || cash("bob") != 4 {
		t.Fatalf("alice %d, bob %d", cash("alice"), cash("bob"))
	}
	r.Forget("alice")
	src.offs["alice"] = ""
	if cash("alice") != 4 {
		t.Fatal("a forgotten subscriber was not re-read")
	}
	if p, err := tf.PlanAt("alice", t0.Add(time.Hour)); err != nil || p != "basic" {
		t.Fatalf("%q %v", p, err)
	}
}

func TestCacheStaysBounded(t *testing.T) {
	src := &fakeSource{offs: map[string]string{}}
	r := NewProfileResolver(src, twoPlanStore(t))
	r.MaxSize = resolverShards * 4
	for i := 0; i < 5000; i++ {
		r.PlanFor(string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676)), t0.Add(time.Hour))
	}
	n := 0
	for i := range r.shards {
		n += len(r.shards[i].m)
	}
	if n > r.MaxSize {
		t.Fatalf("%d entries cached, limit %d", n, r.MaxSize)
	}
}

// With the real data layer: the offering attached by provisioning is what the
// rating path prices on.
func TestResolverAgainstTheDataLayer(t *testing.T) {
	rdb := redistest.Start(t)
	c := datalayer.New(rdb)
	ctx := context.Background()
	prov := func(op datalayer.ProvisionOp) {
		op.Now, op.Fingerprint = t0, op.Token
		if res, err := c.Provision(ctx, op); err != nil || res.Code != datalayer.ProvApplied {
			t.Fatalf("%v %+v", err, res)
		}
	}
	prov(datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: "alice", Token: "c1", Payment: "prepaid"})
	r := NewProfileResolver(c, twoPlanStore(t))
	at := t0.Add(time.Hour)
	if _, err := r.PlanFor("alice", at); !errors.Is(err, ErrNoPlan) {
		t.Fatalf("%v", err)
	}
	prov(datalayer.ProvisionOp{Op: datalayer.ProvOffering, Key: "alice", Token: "o1", Offering: "gold-offer"})
	r.Forget("alice")
	if p, err := r.PlanFor("alice", at); err != nil || p != "gold" {
		t.Fatalf("%q %v", p, err)
	}
}
