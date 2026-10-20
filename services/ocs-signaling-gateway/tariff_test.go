package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
)

func envOf(m map[string]string) func(string, string) string {
	return func(k, def string) string {
		if v, ok := m[k]; ok {
			return v
		}
		return def
	}
}

func TestTariffModes(t *testing.T) {
	rdb := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, _, err := newTariff(ctx, envOf(map[string]string{"OCS_TARIFF": "dynamic"}), rdb, telemetry.NewRegistry()); err == nil || !strings.Contains(err.Error(), "OCS_TARIFF") {
		t.Fatalf("%v", err)
	}
	if _, _, err := newTariff(ctx, envOf(map[string]string{"OCS_CASH_BYTES_RATE": "x"}), rdb, telemetry.NewRegistry()); err == nil {
		t.Fatal("a bad static rate must be refused")
	}
	tf, stop, err := newTariff(ctx, envOf(nil), rdb, telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	stop()
	rates, err := tf.Rates(&canonical.Request{}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
	if err != nil || rates[len(rates)-1].Num != 1 || rates[len(rates)-1].Den != 1024 {
		t.Fatalf("%v %v", rates, err)
	}

	cat := map[string]string{"OCS_TARIFF": "catalog", "OCS_DEFAULT_PLAN": "std"}
	if _, _, err := newTariff(ctx, envOf(map[string]string{"OCS_TARIFF": "catalog"}), rdb, telemetry.NewRegistry()); err == nil || !strings.Contains(err.Error(), "OCS_DEFAULT_PLAN") {
		t.Fatalf("%v", err)
	}
	// Nothing published yet: refuse to start rather than price nothing.
	if _, _, err := newTariff(ctx, envOf(cat), rdb, telemetry.NewRegistry()); err == nil || !strings.Contains(err.Error(), "no catalog version is in effect") {
		t.Fatalf("%v", err)
	}

	pub := &catalog.Replicator{Redis: rdb, Store: catalog.NewStore(0)}
	d := catalog.Document{EffectiveFrom: time.Now().Add(-time.Hour), Plans: []catalog.PricePlan{{ID: "std",
		Rules: []catalog.ChargeRule{{ID: "all", Cash: &catalog.Price{Num: 3, Den: 1}}}}}}
	if _, _, err := pub.Publish(ctx, d, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := newTariff(ctx, envOf(map[string]string{"OCS_TARIFF": "catalog", "OCS_DEFAULT_PLAN": "ghost"}), rdb, telemetry.NewRegistry()); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("%v", err)
	}

	tf, stop, err = newTariff(ctx, envOf(cat), rdb, telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	price := func() int64 {
		rates, err := tf.Rates(&canonical.Request{SubscriberID: "s"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rates[len(rates)-1].Num
	}
	if price() != 3 {
		t.Fatalf("price %d", price())
	}
	// A new version is picked up while the gateway keeps running.
	d.EffectiveFrom = time.Now().Add(-time.Minute)
	d.Plans[0].Rules[0].Cash.Num = 8
	if _, _, err := pub.Publish(ctx, d, 0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for price() != 8 {
		if time.Now().After(deadline) {
			t.Fatal("the gateway never picked up the new catalog version")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCatalogTariffPricesBySubscribersOffering(t *testing.T) {
	rdb := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := catalog.Document{EffectiveFrom: time.Now().Add(-time.Hour),
		Plans: []catalog.PricePlan{
			{ID: "basic", Rules: []catalog.ChargeRule{{ID: "r", Cash: &catalog.Price{Num: 4, Den: 1}}}},
			{ID: "gold", Rules: []catalog.ChargeRule{{ID: "r", Cash: &catalog.Price{Num: 1, Den: 1}}}},
		},
		Offerings: []catalog.ProductOffering{{ID: "gold-offer", PlanID: "gold"}},
	}
	if _, _, err := (&catalog.Replicator{Redis: rdb, Store: catalog.NewStore(0)}).Publish(ctx, d, 0); err != nil {
		t.Fatal(err)
	}
	dl := datalayer.New(rdb)
	for _, op := range []datalayer.ProvisionOp{
		{Op: datalayer.ProvCreate, Key: "alice", Token: "c1", Payment: "prepaid"},
		{Op: datalayer.ProvOffering, Key: "alice", Token: "o1", Offering: "gold-offer"},
		{Op: datalayer.ProvCreate, Key: "bob", Token: "c2", Payment: "prepaid"},
	} {
		op.Now, op.Fingerprint = time.Now(), op.Token
		if r, err := dl.Provision(ctx, op); err != nil || r.Code != datalayer.ProvApplied {
			t.Fatalf("%v %+v", err, r)
		}
	}
	tf, stop, err := newTariff(ctx, envOf(map[string]string{"OCS_TARIFF": "catalog", "OCS_DEFAULT_PLAN": "basic"}), rdb, telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	cash := func(sub string) int64 {
		rates, err := tf.Rates(&canonical.Request{SubscriberID: sub}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rates[len(rates)-1].Num
	}
	if cash("alice") != 1 || cash("bob") != 4 || cash("stranger") != 4 {
		t.Fatalf("alice %d (want 1), bob %d (want 4), stranger %d (want 4)", cash("alice"), cash("bob"), cash("stranger"))
	}
	if _, _, err := newTariff(ctx, envOf(map[string]string{"OCS_TARIFF": "catalog", "OCS_DEFAULT_PLAN": "basic", "OCS_PLAN_CACHE_TTL": "0s"}), rdb, telemetry.NewRegistry()); err == nil {
		t.Fatal("a zero cache TTL must be refused")
	}
}
