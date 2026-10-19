package catalog

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

var t0 = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func doc(from time.Time) Document {
	return Document{
		EffectiveFrom: from,
		Plans: []PricePlan{{
			ID: "std", TimeZone: "Europe/Berlin",
			Filters: []PolicyFilter{{ID: "no-roam", Action: "deny", When: `rat == "ROAM"`}},
			Rules: []ChargeRule{
				{ID: "night-5g", Priority: 20, When: `rat == "NR" && (hour >= 22 || hour < 6)`, Cash: &Price{1, 2}},
				{ID: "data", Priority: 10, When: `unit == "bytes"`, Cash: &Price{2, 1}},
				{ID: "voice", Priority: 10, When: `unit == "seconds"`, Classes: []string{"bundle", "cash"}, Cash: &Price{3, 1}},
				{ID: "sms", Priority: 10, When: `unit == "events"`, Classes: []string{"promo"}},
			},
		}},
		Offerings: []ProductOffering{{
			ID: "gold", PlanID: "std",
			Allowances: []AllowanceBucket{{ID: "data10g", Class: "bundle", Unit: "bytes", Amount: 10 << 30, ValidityDays: 30, Order: 1}},
		}},
	}
}

func compile(t *testing.T, d Document) *Snapshot {
	t.Helper()
	d.Version = 1
	sn, _, err := Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	return sn
}

func price(t *testing.T, sn *Snapshot, plan string, at time.Time, rat string, kind canonical.UnitKind) ([]datalayer.Rate, Match, error) {
	t.Helper()
	req := canonical.Request{RATType: rat}
	u := canonical.UnitRequest{RatingGroup: 100, Kind: kind}
	return sn.Rates(plan, at, &req, &u, nil)
}

func TestRulePriorityConditionsAndClasses(t *testing.T) {
	sn := compile(t, doc(t0))
	// 23:30 UTC is 00:30 in Berlin: night rule for 5G applies (priority wins over "data").
	night := time.Date(2030, 6, 1, 22, 30, 0, 0, time.UTC) // 00:30 CEST
	rates, m, err := price(t, sn, "std", night, "NR", canonical.UnitBytes)
	if err != nil || m.RuleID != "night-5g" {
		t.Fatalf("%v %+v", err, m)
	}
	want := []datalayer.Rate{{Class: "promo", Num: 1, Den: 1}, {Class: "bundle", Num: 1, Den: 1}, {Class: "cash", Num: 1, Den: 2}}
	if fmt.Sprint(rates) != fmt.Sprint(want) {
		t.Fatalf("%v", rates)
	}
	// The same instant on LTE falls through to the data rule.
	if _, m, _ = price(t, sn, "std", night, "LTE", canonical.UnitBytes); m.RuleID != "data" {
		t.Fatalf("%+v", m)
	}
	// Midday in Berlin on 5G: not the night rule.
	if _, m, _ = price(t, sn, "std", time.Date(2030, 6, 1, 10, 0, 0, 0, time.UTC), "NR", canonical.UnitBytes); m.RuleID != "data" {
		t.Fatalf("%+v", m)
	}
	// Class restriction: voice cannot be paid from promo; sms only from promo.
	rates, _, _ = price(t, sn, "std", night, "LTE", canonical.UnitSeconds)
	if len(rates) != 2 || rates[0].Class != "bundle" || rates[1].Class != "cash" {
		t.Fatalf("%v", rates)
	}
	rates, _, _ = price(t, sn, "std", night, "LTE", canonical.UnitEvents)
	if len(rates) != 1 || rates[0].Class != "promo" {
		t.Fatalf("%v", rates)
	}
}

func TestFilterBarsAndErrors(t *testing.T) {
	sn := compile(t, doc(t0))
	if _, m, err := price(t, sn, "std", t0, "ROAM", canonical.UnitBytes); !errors.Is(err, ErrBarred) || m.FilterID != "no-roam" {
		t.Fatalf("%v %+v", err, m)
	}
	if _, _, err := price(t, sn, "nope", t0, "LTE", canonical.UnitBytes); !errors.Is(err, ErrUnknownPlan) {
		t.Fatalf("%v", err)
	}
	d := doc(t0)
	d.Plans[0].Rules = d.Plans[0].Rules[:1] // only the 5G night rule
	if _, _, err := price(t, compile(t, d), "std", t0, "LTE", canonical.UnitBytes); !errors.Is(err, ErrNoRule) {
		t.Fatalf("%v", err)
	}
}

func TestValidationReportsEverything(t *testing.T) {
	d := doc(t0)
	d.EffectiveFrom = time.Time{}
	d.Plans = append(d.Plans, PricePlan{ID: "std", Rules: []ChargeRule{{ID: "r"}}}) // duplicate id
	d.Plans[0].TimeZone = "Mars/Olympus"
	d.Plans[0].Filters = append(d.Plans[0].Filters, PolicyFilter{ID: "f2", Action: "allow", When: `rat ==`})
	d.Plans[0].Rules = append(d.Plans[0].Rules,
		ChargeRule{ID: "bad-expr", When: `bogus == 1`, Cash: &Price{1, 1}},
		ChargeRule{ID: "no-price", Classes: []string{"cash"}},
		ChargeRule{ID: "price-no-cash", Classes: []string{"promo"}, Cash: &Price{1, 1}},
		ChargeRule{ID: "huge", Cash: &Price{1 << 30, 1}},
		ChargeRule{ID: "zero-den", Cash: &Price{1, 0}},
		ChargeRule{ID: "classes", Classes: []string{"gold"}},
		ChargeRule{ID: "data", Cash: &Price{1, 1}}, // duplicate rule id
	)
	d.Offerings = append(d.Offerings,
		ProductOffering{ID: "gold", PlanID: "std"}, // duplicate
		ProductOffering{ID: "x", PlanID: "missing", Allowances: []AllowanceBucket{
			{ID: "a", Class: "cash", Unit: "gigs", Amount: 0},
			{ID: "a", Class: "bundle", Unit: "bytes", Amount: 1, ValidityDays: -1},
		}})
	_, _, err := Compile(d)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("%v", err)
	}
	all := ""
	for _, p := range ve.Problems {
		all += p.String() + "\n"
	}
	for _, want := range []string{
		"effectiveFrom: is required",
		"plans[1].id: duplicate plan id",
		"plans[0].timeZone: unknown time zone",
		"plans[0].filters[1].action",
		"plans[0].filters[1].when",
		"plans[0].rules[4].when",
		`a price is required when "cash" may pay`,
		`"cash" is not among the classes`,
		"num must be 1..",
		`unknown class "gold"`,
		`duplicate rule id "data"`,
		"offerings[1].id: duplicate offering id",
		`offerings[2].planId: unknown price plan "missing"`,
		"allowances[0].class",
		"allowances[0].unit",
		"allowances[0].amount",
		`duplicate allowance id "a"`,
		"allowances[1].validityDays",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in:\n%s", want, all)
		}
	}
	if len(ve.Problems) < 15 {
		t.Errorf("only %d problems reported", len(ve.Problems))
	}
}

func TestWarnings(t *testing.T) {
	d := doc(t0)
	d.Plans[0].Rules = append(d.Plans[0].Rules,
		ChargeRule{ID: "all", Priority: 5, Cash: &Price{1, 1}},
		ChargeRule{ID: "never", Priority: 1, Cash: &Price{1, 1}})
	d.Version = 1
	_, warns, err := Compile(d)
	if err != nil || len(warns) != 1 || !strings.Contains(warns[0].Message, "unreachable") {
		t.Fatalf("%v %v", err, warns)
	}
	// Without a catch-all, a warning says so.
	_, warns, _ = Compile(Document{EffectiveFrom: t0, Version: 1, Plans: []PricePlan{{ID: "p", Rules: []ChargeRule{{ID: "r", When: `rat == "NR"`}}}}})
	if len(warns) != 1 || !strings.Contains(warns[0].Message, "no catch-all") {
		t.Fatalf("%v", warns)
	}
}

func TestSnapshotOwnsItsSource(t *testing.T) {
	d := doc(t0)
	sn := compile(t, d)
	d.Plans[0].Rules[1].Cash.Num = 999 // the caller edits its copy afterwards
	d.Offerings[0].Allowances[0].Amount = 1
	got := sn.Document()
	if got.Plans[0].Rules[1].Cash.Num != 2 || got.Offerings[0].Allowances[0].Amount != 10<<30 {
		t.Fatal("a published snapshot changed with its source")
	}
	if o, ok := sn.Offering("gold"); !ok || o.Allowances[0].Amount != 10<<30 {
		t.Fatalf("%v %v", o, ok)
	}
}

func TestRatesDoNotAllocate(t *testing.T) {
	sn := compile(t, doc(t0))
	req := canonical.Request{RATType: "NR", CellID: "262-01-1"}
	u := canonical.UnitRequest{RatingGroup: 100, Kind: canonical.UnitBytes}
	buf := make([]datalayer.Rate, 0, 8)
	now := time.Date(2030, 6, 1, 22, 30, 0, 0, time.UTC)
	if n := testing.AllocsPerRun(1000, func() { buf, _, _ = sn.Rates("std", now, &req, &u, buf[:0]) }); n != 0 {
		t.Fatalf("%v allocations", n)
	}
}

func BenchmarkRates(b *testing.B) {
	d := doc(t0)
	d.Version = 1
	sn, _, _ := Compile(d)
	req := canonical.Request{RATType: "NR"}
	u := canonical.UnitRequest{RatingGroup: 100, Kind: canonical.UnitBytes}
	buf := make([]datalayer.Rate, 0, 8)
	now := time.Date(2030, 6, 1, 22, 30, 0, 0, time.UTC)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, _, _ = sn.Rates("std", now, &req, &u, buf[:0])
	}
}

// ---- store / time travel ----

func TestTimeTravelAndAppendOnlyHistory(t *testing.T) {
	s := NewStore(0)
	if s.At(t0) != nil || s.LatestVersion() != 0 || s.Current() != nil {
		t.Fatal("an empty store has no version")
	}
	// v1 from t0, v2 future-dated, v3 further out.
	for i, from := range []time.Time{t0, t0.Add(24 * time.Hour), t0.Add(72 * time.Hour)} {
		d := doc(from)
		d.Plans[0].Rules[1].Cash = &Price{int64(i + 2), 1}
		sn, _, err := s.Publish(d, int64(i+1))
		if err != nil || sn.Version != int64(i+1) {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for _, c := range []struct {
		at   time.Time
		want int64
	}{
		{t0.Add(-time.Second), 0},
		{t0, 1},
		{t0.Add(23 * time.Hour), 1},
		{t0.Add(24 * time.Hour), 2},
		{t0.Add(71 * time.Hour), 2},
		{t0.Add(72 * time.Hour), 3},
		{t0.Add(1000 * time.Hour), 3},
	} {
		got := s.At(c.at)
		switch {
		case c.want == 0 && got != nil:
			t.Errorf("at %v: version %d, want none", c.at, got.Version)
		case c.want != 0 && (got == nil || got.Version != c.want):
			t.Errorf("at %v: %v, want %d", c.at, got, c.want)
		}
	}
	// The price in force is the one dated for that moment.
	for at, want := range map[time.Time]int64{t0: 2, t0.Add(30 * time.Hour): 3, t0.Add(80 * time.Hour): 4} {
		sn := s.At(at)
		rates, _, err := sn.Rates("std", at, &canonical.Request{RATType: "LTE"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil || rates[2].Num != want {
			t.Errorf("at %v: %v %v, want cash num %d", at, rates, err, want)
		}
	}
	if v, ok := s.Version(2); !ok || v.Version != 2 {
		t.Fatal("Version(2)")
	}
}

func TestPublishRules(t *testing.T) {
	s := NewStore(0)
	if _, _, err := s.Publish(doc(t0), 2); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("wrong expected version: %v", err)
	}
	if _, _, err := s.Publish(doc(t0), 0); err != nil { // 0 = don't care
		t.Fatal(err)
	}
	if _, _, err := s.Publish(doc(t0), 1); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale editor: %v", err)
	}
	if _, _, err := s.Publish(doc(t0), 0); !errors.Is(err, ErrNotMonotonic) {
		t.Fatalf("same effective time: %v", err)
	}
	if _, _, err := s.Publish(doc(t0.Add(-time.Hour)), 0); !errors.Is(err, ErrNotMonotonic) {
		t.Fatalf("back-dated: %v", err)
	}
	bad := doc(t0.Add(time.Hour))
	bad.Plans[0].Rules[0].When = `((`
	if _, _, err := s.Publish(bad, 0); err == nil || s.LatestVersion() != 1 {
		t.Fatalf("an invalid document must not change the store: %v", err)
	}
	// Install enforces the same invariants for replicated snapshots.
	sn := compile(t, doc(t0.Add(time.Hour)))
	sn.Version = 5
	if err := s.Install(sn); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("gap: %v", err)
	}
}

func TestRetentionKeepsTheVersionInEffect(t *testing.T) {
	s := NewStore(3)
	now := t0.Add(1000 * time.Hour)
	s.now = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		if _, _, err := s.Publish(doc(t0.Add(time.Duration(i)*time.Hour)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(s.Versions()); n != 3 {
		t.Fatalf("%d retained", n)
	}
	if s.Current().Version != 10 {
		t.Fatal("current version lost")
	}

	// With the clock before most versions, the one in effect and the future
	// ones must survive trimming.
	s = NewStore(3)
	s.now = func() time.Time { return t0.Add(2 * time.Hour) } // v3 in effect
	for i := 0; i < 8; i++ {
		if _, _, err := s.Publish(doc(t0.Add(time.Duration(i)*time.Hour)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if s.Current() == nil || s.Current().Version != 3 {
		t.Fatalf("version in effect lost: %+v", s.Current())
	}
	if s.LatestVersion() != 8 {
		t.Fatal("future versions lost")
	}
}

func TestReadersNeverBlockOrSeeATornTimeline(t *testing.T) {
	s := NewStore(8)
	if _, _, err := s.Publish(doc(t0), 0); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var reads atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				sn := s.At(t0.Add(500 * time.Hour))
				if sn == nil || sn.Version < 1 || !sn.HasPlan("std") {
					t.Error("torn read")
					return
				}
				reads.Add(1)
			}
		}()
	}
	for i := 1; i <= 200; i++ {
		if _, _, err := s.Publish(doc(t0.Add(time.Duration(i)*time.Hour)), 0); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	if reads.Load() == 0 || s.LatestVersion() != 201 {
		t.Fatalf("reads %d, version %d", reads.Load(), s.LatestVersion())
	}
}

func TestConcurrentPublishersGetDistinctVersions(t *testing.T) {
	s := NewStore(0)
	var ok, conflict atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Every editor believes the next version is 1.
			_, _, err := s.Publish(doc(t0.Add(time.Duration(g)*time.Hour)), 1)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrVersionConflict):
				conflict.Add(1)
			default:
				t.Error(err)
			}
		}(g)
	}
	wg.Wait()
	if ok.Load() != 1 || conflict.Load() != 15 {
		t.Fatalf("ok=%d conflict=%d", ok.Load(), conflict.Load())
	}
}

// ---- tariff adapter ----

func TestTariffUsesTheVersionInEffectAndMapsErrors(t *testing.T) {
	s := NewStore(0)
	now := t0.Add(time.Hour)
	d1 := doc(t0)
	d2 := doc(t0.Add(48 * time.Hour))
	d2.Plans[0].Rules[1].Cash = &Price{9, 1}
	for _, d := range []Document{d1, d2} {
		if _, _, err := s.Publish(d, 0); err != nil {
			t.Fatal(err)
		}
	}
	tf := &Tariff{Store: s, DefaultPlan: "std", Resolver: MapResolver{"vip": "std"}, Now: func() time.Time { return now }}
	req := &canonical.Request{SubscriberID: "alice", RATType: "LTE"}
	u := &canonical.UnitRequest{Kind: canonical.UnitBytes}

	rates, err := tf.Rates(req, u, nil)
	if err != nil || rates[2].Num != 2 {
		t.Fatalf("%v %v", rates, err)
	}
	now = t0.Add(49 * time.Hour) // v2 takes effect with no restart and no flush
	if rates, _ = tf.Rates(req, u, nil); rates[2].Num != 9 {
		t.Fatalf("%v", rates)
	}
	// Re-rating the past or pricing the future.
	if rates, _, _ := tf.RatesAt(t0, "std", req, u, nil); rates[2].Num != 2 {
		t.Fatalf("%v", rates)
	}
	if _, _, err := tf.RatesAt(t0.Add(-time.Hour), "std", req, u, nil); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("%v", err)
	}

	// Refusals are engine.ErrNoTariff so the engine prices the unit with no rates.
	req.RATType = "ROAM"
	if _, err := tf.Rates(req, u, nil); !errors.Is(err, engine.ErrNoTariff) || !errors.Is(err, ErrBarred) {
		t.Fatalf("%v", err)
	}
	tf.DefaultPlan = ""
	req.SubscriberID = "nobody"
	if _, err := tf.Rates(req, u, nil); !errors.Is(err, engine.ErrNoTariff) || !errors.Is(err, ErrUnknownPlan) {
		t.Fatalf("%v", err)
	}
	if _, err := (&Tariff{Store: NewStore(0)}).Rates(req, u, nil); !errors.Is(err, engine.ErrNoTariff) || !errors.Is(err, ErrNoVersion) {
		t.Fatalf("%v", err)
	}
}

// ---- dry run ----

func TestDryRunShowsWhatWouldChange(t *testing.T) {
	live := NewStore(0)
	if _, _, err := live.Publish(doc(t0), 0); err != nil {
		t.Fatal(err)
	}
	next := doc(t0.Add(24 * time.Hour))
	next.Plans[0].Rules[1].Cash = &Price{5, 1} // data price 2 -> 5
	next.Version = 2
	prop, _, err := Compile(next)
	if err != nil {
		t.Fatal(err)
	}
	at := t0.Add(2 * time.Hour)
	res, err := DryRun(live, prop, []Case{
		{Name: "data lte", PlanID: "std", At: at, RAT: "LTE"},
		{Name: "sms", PlanID: "std", At: at, Unit: "events"},
		{Name: "roaming", PlanID: "std", At: at, RAT: "ROAM"},
		{Name: "no plan", PlanID: "ghost", At: at},
		{Name: "bad unit", PlanID: "std", At: at, Unit: "gigs"},
		{Name: "before history", PlanID: "std", At: t0.Add(-time.Hour)},
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].Changed || res[0].Live.Rates[2].Num != 2 || res[0].Proposed.Rates[2].Num != 5 || res[0].Live.RuleID != "data" {
		t.Fatalf("%+v", res[0])
	}
	if res[1].Changed {
		t.Fatalf("an untouched rule reported as changed: %+v", res[1])
	}
	if res[2].Live.Error != "barred" || res[2].Live.FilterID != "no-roam" || res[2].Changed {
		t.Fatalf("%+v", res[2])
	}
	if res[3].Live.Error != "unknown_plan" || res[4].Live.Error != "invalid_unit" {
		t.Fatalf("%+v %+v", res[3], res[4])
	}
	if res[5].Live.Error != "no_version" || !res[5].Changed {
		t.Fatalf("%+v", res[5])
	}
	if live.LatestVersion() != 1 {
		t.Fatal("a dry run must not publish")
	}
	if _, err := DryRun(nil, prop, make([]Case, MaxDryRunCases+1), t0); err == nil {
		t.Fatal("case limit")
	}
	// First ever catalog: nothing live.
	if res, _ = DryRun(nil, prop, []Case{{PlanID: "std", RAT: "LTE"}}, t0); res[0].Live.Error != "no_version" {
		t.Fatalf("%+v", res[0])
	}
}
