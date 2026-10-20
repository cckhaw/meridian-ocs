package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/control"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

func TestControlTargetsAreTunedWithinTheirBounds(t *testing.T) {
	adm := admission.New(1000, 101, 500)
	brk := failpolicy.NewBreaker(5, time.Second, nil, nil)
	var out bytes.Buffer
	lg, lv := telemetry.NewLogger(&out, "gw", slog.LevelInfo)
	ctl := control.New(control.Config{Service: "gw"})
	defer ctl.Close()
	if err := registerControls(ctl, controlTargets{Adm: adm, Breaker: brk, Level: lv, BaselineTPS: 1000, BaselineConc: 500}); err != nil {
		t.Fatal(err)
	}
	names := map[string]control.ParamInfo{}
	for _, p := range ctl.List() {
		names[p.Name] = p
	}
	if len(names) != 4 || names["admission.max_tps"].Min != 100 || names["admission.max_tps"].Max != 2000 ||
		names["admission.max_inflight"].Min != 50 || names["admission.max_inflight"].Max != 1000 || !names["datatier.breaker"].Risky || names["log.level"].Current != "info" {
		t.Fatalf("%+v", names)
	}

	// Concurrency: lower it to the minimum and the gateway really admits less.
	ch, err := ctl.Propose("agent", "admission.max_inflight", "1", time.Minute, "overload")
	if err != nil || ch.Applied != "50" || !ch.Clamped {
		t.Fatalf("%+v %v", ch, err)
	}
	admitted := 0
	for i := 0; i < 100; i++ {
		if adm.Acquire() {
			admitted++
		}
	}
	if admitted != 50 {
		t.Fatalf("%d admitted under a limit of 50", admitted)
	}
	if err := ctl.Revert(ch.ID, "agent"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		adm.Release()
	}
	if !adm.Acquire() {
		t.Fatal("the limit was not restored")
	}

	// Log level.
	lg.Debug("hidden")
	if _, err := ctl.Propose("agent", "log.level", "debug", time.Minute, "investigating"); err != nil {
		t.Fatal(err)
	}
	lg.Debug("visible")
	if strings.Contains(out.String(), "hidden") || !strings.Contains(out.String(), "visible") {
		t.Fatalf("%s", out.String())
	}

	// The breaker needs a second person.
	p, _ := ctl.Propose("agent", "datatier.breaker", "open", 0, "redis flapping")
	if brk.Forced() {
		t.Fatal("forced before approval")
	}
	if _, err := ctl.Approve(p.ID, "agent"); err == nil {
		t.Fatal("self-approval")
	}
	if _, err := ctl.Approve(p.ID, "oncall"); err != nil || !brk.Forced() || brk.AllowDataTier() {
		t.Fatalf("%v forced=%v", err, brk.Forced())
	}
	ctl.Close()
	if brk.Forced() || lv.Level() != slog.LevelInfo {
		t.Fatal("closing the controller must undo unconfirmed changes")
	}
}

func TestUnlimitedBaselinesAreNotTunable(t *testing.T) {
	ctl := control.New(control.Config{Service: "gw"})
	defer ctl.Close()
	lv := new(slog.LevelVar)
	if err := registerControls(ctl, controlTargets{Adm: admission.New(0, 1, 0), Breaker: failpolicy.NewBreaker(5, time.Second, nil, nil), Level: lv}); err != nil {
		t.Fatal(err)
	}
	for _, p := range ctl.List() {
		if strings.HasPrefix(p.Name, "admission.") {
			t.Fatalf("%s is tunable although it starts unlimited", p.Name)
		}
	}
}

func TestMetricsReflectTheGateway(t *testing.T) {
	rdb := redistest.Start(t)
	store := datalayer.New(rdb)
	if err := store.PutBucket(context.Background(), "m1", datalayer.Bucket{ID: "b", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 10_000, Order: 1}); err != nil {
		t.Fatal(err)
	}
	reg := telemetry.NewRegistry()
	lat := reg.Histogram("ocs_engine_rate_duration_seconds", "x", nil)
	brk := failpolicy.NewBreaker(5, time.Second, nil, nil)
	eng, err := engine.New(engine.Config{Workers: 2}, engine.Deps{Store: store, Latency: lat, Breaker: brk,
		Tariff: engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: {Num: 1, Den: 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	adm := admission.New(0, 1, 1)
	adm.Acquire()
	adm.Acquire() // rejected
	res := identity.NewPassthroughResolver()
	registerMetrics(reg, eng, adm, res, store, brk)

	r := canonical.AcquireRequest()
	r.Type, r.SubscriberID, r.SessionID = canonical.TypeInitial, "m1", "s"
	r.Units = append(r.Units, canonical.UnitRequest{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 100})
	if err := eng.Rate(context.Background(), r, canonical.AcquireResponse()); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	reg.WriteTo(&b)
	out := b.String()
	for _, want := range []string{
		"ocs_engine_requests_total 1\n", "ocs_admission_inflight 1\n", "ocs_admission_rejected_total 1\n",
		"ocs_datatier_breaker_open 0\n", "ocs_engine_rate_duration_seconds_count 1\n",
		"# TYPE ocs_engine_rate_duration_seconds histogram", "go_goroutines ", "go_gc_pause_seconds_total ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	brk.SetForced(true)
	b.Reset()
	reg.WriteTo(&b)
	if !strings.Contains(b.String(), "ocs_datatier_breaker_forced 1\n") || !strings.Contains(b.String(), "ocs_datatier_breaker_open 1\n") {
		t.Fatal("breaker gauges")
	}
}
