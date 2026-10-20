package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/pkg/unsettled"
)

func TestTheUnsettledSpoolHasToBeChosen(t *testing.T) {
	t.Setenv("OCS_UNSETTLED_SPOOL", "")
	if s, err := openUnsettledSpool(); err == nil || s != nil || !strings.Contains(err.Error(), "OCS_UNSETTLED_SPOOL") {
		t.Fatalf("no choice must be refused, not defaulted: %v", err)
	}
	t.Setenv("OCS_UNSETTLED_SPOOL", "off")
	if s, err := openUnsettledSpool(); err != nil || s != nil {
		t.Fatalf("off is a valid, deliberate choice: %v", err)
	}
	dir := t.TempDir() + "/spool"
	t.Setenv("OCS_UNSETTLED_SPOOL", dir)
	s, err := openUnsettledSpool()
	if err != nil || s == nil {
		t.Fatalf("a directory opens (and is created): %v", err)
	}
	_ = s.Close()
	t.Setenv("OCS_UNSETTLED_SPOOL", "/proc/nope/spool")
	if _, err := openUnsettledSpool(); err == nil {
		t.Fatal("a directory that cannot be used must stop the gateway at start-up")
	}
}

func TestUnsettledMetricsAreExposed(t *testing.T) {
	spool, err := unsettled.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	req := canonical.AcquireRequest()
	req.Type, req.SubscriberID, req.SessionID, req.RequestNumber = canonical.TypeTerminate, "u", "s", 1
	req.Units = append(req.Units, canonical.UnitRequest{RatingGroup: 1, Used: 5})
	if err := spool.Record(req); err != nil {
		t.Fatal(err)
	}
	eng, _ := engine.New(engine.Config{Workers: 1}, engine.Deps{Store: nopStore{}, Tariff: engine.StaticTariff{}})
	defer eng.Close()
	reg := telemetry.NewRegistry()
	registerUnsettledMetrics(reg, eng, spool, &unsettled.Reconciler{Spool: spool, Settler: eng})
	var out bytes.Buffer
	_, _ = reg.WriteTo(&out)
	for _, want := range []string{
		"ocs_unsettled_pending 1", "ocs_unsettled_oldest_age_seconds ", "ocs_unsettled_dead_letters 0", "ocs_unsettled_settled_total 0",
		"ocs_unsettled_dead_total 0", "ocs_unsettled_ambiguous_total 0", "ocs_engine_unsettled_updates_total 0", "ocs_unsettled_deferred_total 0", "ocs_engine_unsettled_recorded_total 0", "ocs_engine_unsettled_unrecorded_total 0",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
	// With the spool off the same names exist, as zeros, so alerts and dashboards always find them.
	reg2 := telemetry.NewRegistry()
	registerUnsettledMetrics(reg2, eng, nil, nil)
	out.Reset()
	_, _ = reg2.WriteTo(&out)
	if !strings.Contains(out.String(), "ocs_unsettled_pending 0") {
		t.Errorf("metrics must exist with the spool off:\n%s", out.String())
	}
}

type nopStore struct{}

func (nopStore) Charge(context.Context, *datalayer.Request, *datalayer.Response) error { return nil }
func (nopStore) SweepExpired(context.Context, string, time.Time) (int, error)          { return 0, nil }

func TestPeerRepeatsUsageSetting(t *testing.T) {
	for v, want := range map[string]bool{"": false, "false": false, "true": true} {
		t.Setenv("OCS_UNSETTLED_PEER_REPEATS", v)
		if got, err := peerRepeatsUsage(); err != nil || got != want {
			t.Errorf("%q: %v %v", v, got, err)
		}
	}
	t.Setenv("OCS_UNSETTLED_PEER_REPEATS", "yes")
	if _, err := peerRepeatsUsage(); err == nil || !strings.Contains(err.Error(), "OCS_UNSETTLED_PEER_REPEATS") {
		t.Errorf("a typo must be refused, not read as false: %v", err)
	}
}
