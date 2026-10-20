package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/pkg/unsettled"
)

// openUnsettledSpool applies OCS_UNSETTLED_SPOOL: a directory for the log of terminates that were acknowledged
// while Redis was down, or "off". There is no default: running without the spool means that usage reported in
// such a terminate is lost (the engine counts it as unrecorded), which has to be a deliberate choice.
func openUnsettledSpool() (*unsettled.Spool, error) {
	switch v := os.Getenv("OCS_UNSETTLED_SPOOL"); v {
	case "":
		return nil, fmt.Errorf("OCS_UNSETTLED_SPOOL must be set to a directory (for the log of usage that could not be charged while Redis was down) or \"off\"")
	case "off":
		log.Printf("ocs-signaling-gateway: WARNING OCS_UNSETTLED_SPOOL=off: usage reported in a terminate that arrives while Redis is down is acknowledged and then LOST (counted in ocs_engine_unsettled_unrecorded_total)")
		return nil, nil
	default:
		s, err := unsettled.Open(v)
		if err != nil {
			return nil, fmt.Errorf("OCS_UNSETTLED_SPOOL: %w", err)
		}
		if st := s.Stats(); st.Pending > 0 || st.Dead > 0 || st.Corrupt > 0 {
			log.Printf("ocs-signaling-gateway: unsettled usage spool %s: %d pending, %d dead letters, %d unreadable lines (kept in a side file)", v, st.Pending, st.Dead, st.Corrupt)
		}
		return s, nil
	}
}

// startReconciler replays the spool against the engine until ctx is done.
func startReconciler(ctx context.Context, s *unsettled.Spool, eng *engine.Engine, logf func(string, ...any)) *unsettled.Reconciler {
	if s == nil {
		return nil
	}
	r := &unsettled.Reconciler{Spool: s, Settler: eng, Logf: logf}
	go r.Run(ctx)
	return r
}

// registerUnsettledMetrics exposes the engine's accounting of usage released without being charged and, when the
// spool is on, how the reconciliation is doing. They are registered (as zeros) even with the spool off so the
// alerts and dashboards always find them.
func registerUnsettledMetrics(reg *telemetry.Registry, eng *engine.Engine, s *unsettled.Spool, r *unsettled.Reconciler) {
	f := func(v uint64) float64 { return float64(v) }
	st := func() engine.Stats { return eng.Stats() }
	reg.CounterFunc("ocs_engine_unsettled_recorded_total", "Unsettled terminates saved in the spool for reconciliation.", nil, func() float64 { return f(st().UnsettledRecorded) })
	reg.CounterFunc("ocs_engine_unsettled_unrecorded_total", "Unsettled terminates whose usage is LOST: no spool, or writing the record failed.", nil, func() float64 { return f(st().UnsettledUnrecorded) })

	sp := func() unsettled.Stats {
		if s == nil {
			return unsettled.Stats{}
		}
		return s.Stats()
	}
	rc := func() unsettled.ReconcilerStats {
		if r == nil {
			return unsettled.ReconcilerStats{}
		}
		return r.Stats()
	}
	reg.GaugeFunc("ocs_unsettled_pending", "Unsettled terminates waiting to be charged.", nil, func() float64 { return float64(sp().Pending) })
	reg.GaugeFunc("ocs_unsettled_oldest_age_seconds", "Age of the oldest unsettled terminate still waiting.", nil, func() float64 { return sp().OldestAge.Seconds() })
	reg.GaugeFunc("ocs_unsettled_dead_letters", "Unsettled terminates that can never be charged and are kept for inspection.", nil, func() float64 { return float64(sp().Dead) })
	reg.CounterFunc("ocs_unsettled_settled_total", "Unsettled terminates charged by the reconciler.", nil, func() float64 { return f(rc().Settled) })
	reg.CounterFunc("ocs_unsettled_dead_total", "Unsettled terminates the reconciler found could never be charged (the session is gone).", nil, func() float64 { return f(rc().Dead) })
	reg.CounterFunc("ocs_unsettled_deferred_total", "Reconciliation passes that stopped because the data tier could not take the usage yet.", nil, func() float64 { return f(rc().Deferred) })
}
