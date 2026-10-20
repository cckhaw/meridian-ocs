package main

import (
	"context"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
)

// commandLagBuckets cover a healthy pipeline (milliseconds) and a backed-up one
// (minutes).
var commandLagBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

func f(v uint64) float64 { return float64(v) }

// registerWorkerMetrics exposes what the command consumer did. Outcomes are
// final: "failed" is a business refusal recorded for the caller, not an error of
// the service.
func (a *App) registerWorkerMetrics() {
	w, reg := a.wrk, a.reg
	reg.CounterFunc("ocs_prov_commands_total", "Commands finished by outcome.", telemetry.Labels{"outcome": "applied"}, func() float64 { return f(w.Stats().Applied) })
	reg.CounterFunc("ocs_prov_commands_total", "Commands finished by outcome.", telemetry.Labels{"outcome": "failed"}, func() float64 { return f(w.Stats().Failed) })
	reg.CounterFunc("ocs_prov_dead_lettered_total", "Messages parked on the dead-letter topic.", nil, func() float64 { return f(w.Stats().DeadLettered) })
	reg.CounterFunc("ocs_prov_worker_retries_total", "Infrastructure retries (Redis or Kafka) while applying commands.", nil, func() float64 { return f(w.Stats().Retries) })
	reg.CounterFunc("ocs_prov_worker_commits_total", "Consumer batches whose offsets were committed.", nil, func() float64 { return f(w.Stats().Commits) })
}

// registerGraceMetrics exposes the grace-expiry sweeper. Overdue is the number of subscribers past
// their grace deadline after the last pass: it should be zero except for a moment.
func (a *App) registerGraceMetrics() {
	g, reg := a.grace, a.reg
	reg.CounterFunc("ocs_prov_grace_expired_total", "Subscribers suspended because their grace period ended.", nil, func() float64 { return f(g.Stats().Suspended) })
	reg.CounterFunc("ocs_prov_grace_stale_total", "Overdue grace entries skipped because the subscriber had recovered, been given longer or no longer exists.", nil, func() float64 { return f(g.Stats().Stale) })
	reg.CounterFunc("ocs_prov_grace_sweep_errors_total", "Grace sweeps or subscribers that failed on Redis or Kafka and will be retried.", nil, func() float64 { return f(g.Stats().Errors) })
	reg.GaugeFunc("ocs_prov_grace_overdue", "Subscribers past their grace deadline and not yet suspended, after the last sweep.", nil, func() float64 { return float64(g.Stats().Overdue) })
	reg.GaugeFunc("ocs_prov_grace_oldest_overdue_seconds", "How long the longest-overdue grace period has waited, after the last sweep.", nil, func() float64 { return g.Stats().OldestAge.Seconds() })
}

// registerAPIMetrics exposes what became of each submission.
func (a *App) registerAPIMetrics(svc *service.Service) {
	reg := a.reg
	for name, get := range map[string]func(service.SubmitStats) uint64{
		"accepted":    func(s service.SubmitStats) uint64 { return s.Accepted },
		"duplicate":   func(s service.SubmitStats) uint64 { return s.Duplicates },
		"rejected":    func(s service.SubmitStats) uint64 { return s.Rejected },
		"conflict":    func(s service.SubmitStats) uint64 { return s.Conflicts },
		"unavailable": func(s service.SubmitStats) uint64 { return s.Unavailable },
	} {
		get := get
		reg.CounterFunc("ocs_prov_submissions_total", "Command submissions by result.", telemetry.Labels{"result": name},
			func() float64 { return f(get(svc.Stats())) })
	}
	for _, t := range command.Types {
		t := t
		reg.CounterFunc("ocs_prov_accepted_by_type_total", "Accepted commands by type.", telemetry.Labels{"type": string(t)},
			func() float64 { return f(svc.Stats().ByType[t]) })
	}
}

// registerApprovalMetrics exposes what happened to requests that needed a second
// person, and how many wait now (read from Redis every 30 s, not at scrape time).
func (a *App) registerApprovalMetrics(svc *service.Service) {
	reg := a.reg
	for name, get := range map[string]func(service.ApprovalStats) uint64{
		"requested":           func(s service.ApprovalStats) uint64 { return s.Requested },
		"approved":            func(s service.ApprovalStats) uint64 { return s.Approved },
		"declined":            func(s service.ApprovalStats) uint64 { return s.Declined },
		"refused_self":        func(s service.ApprovalStats) uint64 { return s.RefusedSelf },
		"refused_expired":     func(s service.ApprovalStats) uint64 { return s.RefusedExpired },
		"refused_not_pending": func(s service.ApprovalStats) uint64 { return s.RefusedNotPending },
	} {
		get := get
		reg.CounterFunc("ocs_prov_approvals_total", "Approval requests and decisions by result.", telemetry.Labels{"result": name},
			func() float64 { return f(get(svc.ApprovalStats())) })
	}
	reg.CounterFunc("ocs_prov_approval_event_errors_total", "Approval trail events that could not be published.", nil,
		func() float64 { return f(svc.ApprovalStats().EventErrors) })
	reg.GaugeFunc("ocs_prov_approvals_pending", "Requests waiting for a second person's approval.", nil,
		func() float64 { return float64(a.pending.Load()) })
	reg.GaugeFunc("ocs_prov_approvals_oldest_age_seconds", "How long the oldest waiting request has waited.", nil,
		func() float64 { return float64(a.pendingOldest.Load()) })
}

// refreshPending keeps the pending-approval gauges current.
func (a *App) refreshPending(ctx context.Context, svc *service.Service) {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		if n, age, err := svc.PendingStats(ctx); err == nil {
			a.pending.Store(n)
			a.pendingOldest.Store(int64(age.Seconds()))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// registerCommonMetrics exposes the data-tier durability counters (provisioning
// moves money, so an unreplicated commit matters here as much as in the gateway)
// and the Go runtime.
func (a *App) registerCommonMetrics(data *datalayer.Client) {
	reg := a.reg
	d := func() datalayer.DurabilityStats { return data.DurabilityStats() }
	reg.CounterFunc("ocs_durability_waits_total", "Commits that waited for replica acknowledgement.", nil, func() float64 { return f(d().Waits) })
	reg.CounterFunc("ocs_durability_short_total", "Commits acknowledged by fewer replicas than required.", nil, func() float64 { return f(d().Short) })
	reg.CounterFunc("ocs_durability_wait_errors_total", "Replica waits that failed.", nil, func() float64 { return f(d().WaitErrors) })
	reg.CounterFunc("ocs_durability_suspended_total", "Commits that skipped the wait while waiting was suspended.", nil, func() float64 { return f(d().Suspended) })
	telemetry.RegisterRuntime(reg)
}

// Metrics returns the registry served on /metrics (for tests and tools).
func (a *App) Metrics() *telemetry.Registry { return a.reg }
