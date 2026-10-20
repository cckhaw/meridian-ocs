package main

import (
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
