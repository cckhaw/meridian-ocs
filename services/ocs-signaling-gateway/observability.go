package main

import (
	"log/slog"
	"math"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/control"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

// memStats caches runtime.ReadMemStats (it stops the world briefly) so a
// scrape that reads several gauges pays for it once.
type memStats struct {
	mu   sync.Mutex
	at   time.Time
	last runtime.MemStats
}

func (m *memStats) get() runtime.MemStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Since(m.at) > time.Second {
		runtime.ReadMemStats(&m.last)
		m.at = time.Now()
	}
	return m.last
}

// registerMetrics exposes the gateway's counters. Everything is read at scrape
// time from atomics the components already keep.
func registerMetrics(reg *telemetry.Registry, eng *engine.Engine, adm *admission.Admission, res *identity.Resolver,
	store *datalayer.Client, brk *failpolicy.Breaker) {
	f := func(v uint64) float64 { return float64(v) }
	st := func() engine.Stats { return eng.Stats() }
	reg.CounterFunc("ocs_engine_requests_total", "Charging requests processed by the rating engine.", nil, func() float64 { return f(st().Requests) })
	reg.CounterFunc("ocs_engine_degraded_total", "Requests answered without the data tier (fail policy).", nil, func() float64 { return f(st().Degraded) })
	reg.CounterFunc("ocs_engine_overloaded_total", "Requests shed because a worker queue was full.", nil, func() float64 { return f(st().Overloaded) })
	reg.CounterFunc("ocs_engine_audit_dropped_total", "Audit events dropped because the audit channel was full.", nil, func() float64 { return f(st().AuditDropped) })
	reg.CounterFunc("ocs_engine_unreplicated_total", "Committed steps acknowledged by fewer replicas than required.", nil, func() float64 { return f(st().Unreplicated) })
	reg.CounterFunc("ocs_engine_unpriced_total", "Units the tariff could not price (sent with no rates).", nil, func() float64 { return f(st().Unpriced) })

	reg.GaugeFunc("ocs_admission_inflight", "Requests currently admitted.", nil, func() float64 { return float64(adm.Inflight()) })
	reg.CounterFunc("ocs_admission_rejected_total", "Requests refused by admission control (rate or concurrency).", nil, func() float64 { return f(adm.Rejected()) })

	reg.CounterFunc("ocs_identity_resolutions_total", "Subscriber identity resolutions.", nil, func() float64 { return f(res.Stats().Resolutions) })
	reg.CounterFunc("ocs_identity_unknown_total", "Resolutions that found no subscriber.", nil, func() float64 { return f(res.Stats().Unknown) })
	reg.CounterFunc("ocs_identity_conflicts_total", "Resolutions whose identifiers belong to different subscribers.", nil, func() float64 { return f(res.Stats().Conflicts) })
	reg.CounterFunc("ocs_identity_errors_total", "Resolutions that failed because the directory was unreachable.", nil, func() float64 { return f(res.Stats().Errors) })

	d := func() datalayer.DurabilityStats { return store.DurabilityStats() }
	reg.CounterFunc("ocs_durability_waits_total", "Commits that waited for replica acknowledgement.", nil, func() float64 { return f(d().Waits) })
	reg.CounterFunc("ocs_durability_short_total", "Commits acknowledged by fewer replicas than required.", nil, func() float64 { return f(d().Short) })
	reg.CounterFunc("ocs_durability_wait_errors_total", "Replica waits that failed.", nil, func() float64 { return f(d().WaitErrors) })
	reg.CounterFunc("ocs_durability_suspended_total", "Commits that skipped the wait while waiting was suspended.", nil, func() float64 { return f(d().Suspended) })

	reg.GaugeFunc("ocs_datatier_breaker_open", "1 while the data-tier circuit breaker is open (failures or forced).", nil, func() float64 {
		if brk.Open() {
			return 1
		}
		return 0
	})
	reg.GaugeFunc("ocs_datatier_breaker_forced", "1 while an operator holds the breaker open.", nil, func() float64 {
		if brk.Forced() {
			return 1
		}
		return 0
	})

	var ms memStats
	reg.GaugeFunc("go_goroutines", "Goroutines.", nil, func() float64 { return float64(runtime.NumGoroutine()) })
	reg.GaugeFunc("go_memstats_heap_alloc_bytes", "Heap bytes allocated and in use.", nil, func() float64 { return float64(ms.get().HeapAlloc) })
	reg.CounterFunc("go_gc_pause_seconds_total", "Total stop-the-world GC pause time.", nil, func() float64 { return float64(ms.get().PauseTotalNs) / 1e9 })
	reg.CounterFunc("go_gc_cycles_total", "Completed GC cycles.", nil, func() float64 { return float64(ms.get().NumGC) })
}

// controlTargets are the live objects the control plane may tune.
type controlTargets struct {
	Adm          *admission.Admission
	Breaker      *failpolicy.Breaker
	Level        *slog.LevelVar
	BaselineTPS  int // OCS_MAX_TPS; 0 = unlimited, in which case the rate is not tunable
	BaselineConc int // OCS_MAX_INFLIGHT; 0 = unlimited, likewise
}

// registerControls declares what an AI Ops agent may change, and how far.
//
// The ranges are relative to the deployed baseline so a bad value cannot push
// the gateway far from what the operator chose: the rate and concurrency limits
// may drop to a tenth (shed load) or rise to double (use headroom). A limit
// that is unlimited at start (0) is not tunable: "unlimited" is not a value an
// agent can restore, so there would be nothing safe to revert to.
//
// Deliberately absent: worker pool size (workers partition subscribers by hash;
// resizing at run time would break per-subscriber ordering) and anything that
// moves money.
func registerControls(ctl *control.Controller, t controlTargets) error {
	if t.BaselineTPS > 0 {
		cur := float64(t.BaselineTPS)
		if err := ctl.Register(control.Param{
			Name: "admission.max_tps", Kind: control.Float, Min: math.Max(1, cur/10), Max: cur * 2,
			Description: "Sustained admitted requests per second (Gy and Nchf together).",
			Get:         func() string { return strconv.FormatFloat(cur, 'g', -1, 64) },
			Set: func(v string) error {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return err
				}
				t.Adm.SetRate(f, int(f/10)+1)
				cur = f
				return nil
			},
		}); err != nil {
			return err
		}
	}
	if t.BaselineConc > 0 {
		cur := t.BaselineConc
		if err := ctl.Register(control.Param{
			Name: "admission.max_inflight", Kind: control.Int, Min: math.Max(1, float64(cur)/10), Max: float64(cur) * 2,
			Description: "Maximum concurrently admitted requests.",
			Get:         func() string { return strconv.Itoa(cur) },
			Set: func(v string) error {
				n, err := strconv.Atoi(v)
				if err != nil {
					return err
				}
				t.Adm.SetMaxInflight(n)
				cur = n
				return nil
			},
		}); err != nil {
			return err
		}
	}
	if err := ctl.Register(control.Param{
		Name: "log.level", Kind: control.Enum, Values: []string{"debug", "info", "warn", "error"},
		Description: "Log verbosity.",
		Get:         func() string { return levelName(t.Level.Level()) },
		Set: func(v string) error {
			l, ok := telemetry.ParseLevel(v)
			if !ok {
				return control.ErrInvalidValue
			}
			t.Level.Set(l)
			return nil
		},
	}); err != nil {
		return err
	}
	return ctl.Register(control.Param{
		Name: "datatier.breaker", Kind: control.Enum, Values: []string{"auto", "open"}, Risky: true,
		Description: "open holds the data-tier circuit breaker open: every charging request is answered by the fail policy (prepaid is refused) without touching Redis. Needs a second person's approval.",
		Get: func() string {
			if t.Breaker.Forced() {
				return "open"
			}
			return "auto"
		},
		Set: func(v string) error { t.Breaker.SetForced(v == "open"); return nil },
	})
}

func levelName(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "debug"
	case l <= slog.LevelInfo:
		return "info"
	case l <= slog.LevelWarn:
		return "warn"
	}
	return "error"
}
