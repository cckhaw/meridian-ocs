package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// TimelineSecond is what happened to the sessions that were due in one second of the measured window.
type TimelineSecond struct {
	T           int    `json:"t"`
	Started     uint64 `json:"started"`
	OK          uint64 `json:"ok"`
	Failed      uint64 `json:"failed"`
	MaxCreateUs uint64 `json:"maxCreateFromDueUs"`
}

// LatencySummary is one histogram, in microseconds.
type LatencySummary struct {
	Count uint64 `json:"count"`
	Mean  int64  `json:"meanUs"`
	P50   int64  `json:"p50Us"`
	P90   int64  `json:"p90Us"`
	P99   int64  `json:"p99Us"`
	P999  int64  `json:"p999Us"`
	Max   int64  `json:"maxUs"`
}

func summarize(h *Hist) LatencySummary {
	us := func(d time.Duration) int64 { return d.Microseconds() }
	return LatencySummary{h.Count(), us(h.Mean()), us(h.Quantile(0.5)), us(h.Quantile(0.9)), us(h.Quantile(0.99)), us(h.Quantile(0.999)), us(h.Max())}
}

// Report is everything a run measured, written as JSON with --json.
type Report struct {
	Config struct {
		Rate        float64 `json:"offeredSessionsPerSecond"`
		Duration    string  `json:"duration"`
		Warmup      string  `json:"warmup"`
		Subscribers int     `json:"subscribers"`
		Conns       int     `json:"connections"`
		RequestSize int64   `json:"requestBytes"`
		Updates     int     `json:"updatesPerSession"`
		UsedPercent int64   `json:"usedPercent"`
	} `json:"config"`
	MeasuredSeconds    float64                   `json:"measuredSeconds"`
	Sessions           map[string]uint64         `json:"sessions"`
	Requests           uint64                    `json:"requests"`
	AchievedSessionsPS float64                   `json:"achievedSessionsPerSecond"`
	AchievedRequestsPS float64                   `json:"achievedRequestsPerSecond"`
	Statuses           map[string]uint64         `json:"statuses"`
	Latency            map[string]LatencySummary `json:"latency"`
	GatewayCounted     *float64                  `json:"gatewayCountedRequests,omitempty"`
	ClientSent         uint64                    `json:"clientSentRequests"`
	RefCollisions      uint64                    `json:"referenceCollisions"`
	Timeline           []TimelineSecond          `json:"timeline,omitempty"`
	Verification       *Verification             `json:"verification,omitempty"`
	SLO                []string                  `json:"sloViolations,omitempty"`
	Pass               bool                      `json:"pass"`
	showTimeline       bool
}

func (r *runner) report(measured time.Duration) Report {
	var rp Report
	rp.Config.Rate, rp.Config.Duration, rp.Config.Warmup = r.c.Rate, r.c.Duration.String(), r.c.Warmup.String()
	rp.Config.Subscribers, rp.Config.Conns, rp.Config.RequestSize = r.c.Subscribers, r.c.Conns, r.c.RequestSize
	rp.Config.Updates, rp.Config.UsedPercent = r.c.Updates, r.c.UsedPercent
	rp.MeasuredSeconds = measured.Seconds()
	m := &r.mt
	rp.Sessions = map[string]uint64{
		"started": m.started.Load(), "completed": m.completed.Load(), "denied": m.denied.Load(),
		"failed": m.failed.Load(), "dropped": m.dropped.Load(), "timeouts": m.timeouts.Load(),
	}
	rp.Requests = m.requests.Load()
	if s := measured.Seconds(); s > 0 {
		rp.AchievedSessionsPS = float64(m.completed.Load()+m.denied.Load()) / s
		rp.AchievedRequestsPS = float64(m.requests.Load()) / s
	}
	m.mu.Lock()
	rp.Statuses = map[string]uint64{}
	for k, v := range m.statuses {
		rp.Statuses[k] = v
	}
	m.mu.Unlock()
	rp.ClientSent = r.ct.requests.Load()
	rp.RefCollisions = r.collisions.Load()
	for i := range r.tl {
		s := &r.tl[i]
		if s.started.Load() > 0 {
			rp.Timeline = append(rp.Timeline, TimelineSecond{i, s.started.Load(), s.ok.Load(), s.failed.Load(), s.maxCreateUs.Load()})
		}
	}
	rp.Latency = map[string]LatencySummary{
		"create": summarize(&r.lat.create), "createFromDue": summarize(&r.lat.createCorrected),
		"update": summarize(&r.lat.update), "release": summarize(&r.lat.release),
	}
	return rp
}

// judge applies the service-level objectives that were asked for.
func (rp *Report) judge(c config) {
	if c.SLOP99 > 0 {
		// The tail of the arrival-to-answer time of the create, the strictest view: it includes any queueing.
		if got := time.Duration(rp.Latency["createFromDue"].P99) * time.Microsecond; got > c.SLOP99 {
			rp.SLO = append(rp.SLO, fmt.Sprintf("create P99 from its due time is %v, above the objective %v", got, c.SLOP99))
		}
	}
	if c.SLOOK > 0 {
		total := rp.Sessions["started"] + rp.Sessions["dropped"]
		done := rp.Sessions["completed"] + rp.Sessions["denied"]
		if total == 0 || float64(done)/float64(total) < c.SLOOK {
			rp.SLO = append(rp.SLO, fmt.Sprintf("%d of %d sessions finished as expected, below the objective %.4f", done, total, c.SLOOK))
		}
	}
	if v := rp.Verification; v != nil && v.Reconciler {
		if v.Lost > 0 {
			rp.SLO = append(rp.SLO, fmt.Sprintf("%.0f terminates released without being charged could not be recovered by the gateway (no spool, a failed write, or a session that no longer exists)", v.Lost))
		}
		if v.StillPending > 0 {
			rp.SLO = append(rp.SLO, fmt.Sprintf("%.0f terminates released without being charged were still waiting after %s: the reconciler has not caught up", v.StillPending, c.DrainTimeout))
		}
	}
	if rp.RefCollisions > 0 {
		rp.SLO = append(rp.SLO, fmt.Sprintf("%d sessions were given a charging reference another session already held: the generator sent look-alike requests and the run is not valid", rp.RefCollisions))
	}
	rp.Pass = len(rp.SLO) == 0 && (rp.Verification == nil || rp.Verification.OK)
}

func (rp *Report) write(w io.Writer) {
	p := func(f string, a ...any) { fmt.Fprintf(w, f, a...) }
	cfg := rp.Config
	p("\n=== load test result ===\n")
	p("offered      %.0f sessions/s for %s (after %s warm-up), %d subscribers, %d connections, %d updates per session\n",
		cfg.Rate, cfg.Duration, cfg.Warmup, cfg.Subscribers, cfg.Conns, cfg.Updates)
	p("achieved     %.0f sessions/s, %.0f charging requests/s over %.1f s\n", rp.AchievedSessionsPS, rp.AchievedRequestsPS, rp.MeasuredSeconds)
	p("sessions     started %d, completed %d, denied %d, failed %d, dropped (not started) %d, timeouts %d\n",
		rp.Sessions["started"], rp.Sessions["completed"], rp.Sessions["denied"], rp.Sessions["failed"], rp.Sessions["dropped"], rp.Sessions["timeouts"])
	if len(rp.Statuses) > 0 {
		keys := make([]string, 0, len(rp.Statuses))
		for k := range rp.Statuses {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		p("answers      ")
		for _, k := range keys {
			p("%s×%d  ", k, rp.Statuses[k])
		}
		p("\n")
	}
	p("\n%-15s %9s %9s %9s %9s %9s %9s %9s\n", "latency (ms)", "count", "mean", "p50", "p90", "p99", "p99.9", "max")
	for _, k := range []string{"create", "createFromDue", "update", "release"} {
		l := rp.Latency[k]
		ms := func(us int64) float64 { return float64(us) / 1000 }
		p("%-15s %9d %9.2f %9.2f %9.2f %9.2f %9.2f %9.2f\n", k, l.Count, ms(l.Mean), ms(l.P50), ms(l.P90), ms(l.P99), ms(l.P999), ms(l.Max))
	}
	p("(createFromDue counts from the moment the arrival was due, so a backlog shows up; the others from sending)\n")
	if r := rp.Timeline; r != nil && rp.showTimeline {
		p("\nper second (by when each session was due): t, started, finished as expected, failed, slowest create from due (ms)\n")
		for _, s := range r {
			mark := ""
			if s.Failed > 0 {
				mark = "  <-- failures"
			}
			p("  t=%-3d %5d %5d %5d %8.1f%s\n", s.T, s.Started, s.OK, s.Failed, float64(s.MaxCreateUs)/1000, mark)
		}
	}
	if rp.GatewayCounted != nil {
		p("\ncross-check  client sent %d requests; the gateway's counter rose by %.0f\n", rp.ClientSent, *rp.GatewayCounted)
	}
	if v := rp.Verification; v != nil {
		p("\nbalances     checked %d subscribers:", v.Checked)
		for _, k := range []string{verdictExact, verdictWithin, verdictUnsettled, verdictOverspend, verdictCreated, verdictNegative, verdictUnread} {
			if v.Verdicts[k] > 0 {
				p("  %s %d", k, v.Verdicts[k])
			}
		}
		p("\n")
		if v.Overflowed > 0 {
			p("             %d subscribers spent their whole bundle: usage beyond it is charged to other buckets, which are not checked\n", v.Overflowed)
		}
		if v.UnsettledTerminates > 0 {
			if v.Reconciler {
				p("             the gateway released %.0f terminates without charging them (data tier down): %.0f saved, %.0f charged afterwards by its reconciler, %.0f lost, %.0f still waiting\n",
					v.UnsettledTerminates, v.Recorded, v.Settled, v.Lost, v.StillPending)
			} else {
				p("             the gateway counted %.0f terminates acknowledged without settling (data tier down): up to %d bytes may be uncharged or held; deviations explained by them: %d bytes\n",
					v.UnsettledTerminates, v.Budget, v.Explained)
			}
		}
		for _, s := range v.Violations {
			p("  VIOLATION  %s\n", s)
		}
		for k, tr := range v.Trails {
			p("  history of %s:\n", k)
			for _, l := range tr {
				p("    %s\n", l)
			}
		}
	}
	for _, s := range rp.SLO {
		p("  OBJECTIVE MISSED  %s\n", s)
	}
	if rp.Pass {
		p("\nRESULT: pass\n")
	} else {
		p("\nRESULT: FAIL\n")
	}
}

func (rp *Report) writeJSON(w io.Writer) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(rp)
}
