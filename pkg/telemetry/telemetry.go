// Package telemetry exports runtime metrics in the Prometheus text format and
// builds the structured JSON logger every service uses.
//
// It is deliberately small: counters and gauges are read from the atomics the
// services already keep (nothing is copied on the hot path), and the only
// instrument that costs anything per request is Histogram, which is a handful
// of atomic adds. It does not depend on the OpenTelemetry SDK; the exposition
// is plain Prometheus text, which an OpenTelemetry Collector can scrape and
// forward as OTLP. See docs/architecture/11-aiops.md for what that leaves out.
package telemetry

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	nameRe  = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Labels are a metric's constant labels.
type Labels map[string]string

type kind uint8

const (
	kCounter kind = iota
	kGauge
	kHistogram
)

func (k kind) String() string { return [...]string{"counter", "gauge", "histogram"}[k] }

type series struct {
	labels Labels
	fn     func() float64 // counters and gauges
	hist   *Histogram
}

type family struct {
	name   string
	help   string
	kind   kind
	series []series
}

// Registry holds metric families. It is safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

func (r *Registry) add(name, help string, k kind, labels Labels, s series) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("telemetry: invalid metric name %q", name)
	}
	for l := range labels {
		if !labelRe.MatchString(l) || strings.HasPrefix(l, "__") || (k == kHistogram && l == "le") {
			return fmt.Errorf("telemetry: invalid label %q on %s", l, name)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.families[name]
	if f == nil {
		f = &family{name: name, help: help, kind: k}
		r.families[name] = f
	} else if f.kind != k {
		return fmt.Errorf("telemetry: %s is already registered as a %s", name, f.kind)
	}
	key := labelKey(labels)
	for _, ex := range f.series {
		if labelKey(ex.labels) == key {
			return fmt.Errorf("telemetry: duplicate series %s{%s}", name, key)
		}
	}
	s.labels = labels
	f.series = append(f.series, s)
	return nil
}

// CounterFunc registers a counter read from fn at scrape time. fn must only
// increase. It panics on an invalid or duplicate registration, which is a
// programming error found at start-up.
func (r *Registry) CounterFunc(name, help string, labels Labels, fn func() float64) {
	if err := r.add(name, help, kCounter, labels, series{fn: fn}); err != nil {
		panic(err)
	}
}

// GaugeFunc registers a gauge read from fn at scrape time.
func (r *Registry) GaugeFunc(name, help string, labels Labels, fn func() float64) {
	if err := r.add(name, help, kGauge, labels, series{fn: fn}); err != nil {
		panic(err)
	}
}

// Histogram registers (and returns) a latency histogram with the given upper
// bounds in seconds; DefaultBuckets if none.
func (r *Registry) Histogram(name, help string, labels Labels, bounds ...float64) *Histogram {
	h := NewHistogram(bounds...)
	if err := r.add(name, help, kHistogram, labels, series{hist: h}); err != nil {
		panic(err)
	}
	return h
}

// DefaultBuckets suit a charging path with a 10 ms P99 target: fine below
// 10 ms, coarse above.
var DefaultBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 1}

// Histogram is a fixed-bucket latency histogram. Observe is lock-free.
type Histogram struct {
	bounds  []float64
	buckets []atomic.Uint64 // per bucket (not cumulative); the last is +Inf
	sumNs   atomic.Uint64
	count   atomic.Uint64
}

// NewHistogram creates a histogram. Bounds must be increasing and positive.
func NewHistogram(bounds ...float64) *Histogram {
	if len(bounds) == 0 {
		bounds = DefaultBuckets
	}
	for i, b := range bounds {
		if b <= 0 || math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= bounds[i-1]) {
			panic("telemetry: histogram bounds must be positive and increasing")
		}
	}
	return &Histogram{bounds: append([]float64(nil), bounds...), buckets: make([]atomic.Uint64, len(bounds)+1)}
}

// Observe records one duration.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s := d.Seconds()
	i := sort.SearchFloat64s(h.bounds, s) // first bound >= s
	h.buckets[i].Add(1)
	h.sumNs.Add(uint64(d))
	h.count.Add(1)
}

// Count is the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Quantile estimates the q-quantile (0..1) in seconds by linear interpolation
// inside the bucket, as Prometheus' histogram_quantile does. It returns NaN
// with no observations. The result is only as fine as the buckets.
func (h *Histogram) Quantile(q float64) float64 {
	total := h.count.Load()
	if total == 0 || q < 0 || q > 1 {
		return math.NaN()
	}
	rank := q * float64(total)
	var cum float64
	for i := range h.buckets {
		n := float64(h.buckets[i].Load())
		if cum+n >= rank {
			if i == len(h.bounds) { // +Inf bucket: no upper bound to interpolate to
				return h.bounds[len(h.bounds)-1]
			}
			lo := 0.0
			if i > 0 {
				lo = h.bounds[i-1]
			}
			if n == 0 {
				return h.bounds[i]
			}
			return lo + (h.bounds[i]-lo)*(rank-cum)/n
		}
		cum += n
	}
	return h.bounds[len(h.bounds)-1]
}

func labelKey(l Labels) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k + `="` + escape(l[k]) + `"`)
	}
	return sb.String()
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func escapeHelp(s string) string { return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s) }

func fmtFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func withLabels(l Labels, extra string) string {
	k := labelKey(l)
	switch {
	case k == "" && extra == "":
		return ""
	case k == "":
		return "{" + extra + "}"
	case extra == "":
		return "{" + k + "}"
	}
	return "{" + k + "," + extra + "}"
}

// WriteTo writes every family in the Prometheus text exposition format 0.0.4,
// sorted by name so the output is stable.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	fams := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		cp := *f
		cp.series = append([]series(nil), f.series...)
		fams = append(fams, &cp)
	}
	r.mu.Unlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name < fams[j].name })

	var sb strings.Builder
	for _, f := range fams {
		if f.help != "" {
			fmt.Fprintf(&sb, "# HELP %s %s\n", f.name, escapeHelp(f.help))
		}
		fmt.Fprintf(&sb, "# TYPE %s %s\n", f.name, f.kind)
		sort.Slice(f.series, func(i, j int) bool { return labelKey(f.series[i].labels) < labelKey(f.series[j].labels) })
		for _, s := range f.series {
			if f.kind != kHistogram {
				fmt.Fprintf(&sb, "%s%s %s\n", f.name, withLabels(s.labels, ""), fmtFloat(s.fn()))
				continue
			}
			h := s.hist
			// Read the buckets once so _count equals the +Inf bucket in this scrape.
			counts := make([]uint64, len(h.buckets))
			var total uint64
			for i := range h.buckets {
				counts[i] = h.buckets[i].Load()
				total += counts[i]
			}
			var cum uint64
			for i, b := range h.bounds {
				cum += counts[i]
				fmt.Fprintf(&sb, "%s_bucket%s %d\n", f.name, withLabels(s.labels, `le="`+fmtFloat(b)+`"`), cum)
			}
			fmt.Fprintf(&sb, "%s_bucket%s %d\n", f.name, withLabels(s.labels, `le="+Inf"`), total)
			fmt.Fprintf(&sb, "%s_sum%s %s\n", f.name, withLabels(s.labels, ""), fmtFloat(float64(h.sumNs.Load())/1e9))
			fmt.Fprintf(&sb, "%s_count%s %d\n", f.name, withLabels(s.labels, ""), total)
		}
	}
	n, err := io.WriteString(w, sb.String())
	return int64(n), err
}

// Handler serves the registry on /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

// NewLogger returns a JSON logger that stamps every line with the service
// name. Level is adjustable at runtime through the returned LevelVar (the AI
// Ops control plane drives it). Secrets must never be logged by callers; this
// package does not redact.
func NewLogger(w io.Writer, service string, initial slog.Level) (*slog.Logger, *slog.LevelVar) {
	lv := new(slog.LevelVar)
	lv.Set(initial)
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lv})
	return slog.New(h).With("service", service), lv
}

// ParseLevel parses debug, info, warn or error.
func ParseLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	}
	return 0, false
}
