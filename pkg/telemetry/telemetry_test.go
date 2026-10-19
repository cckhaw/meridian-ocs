package telemetry

import (
	"bytes"
	"log/slog"
	"math"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b bytes.Buffer
	if _, err := r.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	var reqs atomic.Uint64
	reqs.Store(42)
	r.CounterFunc("ocs_requests_total", "Requests handled.", Labels{"protocol": "gy"}, func() float64 { return float64(reqs.Load()) })
	r.CounterFunc("ocs_requests_total", "Requests handled.", Labels{"protocol": "nchf"}, func() float64 { return 7 })
	r.GaugeFunc("ocs_inflight", "In flight.\nSecond line \\ backslash.", nil, func() float64 { return 3.5 })
	r.GaugeFunc("ocs_weird", "", Labels{"v": "a\"b\\c\nd"}, func() float64 { return math.Inf(1) })

	want := `# HELP ocs_inflight In flight.\nSecond line \\ backslash.
# TYPE ocs_inflight gauge
ocs_inflight 3.5
# HELP ocs_requests_total Requests handled.
# TYPE ocs_requests_total counter
ocs_requests_total{protocol="gy"} 42
ocs_requests_total{protocol="nchf"} 7
# TYPE ocs_weird gauge
ocs_weird{v="a\"b\\c\nd"} +Inf
`
	if got := render(t, r); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	reqs.Store(43)
	if !strings.Contains(render(t, r), `ocs_requests_total{protocol="gy"} 43`) {
		t.Fatal("a counter must be read at scrape time")
	}
}

func TestHistogramExposition(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("ocs_latency_seconds", "Latency.", Labels{"op": "rate"}, 0.001, 0.01)
	for _, d := range []time.Duration{500 * time.Microsecond, time.Millisecond, 5 * time.Millisecond, 50 * time.Millisecond} {
		h.Observe(d)
	}
	h.Observe(-time.Second) // clamped to zero
	want := `# HELP ocs_latency_seconds Latency.
# TYPE ocs_latency_seconds histogram
ocs_latency_seconds_bucket{op="rate",le="0.001"} 3
ocs_latency_seconds_bucket{op="rate",le="0.01"} 4
ocs_latency_seconds_bucket{op="rate",le="+Inf"} 5
ocs_latency_seconds_sum{op="rate"} 0.0565
ocs_latency_seconds_count{op="rate"} 5
`
	if got := render(t, r); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestQuantile(t *testing.T) {
	h := NewHistogram(0.001, 0.01, 0.1)
	if !math.IsNaN(h.Quantile(0.5)) {
		t.Fatal("no observations")
	}
	for i := 0; i < 90; i++ {
		h.Observe(500 * time.Microsecond) // bucket 0
	}
	for i := 0; i < 10; i++ {
		h.Observe(5 * time.Millisecond) // bucket 1
	}
	if p50 := h.Quantile(0.5); p50 <= 0 || p50 > 0.001 {
		t.Fatalf("p50 %v", p50)
	}
	if p99 := h.Quantile(0.99); p99 < 0.001 || p99 > 0.01 {
		t.Fatalf("p99 %v", p99)
	}
	h.Observe(time.Hour)
	if q := h.Quantile(1); q != 0.1 {
		t.Fatalf("beyond the last bound reports that bound, got %v", q)
	}
}

func TestRegistrationErrors(t *testing.T) {
	r := NewRegistry()
	r.GaugeFunc("a", "", nil, func() float64 { return 0 })
	for name, f := range map[string]func(){
		"bad name":   func() { r.GaugeFunc("1bad", "", nil, func() float64 { return 0 }) },
		"bad label":  func() { r.GaugeFunc("b", "", Labels{"x-y": "1"}, func() float64 { return 0 }) },
		"reserved":   func() { r.GaugeFunc("b", "", Labels{"__x": "1"}, func() float64 { return 0 }) },
		"le on hist": func() { r.Histogram("h", "", Labels{"le": "1"}) },
		"duplicate":  func() { r.GaugeFunc("a", "", nil, func() float64 { return 0 }) },
		"kind clash": func() { r.CounterFunc("a", "", Labels{"x": "1"}, func() float64 { return 0 }) },
		"bad bounds": func() { NewHistogram(0.1, 0.05) },
		"zero bound": func() { NewHistogram(0) },
		"nan bound":  func() { NewHistogram(math.NaN()) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: accepted", name)
				}
			}()
			f()
		}()
	}
}

func TestHandlerAndConcurrency(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("lat", "", nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				h.Observe(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	for i := 0; i < 20; i++ { // scrape while observing
		w := httptest.NewRecorder()
		r.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
			t.Fatalf("%d %q", w.Code, w.Header().Get("Content-Type"))
		}
	}
	wg.Wait()
	if h.Count() != 16000 {
		t.Fatalf("%d", h.Count())
	}
	// In one scrape the +Inf bucket always equals _count.
	out := render(t, r)
	inf, cnt := "", ""
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, `lat_bucket{le="+Inf"}`) {
			inf = strings.Fields(l)[1]
		}
		if strings.HasPrefix(l, "lat_count") {
			cnt = strings.Fields(l)[1]
		}
	}
	if inf == "" || inf != cnt {
		t.Fatalf("+Inf %q count %q", inf, cnt)
	}
}

func TestObserveDoesNotAllocate(t *testing.T) {
	h := NewHistogram()
	if n := testing.AllocsPerRun(1000, func() { h.Observe(3 * time.Millisecond) }); n != 0 {
		t.Fatalf("%v allocations", n)
	}
}

func TestLoggerLevelIsAdjustable(t *testing.T) {
	var b bytes.Buffer
	lg, lv := NewLogger(&b, "ocs-test", slog.LevelInfo)
	lg.Debug("hidden")
	lg.Info("shown", "k", 1)
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), `"service":"ocs-test"`) || !strings.Contains(b.String(), `"msg":"shown"`) {
		t.Fatal(b.String())
	}
	lvl, ok := ParseLevel("DEBUG")
	if !ok {
		t.Fatal("parse")
	}
	lv.Set(lvl)
	lg.Debug("now visible")
	if !strings.Contains(b.String(), "now visible") {
		t.Fatal("level change had no effect")
	}
	if _, ok := ParseLevel("loud"); ok {
		t.Fatal("bad level accepted")
	}
}

func BenchmarkObserve(b *testing.B) {
	h := NewHistogram()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(3 * time.Millisecond)
		}
	})
}
