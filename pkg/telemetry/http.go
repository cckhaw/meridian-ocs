package telemetry

import (
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	code  int
	wrote bool
}

func (w *statusWriter) WriteHeader(c int) {
	if !w.wrote {
		w.code, w.wrote = c, true
	}
	w.ResponseWriter.WriteHeader(c)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.code, w.wrote = http.StatusOK, true
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// HTTPDurationBuckets suit an API whose requests are mostly one Redis round
// trip and one Kafka produce.
var HTTPDurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// Instrument counts the requests whose path starts with prefix by status class
// (2xx, 3xx, 4xx, 5xx) and times them, as <name>_http_requests_total{code} and
// <name>_http_request_duration_seconds. Other paths (the console's static files,
// the root redirect) are passed through uncounted so they cannot dilute the
// API's latency. The path is never a label: it can carry subscriber keys.
func Instrument(next http.Handler, reg *Registry, name, prefix string) http.Handler {
	var classes [4]atomic.Uint64
	for i, c := range []string{"2xx", "3xx", "4xx", "5xx"} {
		i := i
		reg.CounterFunc(name+"_http_requests_total", "API requests by status class.", Labels{"code": c},
			func() float64 { return float64(classes[i].Load()) })
	}
	hist := reg.Histogram(name+"_http_request_duration_seconds", "API request duration.", nil, HTTPDurationBuckets...)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, prefix) {
			next.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		hist.Observe(time.Since(start))
		switch c := sw.code; {
		case c >= 500:
			classes[3].Add(1)
		case c >= 400:
			classes[2].Add(1)
		case c >= 300:
			classes[1].Add(1)
		default:
			classes[0].Add(1)
		}
	})
}
