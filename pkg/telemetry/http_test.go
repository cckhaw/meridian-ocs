package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstrumentCountsByClassAndSkipsOtherPaths(t *testing.T) {
	reg := NewRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/ok", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) })
	mux.HandleFunc("/v1/created", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(201) })
	mux.HandleFunc("/v1/bad", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", 400) })
	mux.HandleFunc("/v1/boom", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", 503) })
	mux.HandleFunc("/v1/moved", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/v1/ok", 302) })
	mux.HandleFunc("/ui/app.js", func(w http.ResponseWriter, _ *http.Request) {})
	h := Instrument(mux, reg, "t", "/v1/")
	for _, p := range []string{"/v1/ok", "/v1/ok", "/v1/created", "/v1/bad", "/v1/boom", "/v1/moved", "/ui/app.js", "/nothing"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", p, nil))
	}
	var sb strings.Builder
	_, _ = reg.WriteTo(&sb)
	out := sb.String()
	for _, want := range []string{
		`t_http_requests_total{code="2xx"} 3`, `t_http_requests_total{code="3xx"} 1`,
		`t_http_requests_total{code="4xx"} 1`, `t_http_requests_total{code="5xx"} 1`,
		`t_http_request_duration_seconds_count 6`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "/v1/") {
		t.Error("a path leaked into a label")
	}
}
