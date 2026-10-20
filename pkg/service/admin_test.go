package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Handlers registered after the admin server is built (which is when a service's
// start function runs) must still be served.
func TestAdminHandlersRegisteredLateAreServed(t *testing.T) {
	base := http.NewServeMux()
	base.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("healthy")) })
	h := adminHandler(base) // the server exists first

	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	if w := get("/late-metrics"); w.Code != http.StatusNotFound {
		t.Fatalf("before registration: %d", w.Code)
	}
	HandleAdmin("/late-metrics", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }))
	if w := get("/late-metrics"); w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("after registration: %d %q", w.Code, w.Body.String())
	}
	if w := get("/healthz"); w.Body.String() != "healthy" {
		t.Fatal("the base endpoints must keep working")
	}
}
