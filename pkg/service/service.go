// Package service provides the common process lifecycle shared by every OCS
// microservice: signal handling plus liveness/readiness endpoints.
package service

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	adminMu    sync.Mutex
	adminExtra = map[string]http.Handler{}
)

// HandleAdmin adds a handler (for example /metrics) to the admin server at an
// exact path. It may be called at any time, including from the start function
// RunWith runs, because the admin server looks handlers up per request. The
// admin port serves operations tooling and must not be exposed outside the
// cluster network.
func HandleAdmin(pattern string, h http.Handler) {
	adminMu.Lock()
	defer adminMu.Unlock()
	adminExtra[pattern] = h
}

// adminHandler serves registered extra handlers first, then the base mux.
func adminHandler(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		adminMu.Lock()
		h := adminExtra[r.URL.Path]
		adminMu.Unlock()
		if h != nil {
			h.ServeHTTP(w, r)
			return
		}
		base.ServeHTTP(w, r)
	})
}

// Run starts the admin HTTP server and blocks until SIGINT/SIGTERM. It is for
// services that have no main loop yet.
func Run(name string) {
	RunWith(name, func(ctx context.Context, ready func()) error {
		ready()
		<-ctx.Done()
		return nil
	})
}

// RunWith starts the admin HTTP server (/healthz, /readyz), then calls start
// with a context that is cancelled on SIGINT/SIGTERM. /readyz stays 503 until
// start calls ready(). If start returns an error the process exits non-zero;
// otherwise RunWith shuts the admin server down gracefully and returns.
// The admin address is taken from OCS_ADMIN_ADDR (default ":8080").
func RunWith(name string, start func(ctx context.Context, ready func()) error) {
	addr := os.Getenv("OCS_ADMIN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: adminHandler(mux), ReadHeaderTimeout: 5 * time.Second}
	adminErr := make(chan error, 1)
	go func() { adminErr <- srv.ListenAndServe() }()
	log.Printf("%s: admin server listening on %s", name, addr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startErr := make(chan error, 1)
	go func() { startErr <- start(ctx, func() { ready.Store(true) }) }()

	select {
	case err := <-adminErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s: admin server: %v", name, err)
		}
	case err := <-startErr:
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
	case <-ctx.Done():
		stop()
		<-startErr // let the service drain
	}

	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("%s: shutdown: %v", name, err)
	}
}
