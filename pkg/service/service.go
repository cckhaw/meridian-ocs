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
	"sync/atomic"
	"syscall"
	"time"
)

// Run starts the admin HTTP server (/healthz, /readyz) and blocks until
// SIGINT/SIGTERM, then shuts down gracefully. The admin address is taken from
// OCS_ADMIN_ADDR (default ":8080").
func Run(name string) {
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

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	ready.Store(true)
	log.Printf("%s: admin server listening on %s", name, addr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s: admin server: %v", name, err)
		}
	case <-ctx.Done():
	}

	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("%s: shutdown: %v", name, err)
	}
}
