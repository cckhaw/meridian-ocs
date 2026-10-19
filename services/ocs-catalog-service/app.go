package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/services/ocs-catalog-service/internal/api"
)

// App is the running service.
type App struct {
	cfg  Config
	logf func(string, ...any)
	rdb  *redis.Client
	rep  *catalog.Replicator
	srv  *http.Server
	ln   net.Listener
}

// NewApp connects to Redis, loads the catalog and binds the listener. It
// refuses to start if the catalog cannot be read: serving an empty or stale
// management view while a publish may be in flight would mislead editors.
func NewApp(ctx context.Context, cfg Config, logf func(string, ...any)) (*App, error) {
	a := &App{cfg: cfg, logf: logf}
	a.rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	store := catalog.NewStore(cfg.Retain)
	a.rep = &catalog.Replicator{Redis: a.rdb, Store: store, Resync: cfg.Resync,
		OnError: func(err error) { logf("catalog sync: %v", err) }}
	if err := a.rep.Sync(ctx); err != nil {
		a.rdb.Close()
		return nil, fmt.Errorf("loading the catalog from %s: %w", cfg.RedisAddr, err)
	}

	h := api.New(store, a.rep, cfg.Auth == "off", cfg.Publishers)
	a.srv = &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
	if cfg.Auth == "mtls" {
		var err error
		if a.srv.TLSConfig, err = tlsconf.Server(cfg.TLSCert, cfg.TLSKey, cfg.ClientCA, "h2", "http/1.1"); err != nil {
			a.rdb.Close()
			return nil, err
		}
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		a.rdb.Close()
		return nil, err
	}
	a.ln = ln
	if v := store.Current(); v != nil {
		logf("catalog version %d in effect (latest %d)", v.Version, store.LatestVersion())
	} else {
		logf("no catalog published yet (latest version %d)", store.LatestVersion())
	}
	if cfg.Auth == "off" {
		logf("WARNING: authentication is off, anyone who can reach %s can publish", ln.Addr())
	} else {
		logf("REST on %s (mTLS); publishers: %v", ln.Addr(), cfg.Publishers)
	}
	return a, nil
}

// Addr is the bound address.
func (a *App) Addr() string { return a.ln.Addr().String() }

// Run serves until ctx ends, then shuts down.
func (a *App) Run(ctx context.Context) error {
	repDone := make(chan struct{})
	go func() { defer close(repDone); _ = a.rep.Run(ctx) }()

	errc := make(chan error, 1)
	go func() {
		var err error
		if a.cfg.Auth == "mtls" {
			err = a.srv.ServeTLS(a.ln, "", "")
		} else {
			err = a.srv.Serve(a.ln)
		}
		if !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()
	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = a.srv.Shutdown(c)
	<-repDone
	return err
}

// Close releases the Redis connection.
func (a *App) Close() { a.rdb.Close() }
