package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/grpcapi"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/rest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/worker"
)

// App is the assembled service.
type App struct {
	cfg  Config
	logf func(string, ...any)

	rdb  *redis.Client
	prod *bus.KafkaProducer
	cons *kgo.Client
	wrk  *worker.Worker

	restSrv *http.Server
	grpcSrv *grpc.Server
	restLn  net.Listener
	grpcLn  net.Listener
}

// RESTAddr and GRPCAddr return the bound listener addresses ("" if not serving).
func (a *App) RESTAddr() string { return addrOf(a.restLn) }
func (a *App) GRPCAddr() string { return addrOf(a.grpcLn) }

func addrOf(l net.Listener) string {
	if l == nil {
		return ""
	}
	return l.Addr().String()
}

// NewApp connects the dependencies and binds the listeners. Close with Shutdown.
func NewApp(ctx context.Context, cfg Config, g svcenv.Getenv, logf func(string, ...any)) (*App, error) {
	a := &App{cfg: cfg, logf: logf}
	ok := false
	defer func() {
		if !ok {
			a.Close()
		}
	}()

	a.rdb = redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     64,
	})
	data := datalayer.New(a.rdb)
	if err := svcenv.ConfigureDurability(ctx, g, data, logf); err != nil {
		return nil, err
	}
	st := status.New(a.rdb, "", 0)

	var err error
	if a.prod, err = bus.NewProducer(cfg.Bus.Brokers); err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	if cfg.runsWorker() {
		if a.cons, err = bus.NewConsumer(cfg.Bus); err != nil {
			return nil, fmt.Errorf("kafka consumer: %w", err)
		}
		applier := &worker.DataApplier{Data: data, Dir: identity.NewRedisDirectory(a.rdb, cfg.IdentityPrefix)}
		a.wrk = worker.New(worker.Config{Bus: cfg.Bus, Limits: cfg.Limits, Workers: cfg.Workers, Logf: logf}, a.cons, applier, st, a.prod)
	}

	if cfg.runsAPI() {
		az := auth.Off()
		if cfg.Auth == "mtls" {
			az = auth.New(cfg.Standard, cfg.Privileged)
		}
		logf("API access: %s", az.Describe())
		svc := service.New(service.Config{Limits: cfg.Limits, CommandsTopic: cfg.Bus.CommandsTopic}, st, a.prod, data)

		if cfg.RESTAddr != "off" {
			a.restSrv = &http.Server{Handler: rest.New(svc, az), ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
			if cfg.Auth == "mtls" {
				if a.restSrv.TLSConfig, err = tlsconf.Server(cfg.TLSCert, cfg.TLSKey, cfg.ClientCA, "h2", "http/1.1"); err != nil {
					return nil, fmt.Errorf("REST TLS: %w", err)
				}
			}
			if a.restLn, err = net.Listen("tcp", cfg.RESTAddr); err != nil {
				return nil, err
			}
		}
		if cfg.GRPCAddr != "off" {
			var opts []grpc.ServerOption
			if cfg.Auth == "mtls" {
				tc, err := tlsconf.Server(cfg.TLSCert, cfg.TLSKey, cfg.ClientCA, "h2")
				if err != nil {
					return nil, fmt.Errorf("gRPC TLS: %w", err)
				}
				opts = append(opts, grpc.Creds(credentials.NewTLS(tc)))
			}
			a.grpcSrv = grpc.NewServer(opts...)
			grpcapi.New(svc, az).Register(a.grpcSrv)
			if a.grpcLn, err = net.Listen("tcp", cfg.GRPCAddr); err != nil {
				return nil, err
			}
		}
		if cfg.Auth == "off" {
			logf("WARNING authentication is OFF: anyone who can reach the API can move money")
		}
	}
	ok = true
	return a, nil
}

// Run serves until ctx ends or a component fails, then shuts down.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 3)
	running := 0
	start := func(f func() error) { running++; go func() { errc <- f() }() }

	if a.wrk != nil {
		a.logf("command worker running (group %s, topic %s)", a.cfg.Bus.Group, a.cfg.Bus.CommandsTopic)
		start(func() error { return a.wrk.Run(ctx) })
	}
	if a.restSrv != nil {
		a.logf("REST listening on %s", a.RESTAddr())
		start(func() error {
			var err error
			if a.restSrv.TLSConfig != nil {
				err = a.restSrv.ServeTLS(a.restLn, "", "")
			} else {
				err = a.restSrv.Serve(a.restLn)
			}
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	if a.grpcSrv != nil {
		a.logf("gRPC listening on %s", a.GRPCAddr())
		start(func() error { return a.grpcSrv.Serve(a.grpcLn) })
	}
	if running == 0 {
		return errors.New("nothing to run: the mode and listeners leave no component enabled")
	}

	var first error
	select {
	case first = <-errc:
		running--
	case <-ctx.Done():
	}
	a.shutdown(cancel)
	for ; running > 0; running-- {
		if err := <-errc; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (a *App) shutdown(cancelWorker context.CancelFunc) {
	cancelWorker()
	if a.restSrv != nil {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = a.restSrv.Shutdown(c)
		cancel()
	}
	if a.grpcSrv != nil {
		done := make(chan struct{})
		go func() { a.grpcSrv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			a.grpcSrv.Stop()
		}
	}
}

// Close releases the connections. It is safe to call more than once.
func (a *App) Close() {
	if a.cons != nil {
		a.cons.Close()
		a.cons = nil
	}
	if a.prod != nil {
		a.prod.Close()
		a.prod = nil
	}
	if a.rdb != nil {
		_ = a.rdb.Close()
		a.rdb = nil
	}
	for _, l := range []net.Listener{a.restLn, a.grpcLn} {
		if l != nil {
			_ = l.Close()
		}
	}
}
