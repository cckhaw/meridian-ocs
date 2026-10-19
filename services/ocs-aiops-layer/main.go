// Command ocs-aiops-layer is the mapping workbench for AI Ops agents and
// operators: it validates declarative mapping documents (pkg/mapping) and tries
// them on sample records before they are deployed. It changes no state.
//
// The other halves of the AI Ops layer live inside the services they act on:
// every process exposes /metrics (pkg/telemetry) and, if configured, a guarded
// gRPC control plane (pkg/control). See docs/architecture/11-aiops.md.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/service"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
)

func run(ctx context.Context, ready func()) error {
	cfg, err := LoadConfig(svcenv.OS)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: New(cfg.Auth == "off"), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
	if cfg.Auth == "mtls" {
		if srv.TLSConfig, err = tlsconf.Server(cfg.TLSCert, cfg.TLSKey, cfg.ClientCA, "h2", "http/1.1"); err != nil {
			return err
		}
	} else {
		log.Printf("ocs-aiops-layer: WARNING authentication is OFF")
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	log.Printf("ocs-aiops-layer: mapping workbench on %s", ln.Addr())
	errc := make(chan error, 1)
	go func() {
		var e error
		if cfg.Auth == "mtls" {
			e = srv.ServeTLS(ln, "", "")
		} else {
			e = srv.Serve(ln)
		}
		if !errors.Is(e, http.ErrServerClosed) {
			errc <- e
		}
		close(errc)
	}()
	ready()
	select {
	case err = <-errc:
	case <-ctx.Done():
	}
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(c)
	return err
}

func main() { service.RunWith("ocs-aiops-layer", run) }
