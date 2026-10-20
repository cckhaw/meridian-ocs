package controlrpc

import (
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cckhaw/meridian-ocs/pkg/control"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
)

// ServeFromEnv starts the control listener a process is configured for:
//
//	OCS_CONTROL_ADDR         listen address; unset = no control plane at all
//	OCS_CONTROL_AUTH         required with the address: "mtls" or "off"
//	OCS_CONTROL_TLS_CERT, OCS_CONTROL_TLS_KEY, OCS_CONTROL_CLIENT_CA   (mtls)
//	OCS_CONTROL_AGENTS       client certificate common names that may propose changes (mtls)
//	OCS_CONTROL_APPROVERS    common names that may also approve risky changes and freeze
//
// The control plane is off unless asked for, and there is no default for the
// authentication mode: opening a tuning channel to a charging path has to be a
// deliberate decision. It returns the bound address ("" if not started) and a
// function that stops the listener.
func ServeFromEnv(g svcenv.Getenv, ctl *control.Controller, logf func(string, ...any)) (addr string, stop func(), err error) {
	listen := g("OCS_CONTROL_ADDR")
	if listen == "" {
		return "", func() {}, nil
	}
	var opts []grpc.ServerOption
	agents, approvers := g.List("OCS_CONTROL_AGENTS"), g.List("OCS_CONTROL_APPROVERS")
	open := false
	switch mode := g("OCS_CONTROL_AUTH"); mode {
	case "mtls":
		cert, key, ca := g("OCS_CONTROL_TLS_CERT"), g("OCS_CONTROL_TLS_KEY"), g("OCS_CONTROL_CLIENT_CA")
		if cert == "" || key == "" || ca == "" {
			return "", nil, errors.New("OCS_CONTROL_AUTH=mtls needs OCS_CONTROL_TLS_CERT, OCS_CONTROL_TLS_KEY and OCS_CONTROL_CLIENT_CA")
		}
		if len(agents)+len(approvers) == 0 {
			return "", nil, errors.New("OCS_CONTROL_AUTH=mtls needs OCS_CONTROL_AGENTS and/or OCS_CONTROL_APPROVERS: nobody would be allowed in")
		}
		tc, err := tlsconf.Server(cert, key, ca, "h2")
		if err != nil {
			return "", nil, fmt.Errorf("control TLS: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tc)))
		if len(approvers) == 0 {
			logf("control: no OCS_CONTROL_APPROVERS: risky parameters cannot be changed")
		}
	case "off":
		if len(agents)+len(approvers) > 0 {
			return "", nil, errors.New("OCS_CONTROL_AGENTS / OCS_CONTROL_APPROVERS have no effect with OCS_CONTROL_AUTH=off: refusing a configuration that looks restricted but is not")
		}
		open = true
		logf("control: WARNING authentication is OFF: anyone who can reach %s can tune this process", listen)
	default:
		return "", nil, fmt.Errorf(`OCS_CONTROL_AUTH must be set to "mtls" or "off" when OCS_CONTROL_ADDR is set (got %q)`, mode)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return "", nil, err
	}
	gs := grpc.NewServer(opts...)
	New(ctl, open, agents, approvers).Register(gs)
	go func() { _ = gs.Serve(ln) }()
	logf("control plane listening on %s (%d parameters)", ln.Addr(), len(ctl.List()))
	return ln.Addr().String(), func() {
		done := make(chan struct{})
		go func() { gs.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			gs.Stop()
		}
	}, nil
}
