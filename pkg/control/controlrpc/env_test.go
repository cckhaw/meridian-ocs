package controlrpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cckhaw/meridian-ocs/pkg/control"
	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestServeFromEnvRefusals(t *testing.T) {
	ctl := control.New(control.Config{Service: "x"})
	logf := func(string, ...any) {}
	pki := tlstest.New(t)
	tls := map[string]string{"OCS_CONTROL_ADDR": "127.0.0.1:0", "OCS_CONTROL_AUTH": "mtls",
		"OCS_CONTROL_TLS_CERT": pki.CertFile, "OCS_CONTROL_TLS_KEY": pki.KeyFile, "OCS_CONTROL_CLIENT_CA": pki.CAFile}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range tls {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	for name, c := range map[string]struct {
		env  map[string]string
		want string
	}{
		"auth unset":          {map[string]string{"OCS_CONTROL_ADDR": "127.0.0.1:0"}, `must be set to "mtls" or "off"`},
		"auth typo":           {map[string]string{"OCS_CONTROL_ADDR": "127.0.0.1:0", "OCS_CONTROL_AUTH": "tls"}, `must be set to "mtls" or "off"`},
		"mtls no certs":       {map[string]string{"OCS_CONTROL_ADDR": "127.0.0.1:0", "OCS_CONTROL_AUTH": "mtls", "OCS_CONTROL_AGENTS": "a"}, "OCS_CONTROL_TLS_CERT"},
		"mtls nobody allowed": {with(nil), "nobody would be allowed in"},
		"off with lists":      {map[string]string{"OCS_CONTROL_ADDR": "127.0.0.1:0", "OCS_CONTROL_AUTH": "off", "OCS_CONTROL_AGENTS": "a"}, "looks restricted but is not"},
	} {
		if _, _, err := ServeFromEnv(envOf(c.env), ctl, logf); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
	// Unset address: no control plane, no error, nothing listening.
	if addr, stop, err := ServeFromEnv(envOf(nil), ctl, logf); err != nil || addr != "" {
		t.Fatalf("%q %v", addr, err)
	} else {
		stop()
	}

	addr, stop, err := ServeFromEnv(envOf(with(map[string]string{"OCS_CONTROL_AGENTS": "agent"})), ctl, logf)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(credentials.NewTLS(pki.ClientTLS(t, "agent"))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := ocsv1.NewControlServiceClient(conn).ListParameters(ctx, &ocsv1.ListParametersRequest{Service: "x"}); err != nil {
		t.Fatal(err)
	}
}
