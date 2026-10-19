package tlsconf_test

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

func TestServerWithAndWithoutClientCertificates(t *testing.T) {
	pki := tlstest.New(t)
	cfg, err := tlsconf.Server(pki.CertFile, pki.KeyFile, pki.CAFile, "h2")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion < tls.VersionTLS12 || cfg.ClientAuth != tls.RequireAndVerifyClientCert || cfg.NextProtos[0] != "h2" {
		t.Fatalf("%+v", cfg)
	}
	noClient, err := tlsconf.Server(pki.CertFile, pki.KeyFile, "")
	if err != nil || noClient.ClientAuth != tls.NoClientCert {
		t.Fatalf("%v", err)
	}

	bad := filepath.Join(t.TempDir(), "bad.pem")
	_ = os.WriteFile(bad, []byte("nope"), 0o600)
	if _, err := tlsconf.Server(pki.CertFile, pki.KeyFile, bad); err == nil {
		t.Error("a CA file without certificates")
	}
	if _, err := tlsconf.Server(pki.CertFile, pki.KeyFile, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing CA file")
	}
	if _, err := tlsconf.Server(filepath.Join(t.TempDir(), "x"), pki.KeyFile, ""); err == nil {
		t.Error("a missing certificate")
	}
}

func TestPeerCommonNameIsTakenFromTheVerifiedChain(t *testing.T) {
	pki := tlstest.New(t)
	cfg, _ := tlsconf.Server(pki.CertFile, pki.KeyFile, pki.CAFile)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			got <- "accept: " + err.Error()
			return
		}
		defer c.Close()
		tc := c.(*tls.Conn)
		if err := tc.Handshake(); err != nil {
			got <- "handshake: " + err.Error()
			return
		}
		st := tc.ConnectionState()
		got <- tlsconf.PeerCommonName(&st)
	}()
	conn, err := tls.Dial("tcp", ln.Addr().String(), pki.ClientTLS(t, "crm-1"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("x"))
	if name := <-got; name != "crm-1" {
		t.Fatalf("%q", name)
	}
	conn.Close()

	if tlsconf.PeerCommonName(nil) != "" || tlsconf.PeerCommonName(&tls.ConnectionState{}) != "" {
		t.Fatal("no verified client certificate means no name")
	}
	_ = net.IPv4zero
}
