// Package tlstest generates a throwaway CA with a server and client
// certificate for tests.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// PKI holds the generated material. Files are written to a test temp dir.
type PKI struct {
	Dir                       string
	CAPEM                     []byte
	CAFile, CertFile, KeyFile string
	ca                        *x509.Certificate
	caKey                     *ecdsa.PrivateKey
}

// New creates a CA and a server certificate for 127.0.0.1 / localhost.
func New(tb testing.TB) *PKI {
	tb.Helper()
	dir := tb.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &caKey.PublicKey, caKey)
	if err != nil {
		tb.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	p := &PKI{Dir: dir, ca: ca, caKey: caKey, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
	p.CAFile = p.write(tb, "ca.crt", p.CAPEM)
	certPEM, keyPEM := p.issue(tb, "server", x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	p.CertFile, p.KeyFile = p.write(tb, "server.crt", certPEM), p.write(tb, "server.key", keyPEM)
	return p
}

func (p *PKI) write(tb testing.TB, name string, b []byte) string {
	f := filepath.Join(p.Dir, name)
	if err := os.WriteFile(f, b, 0o600); err != nil {
		tb.Fatal(err)
	}
	return f
}

func (p *PKI) issue(tb testing.TB, cn string, usage x509.ExtKeyUsage, ips []net.IP) (certPEM, keyPEM []byte) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: ips}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.ca, &k.PublicKey, p.caKey)
	if err != nil {
		tb.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(k)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})
}

// ClientTLS returns a client configuration trusting the CA. With a name it
// presents a client certificate with that common name.
func (p *PKI) ClientTLS(tb testing.TB, commonName string) *tls.Config {
	tb.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(p.CAPEM)
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if commonName != "" {
		c, k := p.issue(tb, commonName, x509.ExtKeyUsageClientAuth, nil)
		cert, err := tls.X509KeyPair(c, k)
		if err != nil {
			tb.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}
