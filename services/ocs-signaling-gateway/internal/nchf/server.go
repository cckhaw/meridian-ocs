package nchf

import (
	"crypto/tls"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
)

// MaxConcurrentStreams bounds HTTP/2 streams per connection; the admission
// controller bounds them globally.
const MaxConcurrentStreams = 1000

// NewServer returns an http.Server for the handler.
//
// Without tlsCfg it serves cleartext HTTP/2 with prior knowledge (h2c), which
// is how SBA traffic usually runs behind a service-mesh sidecar, and also
// plain HTTP/1.1 for tools such as curl. With tlsCfg, call ServeTLS(ln, "", "")
// and HTTP/2 is negotiated through ALPN.
func NewServer(addr string, h http.Handler, tlsCfg *tls.Config) *http.Server {
	h2s := &http2.Server{MaxConcurrentStreams: MaxConcurrentStreams, IdleTimeout: 5 * time.Minute}
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       5 * time.Minute,
	}
	if tlsCfg != nil {
		srv.Handler = h
		srv.TLSConfig = tlsCfg
		if err := http2.ConfigureServer(srv, h2s); err != nil {
			panic(err) // only fails on an invalid h2s, which is a constant here
		}
	} else {
		srv.Handler = h2c.NewHandler(h, h2s)
	}
	return srv
}

// TLSConfig loads a server certificate (TLS >= 1.2) and, if a client CA file
// is given, requires and verifies client certificates (mutual TLS). HTTP/2 is
// negotiated through ALPN.
//
// This is transport security only; access tokens are checked separately by the
// handler (internal/oauth).
func TLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	return tlsconf.Server(certFile, keyFile, clientCAFile, "h2")
}
