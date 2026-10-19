package nchf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
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

// TLSConfig loads a server certificate and, when clientCAFile is set, requires
// and verifies client certificates (mutual TLS, TS 33.501 §13). TLS 1.2 is the
// minimum.
//
// This is transport security only. OAuth2 access token validation (the
// "nchf-convergedcharging" scope) is not implemented.
func TLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}
	if clientCAFile != "" {
		pem, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("nchf: no certificates found in client CA file")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
