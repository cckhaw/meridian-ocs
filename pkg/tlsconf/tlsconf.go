// Package tlsconf builds server TLS configurations for the OCS APIs.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// Server loads a server certificate and, when clientCAFile is set, requires and
// verifies client certificates (mutual TLS, TS 33.501 §13). TLS 1.2 is the
// minimum. nextProtos is the ALPN list (for example "h2").
func Server(certFile, keyFile, clientCAFile string, nextProtos ...string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: nextProtos}
	if clientCAFile != "" {
		pem, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("tlsconf: no certificates found in client CA file")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// PeerCommonName returns the subject common name of the verified client
// certificate, or "" when there is none.
func PeerCommonName(state *tls.ConnectionState) string {
	if state == nil || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return ""
	}
	return state.VerifiedChains[0][0].Subject.CommonName
}
