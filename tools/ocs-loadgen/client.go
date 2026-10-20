package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

// tlsFiles is an optional client TLS setup: a CA to trust and a client certificate.
type tlsFiles struct{ CA, Cert, Key string }

func (f tlsFiles) config() (*tls.Config, error) {
	c := &tls.Config{MinVersion: tls.VersionTLS12}
	if f.CA != "" {
		pem, err := os.ReadFile(f.CA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: no certificates", f.CA)
		}
		c.RootCAs = pool
	}
	if (f.Cert == "") != (f.Key == "") {
		return nil, errors.New("a client certificate needs both its certificate and its key")
	}
	if f.Cert != "" {
		cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
		if err != nil {
			return nil, err
		}
		c.Certificates = []tls.Certificate{cert}
	}
	return c, nil
}

// nchfPool spreads requests over several HTTP/2 connections: one connection is limited by the
// server's concurrent-stream setting, and a single connection would measure that limit.
type nchfPool struct {
	base    *url.URL
	clients []*http.Client
	next    atomic.Uint64
}

func newNchfPool(base string, conns int, tf tlsFiles) (*nchfPool, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("--nchf %q: want http://host:port or https://host:port", base)
	}
	p := &nchfPool{base: u}
	for i := 0; i < conns; i++ {
		tr := &http2.Transport{}
		if u.Scheme == "http" {
			// HTTP/2 over cleartext (prior knowledge), as the development stack serves it.
			tr.AllowHTTP = true
			tr.DialTLSContext = func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, addr)
			}
		} else {
			cfg, err := tf.config()
			if err != nil {
				return nil, err
			}
			tr.TLSClientConfig = cfg
		}
		p.clients = append(p.clients, &http.Client{Transport: tr})
	}
	return p, nil
}

func (p *nchfPool) pick() *http.Client {
	return p.clients[int(p.next.Add(1))%len(p.clients)]
}

// reply is what a charging call returned. err is set when no HTTP answer arrived (transport error or
// timeout): the outcome of the request is then unknown.
type reply struct {
	status   int
	body     []byte
	location string
	err      error
}

func (p *nchfPool) post(ctx context.Context, path string, body []byte, timeout time.Duration) reply {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u := *p.base
	u.Path, u.RawQuery = path, ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return reply{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.pick().Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return reply{err: err}
	}
	return reply{status: resp.StatusCode, body: b, location: resp.Header.Get("Location")}
}

func (p *nchfPool) close() {
	for _, c := range p.clients {
		c.CloseIdleConnections()
	}
}
