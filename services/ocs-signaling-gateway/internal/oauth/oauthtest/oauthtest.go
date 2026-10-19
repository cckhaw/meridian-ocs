// Package oauthtest mints NRF-style access tokens for tests.
package oauthtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issuer is a throwaway NRF signing key.
type Issuer struct {
	Key *ecdsa.PrivateKey
	// PEM is the public key in the format the gateway loads (OCS_NCHF_OAUTH_KEYS).
	PEM []byte
}

// NewIssuer generates an ES256 key pair.
func NewIssuer(tb testing.TB) *Issuer {
	tb.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if err != nil {
		tb.Fatal(err)
	}
	return &Issuer{Key: k, PEM: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})}
}

type claims struct {
	iss, sub, scope string
	aud             []string
	exp             time.Time
}

// Option changes a token claim.
type Option func(*claims)

func Audience(a ...string) Option { return func(c *claims) { c.aud = a } }
func Scope(s string) Option       { return func(c *claims) { c.scope = s } }
func Issuer_(iss string) Option   { return func(c *claims) { c.iss = iss } }
func Expires(t time.Time) Option  { return func(c *claims) { c.exp = t } }

// Token signs a token that is valid for the Nchf service unless options say
// otherwise: audience "CHF", scope "nchf-convergedcharging", 10 minutes.
func (i *Issuer) Token(tb testing.TB, opts ...Option) string {
	tb.Helper()
	c := claims{iss: "0b7d3a52-2222-4c1e-8f4a-000000000002", sub: "4a1c9e77-3333-4d0f-b5a6-000000000003",
		scope: "nchf-convergedcharging", aud: []string{"CHF"}, exp: time.Now().Add(10 * time.Minute)}
	for _, o := range opts {
		o(&c)
	}
	s, err := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": c.iss, "sub": c.sub, "aud": c.aud, "scope": c.scope, "exp": c.exp.Unix(),
	}).SignedString(i.Key)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}
