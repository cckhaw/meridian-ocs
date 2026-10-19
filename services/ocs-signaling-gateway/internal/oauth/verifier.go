// Package oauth validates the OAuth 2.0 access tokens that NF service
// consumers present to the Nchf API (3GPP TS 33.501 §13.4, TS 29.510 §6.3).
//
// A token is a JWS issued by the NRF. It is accepted only if
//
//   - its algorithm is on an allow-list (ES256, ES384, RS256, PS256) and the
//     signature verifies with one of the configured NRF public keys; "none",
//     HMAC and any algorithm/key mismatch are refused;
//   - it has an expiry and is currently valid (with a small leeway);
//   - its audience contains this producer (its NF instance id or "CHF");
//   - its scope contains "nchf-convergedcharging" as a whole word;
//   - its issuer is on the allow-list, if one is configured.
//
// Signature and format failures are "invalid token" (401); a correctly signed
// token that is not meant for this service is "forbidden" (403).
package oauth

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrInvalidToken: missing, malformed, badly signed, expired or otherwise
	// unacceptable token (HTTP 401).
	ErrInvalidToken = errors.New("oauth: invalid access token")
	// ErrForbidden: a valid token whose claims do not allow this service
	// (HTTP 403).
	ErrForbidden = errors.New("oauth: token does not grant access to this service")
)

// DefaultScope is the OAuth scope of the Nchf_ConvergedCharging service.
const DefaultScope = "nchf-convergedcharging"

const maxTokenLen = 8 << 10

var allowedMethods = []string{"ES256", "ES384", "RS256", "PS256"}

// Config configures a Verifier.
type Config struct {
	// Audience lists the values this producer accepts in `aud`: its NF
	// instance id and/or its NF type ("CHF"). At least one is required.
	Audience []string
	// Scope must appear in the token's space-separated `scope`
	// (default DefaultScope).
	Scope string
	// Issuers, if non-empty, restricts `iss` (the NRF's NF instance id).
	Issuers []string
	// Leeway tolerates clock skew on exp / nbf (default 30s).
	Leeway time.Duration
	// CacheSize bounds the cache of verified tokens (default 10000).
	CacheSize int
	Now       func() time.Time
}

// Claims are the access-token claims the service uses.
type Claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

// Stats are cumulative counters.
type Stats struct {
	Verified  uint64 // signature verifications performed
	CacheHits uint64
	Invalid   uint64
	Forbidden uint64
}

type cached struct {
	claims *Claims
	exp    time.Time
	gen    uint64
}

const cacheShards = 16

type shard struct {
	mu sync.Mutex
	m  map[[sha256.Size]byte]cached
}

// Verifier validates access tokens. It is safe for concurrent use.
type Verifier struct {
	cfg    Config
	parser *jwt.Parser
	keys   atomic.Pointer[keyState]
	shards [cacheShards]shard
	perMax int
	audSet map[string]struct{}
	issSet map[string]struct{}

	verified, hits, invalid, forbidden atomic.Uint64
}

type keyState struct {
	set *KeySet
	gen uint64
}

// NewVerifier builds a Verifier with an initial key set.
func NewVerifier(cfg Config, keys *KeySet) (*Verifier, error) {
	if len(cfg.Audience) == 0 {
		return nil, errors.New("oauth: at least one accepted audience is required")
	}
	if keys == nil || keys.Len() == 0 {
		return nil, errors.New("oauth: no verification keys")
	}
	if cfg.Scope == "" {
		cfg.Scope = DefaultScope
	}
	if cfg.Leeway <= 0 {
		cfg.Leeway = 30 * time.Second
	}
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = 10000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	v := &Verifier{cfg: cfg, perMax: (cfg.CacheSize + cacheShards - 1) / cacheShards,
		audSet: map[string]struct{}{}, issSet: map[string]struct{}{}}
	for _, a := range cfg.Audience {
		v.audSet[a] = struct{}{}
	}
	for _, i := range cfg.Issuers {
		v.issSet[i] = struct{}{}
	}
	v.parser = jwt.NewParser(
		jwt.WithValidMethods(allowedMethods),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(cfg.Leeway),
		jwt.WithTimeFunc(cfg.Now),
	)
	for i := range v.shards {
		v.shards[i].m = make(map[[sha256.Size]byte]cached)
	}
	v.keys.Store(&keyState{set: keys, gen: 1})
	return v, nil
}

// SetKeys replaces the verification keys (NRF key rotation). Tokens verified
// with the old keys are no longer accepted from the cache.
func (v *Verifier) SetKeys(keys *KeySet) error {
	if keys == nil || keys.Len() == 0 {
		return errors.New("oauth: refusing to install an empty key set")
	}
	old := v.keys.Load()
	v.keys.Store(&keyState{set: keys, gen: old.gen + 1})
	return nil
}

// Stats returns a snapshot of the counters.
func (v *Verifier) Stats() Stats {
	return Stats{v.verified.Load(), v.hits.Load(), v.invalid.Load(), v.forbidden.Load()}
}

// Verify validates a bearer token and returns its claims. The returned error
// wraps ErrInvalidToken or ErrForbidden.
func (v *Verifier) Verify(token string) (*Claims, error) {
	if token == "" || len(token) > maxTokenLen {
		v.invalid.Add(1)
		return nil, fmt.Errorf("%w: empty or oversized", ErrInvalidToken)
	}
	now := v.cfg.Now()
	ks := v.keys.Load()
	h := sha256.Sum256([]byte(token))
	sh := &v.shards[h[0]%cacheShards]

	sh.mu.Lock()
	if c, ok := sh.m[h]; ok && c.gen == ks.gen && now.Before(c.exp.Add(v.cfg.Leeway)) {
		sh.mu.Unlock()
		v.hits.Add(1)
		return c.claims, nil // claims were checked when it was cached
	} else if ok {
		delete(sh.m, h)
	}
	sh.mu.Unlock()

	claims, err := v.parse(token, ks.set)
	if err != nil {
		v.invalid.Add(1)
		return nil, err
	}
	if err := v.checkClaims(claims); err != nil {
		v.forbidden.Add(1)
		return nil, err
	}

	exp := claims.ExpiresAt.Time
	sh.mu.Lock()
	if len(sh.m) >= v.perMax {
		for k, c := range sh.m { // drop expired, then anything, to stay bounded
			if now.After(c.exp) || c.gen != ks.gen {
				delete(sh.m, k)
			}
		}
		for k := range sh.m {
			if len(sh.m) < v.perMax {
				break
			}
			delete(sh.m, k)
		}
	}
	sh.m[h] = cached{claims: claims, exp: exp, gen: ks.gen}
	sh.mu.Unlock()
	return claims, nil
}

func (v *Verifier) parse(token string, ks *KeySet) (*Claims, error) {
	var lastErr error
	for _, key := range ks.keys {
		claims := &Claims{}
		_, err := v.parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
			// The key type must match the algorithm family declared in the
			// header, so a token cannot choose how it is verified.
			switch t.Method.(type) {
			case *jwt.SigningMethodECDSA:
				if _, ok := key.(*ecdsa.PublicKey); ok {
					return key, nil
				}
			case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
				if _, ok := key.(*rsa.PublicKey); ok {
					return key, nil
				}
			}
			return nil, errKeyMismatch
		})
		v.verified.Add(1)
		if err == nil {
			return claims, nil
		}
		lastErr = err
		// A time-based failure with a good signature would fail identically
		// against every other key: stop early.
		if errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet) ||
			errors.Is(err, jwt.ErrTokenMalformed) || errors.Is(err, jwt.ErrTokenUnverifiable) && !errors.Is(err, errKeyMismatch) {
			break
		}
	}
	return nil, fmt.Errorf("%w: %v", ErrInvalidToken, lastErr)
}

var errKeyMismatch = errors.New("key does not match the token algorithm")

func (v *Verifier) checkClaims(c *Claims) error {
	if len(v.issSet) > 0 {
		if _, ok := v.issSet[c.Issuer]; !ok {
			return fmt.Errorf("%w: issuer not accepted", ErrForbidden)
		}
	}
	audOK := false
	for _, a := range c.Audience {
		if _, ok := v.audSet[a]; ok {
			audOK = true
			break
		}
	}
	if !audOK {
		return fmt.Errorf("%w: audience", ErrForbidden)
	}
	for _, s := range strings.Fields(c.Scope) {
		if s == v.cfg.Scope {
			return nil
		}
	}
	return fmt.Errorf("%w: scope %q required", ErrForbidden, v.cfg.Scope)
}
