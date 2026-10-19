package oauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	t0    = time.Unix(1_800_000_000, 0)
	nfID  = "6c5b8f0e-1111-4a2b-9c3d-000000000001" // this CHF instance
	nrfID = "0b7d3a52-2222-4c1e-8f4a-000000000002"
	smfID = "4a1c9e77-3333-4d0f-b5a6-000000000003"
)

type keypair struct {
	ec  *ecdsa.PrivateKey
	rsa *rsa.PrivateKey
}

var (
	keysOnce sync.Once
	kpA, kpB keypair
)

func keys(t testing.TB) (keypair, keypair) {
	keysOnce.Do(func() {
		gen := func() keypair {
			e, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			r, _ := rsa.GenerateKey(rand.Reader, 2048)
			return keypair{e, r}
		}
		kpA, kpB = gen(), gen()
	})
	return kpA, kpB
}

func pubPEM(t testing.TB, pub any) []byte {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func certPEM(t testing.TB, priv any, pub any, notBefore, notAfter time.Time) []byte {
	tpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "nrf"},
		NotBefore: notBefore, NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

type claimOpts struct {
	iss, sub string
	aud      any
	scope    string
	exp, nbf time.Time
	noExp    bool
}

func goodClaims() claimOpts {
	return claimOpts{iss: nrfID, sub: smfID, aud: []string{"CHF"}, scope: "nchf-convergedcharging", exp: t0.Add(10 * time.Minute)}
}

func (o claimOpts) mapClaims() jwt.MapClaims {
	m := jwt.MapClaims{"iss": o.iss, "sub": o.sub, "aud": o.aud, "scope": o.scope}
	if !o.noExp {
		m["exp"] = o.exp.Unix()
	}
	if !o.nbf.IsZero() {
		m["nbf"] = o.nbf.Unix()
	}
	return m
}

func sign(t testing.TB, method jwt.SigningMethod, key any, o claimOpts) string {
	s, err := jwt.NewWithClaims(method, o.mapClaims()).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newVerifier(t testing.TB, pubs []any, mod func(*Config)) *Verifier {
	var pemBytes []byte
	for _, p := range pubs {
		pemBytes = append(pemBytes, pubPEM(t, p)...)
	}
	ks, _, err := ParseKeySet(pemBytes, t0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Audience: []string{nfID, "CHF"}, Now: func() time.Time { return t0 }}
	if mod != nil {
		mod(&cfg)
	}
	v, err := NewVerifier(cfg, ks)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestAcceptsTokensSignedWithEachSupportedAlgorithm(t *testing.T) {
	a, _ := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey, &a.rsa.PublicKey}, nil)
	for name, tok := range map[string]string{
		"ES256": sign(t, jwt.SigningMethodES256, a.ec, goodClaims()),
		"RS256": sign(t, jwt.SigningMethodRS256, a.rsa, goodClaims()),
		"PS256": sign(t, jwt.SigningMethodPS256, a.rsa, goodClaims()),
	} {
		c, err := v.Verify(tok)
		if err != nil || c.Issuer != nrfID || c.Subject != smfID {
			t.Errorf("%s: %v %+v", name, err, c)
		}
	}
}

func TestRejectsBadSignaturesAndAlgorithmTricks(t *testing.T) {
	a, b := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, nil)
	good := sign(t, jwt.SigningMethodES256, a.ec, goodClaims())
	parts := strings.Split(good, ".")

	enc := func(v any) string { raw, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(raw) }
	payload := enc(goodClaims().mapClaims())

	// HS256 using the PEM text of the public key as the HMAC secret: the classic
	// algorithm-confusion attack against verifiers that trust the header.
	hsInput := enc(map[string]string{"alg": "HS256", "typ": "JWT"}) + "." + payload
	mac := hmac.New(sha256.New, pubPEM(t, &a.ec.PublicKey))
	mac.Write([]byte(hsInput))
	hs256 := hsInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	tests := map[string]string{
		"signed by another key":       sign(t, jwt.SigningMethodES256, b.ec, goodClaims()),
		"tampered payload":            parts[0] + "." + enc(func() jwt.MapClaims { m := goodClaims().mapClaims(); m["scope"] = "nudm-sdm"; return m }()) + "." + parts[2],
		"alg none":                    enc(map[string]string{"alg": "none", "typ": "JWT"}) + "." + payload + ".",
		"alg none with a signature":   enc(map[string]string{"alg": "none"}) + "." + payload + "." + parts[2],
		"HS256 keyed with the public": hs256,
		"RS256 token, only an EC key": sign(t, jwt.SigningMethodRS256, a.rsa, goodClaims()),
		"empty":                       "",
		"not a JWT":                   "hello",
		"two parts":                   parts[0] + "." + parts[1],
		"garbage parts":               "!!!.@@@.###",
		"truncated signature":         parts[0] + "." + parts[1] + "." + parts[2][:len(parts[2])/2],
		"oversized":                   strings.Repeat("a", maxTokenLen+1),
	}
	for name, tok := range tests {
		if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: want ErrInvalidToken, got %v", name, err)
		}
	}
	if v.Stats().Invalid != uint64(len(tests)) {
		t.Errorf("%+v", v.Stats())
	}
}

func TestTimeClaims(t *testing.T) {
	a, _ := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, func(c *Config) { c.Leeway = 30 * time.Second })
	mk := func(mod func(*claimOpts)) string {
		o := goodClaims()
		mod(&o)
		return sign(t, jwt.SigningMethodES256, a.ec, o)
	}
	ok := map[string]string{
		"just expired, within leeway":  mk(func(o *claimOpts) { o.exp = t0.Add(-20 * time.Second) }),
		"not yet valid, within leeway": mk(func(o *claimOpts) { o.nbf = t0.Add(20 * time.Second) }),
	}
	for name, tok := range ok {
		if _, err := v.Verify(tok); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	bad := map[string]string{
		"expired":          mk(func(o *claimOpts) { o.exp = t0.Add(-time.Minute) }),
		"not yet valid":    mk(func(o *claimOpts) { o.nbf = t0.Add(time.Minute) }),
		"no expiry at all": mk(func(o *claimOpts) { o.noExp = true }),
	}
	for name, tok := range bad {
		if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestAudienceScopeAndIssuerChecks(t *testing.T) {
	a, _ := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, func(c *Config) { c.Issuers = []string{nrfID} })
	mk := func(mod func(*claimOpts)) string {
		o := goodClaims()
		mod(&o)
		return sign(t, jwt.SigningMethodES256, a.ec, o)
	}
	for name, tok := range map[string]string{
		"audience is the NF type as a string": mk(func(o *claimOpts) { o.aud = "CHF" }),
		"audience is this instance id":        mk(func(o *claimOpts) { o.aud = []string{nfID} }),
		"audience list containing ours":       mk(func(o *claimOpts) { o.aud = []string{"UDM", "CHF"} }),
		"scope among several":                 mk(func(o *claimOpts) { o.scope = "nudm-sdm nchf-convergedcharging nsmf-pdusession" }),
	} {
		if _, err := v.Verify(tok); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, tok := range map[string]string{
		"audience for another service":  mk(func(o *claimOpts) { o.aud = []string{"UDM"} }),
		"audience for another instance": mk(func(o *claimOpts) { o.aud = []string{"11111111-0000-0000-0000-000000000000"} }),
		"no audience":                   mk(func(o *claimOpts) { o.aud = []string{} }),
		"other scope":                   mk(func(o *claimOpts) { o.scope = "nudm-sdm" }),
		"scope is only a prefix":        mk(func(o *claimOpts) { o.scope = "nchf-convergedcharging2" }),
		"scope is only a suffix":        mk(func(o *claimOpts) { o.scope = "xnchf-convergedcharging" }),
		"no scope":                      mk(func(o *claimOpts) { o.scope = "" }),
		"issuer not on the allow-list":  mk(func(o *claimOpts) { o.iss = "99999999-0000-0000-0000-000000000000" }),
	} {
		if _, err := v.Verify(tok); !errors.Is(err, ErrForbidden) {
			t.Errorf("%s: want ErrForbidden, got %v", name, err)
		}
	}
	if st := v.Stats(); st.Forbidden != 8 {
		t.Errorf("%+v", st)
	}
}

func TestVerifiedTokensAreCachedUntilTheyExpireOrKeysChange(t *testing.T) {
	a, b := keys(t)
	now := t0
	v := newVerifier(t, []any{&a.ec.PublicKey}, func(c *Config) { c.Now = func() time.Time { return now }; c.Leeway = time.Second })
	tok := sign(t, jwt.SigningMethodES256, a.ec, goodClaims())

	for i := 0; i < 5; i++ {
		if _, err := v.Verify(tok); err != nil {
			t.Fatal(err)
		}
	}
	if st := v.Stats(); st.Verified != 1 || st.CacheHits != 4 {
		t.Fatalf("one signature check expected for 5 uses: %+v", st)
	}

	// The cache never extends a token's life.
	now = t0.Add(11 * time.Minute)
	if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expired token served from the cache: %v", err)
	}

	// Rotation: a token cached under the old key set is not served afterwards.
	now = t0
	if _, err := v.Verify(tok); err != nil {
		t.Fatal(err)
	}
	ks, _, _ := ParseKeySet(pubPEM(t, &b.ec.PublicKey), t0)
	if err := v.SetKeys(ks); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("a token signed by a removed key must stop working at once: %v", err)
	}
	if _, err := v.Verify(sign(t, jwt.SigningMethodES256, b.ec, goodClaims())); err != nil {
		t.Fatalf("token from the new key: %v", err)
	}
	if err := v.SetKeys(&KeySet{}); err == nil {
		t.Fatal("an empty key set must be refused")
	}
}

func TestInvalidTokensAreNeverCached(t *testing.T) {
	a, b := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, nil)
	bad := sign(t, jwt.SigningMethodES256, b.ec, goodClaims())
	for i := 0; i < 3; i++ {
		_, _ = v.Verify(bad)
	}
	if st := v.Stats(); st.CacheHits != 0 || st.Verified != 3 {
		t.Fatalf("%+v", st)
	}
}

func TestCacheIsBounded(t *testing.T) {
	a, _ := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, func(c *Config) { c.CacheSize = 64 })
	for i := 0; i < 400; i++ {
		o := goodClaims()
		o.sub = "consumer-" + string(rune('a'+i%26)) + string(rune('A'+i/26))
		if _, err := v.Verify(sign(t, jwt.SigningMethodES256, a.ec, o)); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for i := range v.shards {
		v.shards[i].mu.Lock()
		n += len(v.shards[i].m)
		v.shards[i].mu.Unlock()
	}
	if n > 64+cacheShards {
		t.Fatalf("%d cached tokens, bound is 64", n)
	}
}

func TestKeyRotationAcceptsOldAndNewKeysWhileBothAreConfigured(t *testing.T) {
	a, b := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey, &b.ec.PublicKey}, nil)
	for i, kp := range []keypair{a, b} {
		if _, err := v.Verify(sign(t, jwt.SigningMethodES256, kp.ec, goodClaims())); err != nil {
			t.Errorf("key %d: %v", i, err)
		}
	}
}

func TestNewVerifierRequiresAudienceAndKeys(t *testing.T) {
	a, _ := keys(t)
	ks, _, _ := ParseKeySet(pubPEM(t, &a.ec.PublicKey), t0)
	if _, err := NewVerifier(Config{}, ks); err == nil {
		t.Error("no audience must be refused: any token would otherwise pass the audience check")
	}
	if _, err := NewVerifier(Config{Audience: []string{"CHF"}}, nil); err == nil {
		t.Error("no keys")
	}
}

func TestParseKeySet(t *testing.T) {
	a, b := keys(t)
	good := certPEM(t, a.ec, &a.ec.PublicKey, t0.Add(-time.Hour), t0.Add(time.Hour))
	expired := certPEM(t, b.ec, &b.ec.PublicKey, t0.Add(-48*time.Hour), t0.Add(-time.Hour))
	future := certPEM(t, b.rsa, &b.rsa.PublicKey, t0.Add(time.Hour), t0.Add(48*time.Hour))
	privDER, _ := x509.MarshalECPrivateKey(a.ec)
	priv := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	// A rotation bundle: certificate, expired certificate, not-yet-valid
	// certificate, a bare public key and a stray private key block.
	bundle := append(append(append(append(append([]byte{}, good...), expired...), future...), pubPEM(t, &b.rsa.PublicKey)...), priv...)
	ks, skipped, err := ParseKeySet(bundle, t0)
	if err != nil || ks.Len() != 2 || len(skipped) != 2 {
		t.Fatalf("len=%d skipped=%v err=%v", ks.Len(), skipped, err)
	}

	if _, _, err := ParseKeySet([]byte("not pem"), t0); err == nil {
		t.Error("garbage")
	}
	if _, skipped, err := ParseKeySet(expired, t0); err == nil || len(skipped) != 1 {
		t.Errorf("only an expired certificate leaves nothing usable: %v %v", skipped, err)
	}
	weak, _ := rsa.GenerateKey(rand.Reader, 1024)
	if _, _, err := ParseKeySet(pubPEM(t, &weak.PublicKey), t0); err == nil {
		t.Error("1024-bit RSA must be refused")
	}
	p521, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if _, _, err := ParseKeySet(pubPEM(t, &p521.PublicKey), t0); err == nil {
		t.Error("P-521 is not on the allow-list")
	}

	path := filepath.Join(t.TempDir(), "nrf.pem")
	if err := os.WriteFile(path, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if ks, _, err := LoadKeySet(path, t0); err != nil || ks.Len() != 1 {
		t.Fatalf("%v", err)
	}
	if _, _, err := LoadKeySet(filepath.Join(t.TempDir(), "missing"), t0); err == nil {
		t.Error("missing file")
	}
}

func TestVerifyIsSafeForConcurrentUse(t *testing.T) {
	a, _ := keys(t)
	v := newVerifier(t, []any{&a.ec.PublicKey}, func(c *Config) { c.CacheSize = 32 })
	toks := make([]string, 50)
	for i := range toks {
		o := goodClaims()
		o.sub = "c" + string(rune('A'+i))
		toks[i] = sign(t, jwt.SigningMethodES256, a.ec, o)
	}
	ks, _, _ := ParseKeySet(pubPEM(t, &a.ec.PublicKey), t0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if _, err := v.Verify(toks[(g+i)%len(toks)]); err != nil {
					t.Error(err)
					return
				}
				if g == 0 && i%100 == 0 {
					_ = v.SetKeys(ks)
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkVerify(b *testing.B) {
	a, _ := keys(b)
	v := newVerifier(b, []any{&a.ec.PublicKey}, nil)
	tok := sign(b, jwt.SigningMethodES256, a.ec, goodClaims())
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := v.Verify(tok); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("signature check", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v.keys.Store(&keyState{set: v.keys.Load().set, gen: v.keys.Load().gen + 1}) // defeat the cache
			if _, err := v.Verify(tok); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestWatchKeyFileRotatesKeysWithoutARestart(t *testing.T) {
	a, b := keys(t)
	path := filepath.Join(t.TempDir(), "nrf.pem")
	if err := os.WriteFile(path, pubPEM(t, &a.ec.PublicKey), 0o600); err != nil {
		t.Fatal(err)
	}
	ks, _, _ := LoadKeySet(path, t0)
	v, err := NewVerifier(Config{Audience: []string{"CHF"}, Now: func() time.Time { return t0 }}, ks)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var logs []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go WatchKeyFile(ctx, path, v, 10*time.Millisecond, func(f string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(f, args...))
		mu.Unlock()
	})

	tokA := sign(t, jwt.SigningMethodES256, a.ec, goodClaims())
	tokB := sign(t, jwt.SigningMethodES256, b.ec, goodClaims())
	if _, err := v.Verify(tokA); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(tokB); err == nil {
		t.Fatal("the new key is not trusted yet")
	}

	waitFor := func(what string, ok func() bool) {
		t.Helper()
		for i := 0; i < 300; i++ {
			if ok() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out: %s", what)
	}
	rewrite := func(content []byte) {
		// Make the change visible even on coarse file-system timestamps.
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(path, time.Now().Add(time.Second), time.Now().Add(time.Second))
	}

	// 1. Rotation: the file now holds the new key only.
	rewrite(pubPEM(t, &b.ec.PublicKey))
	waitFor("new key trusted", func() bool { _, err := v.Verify(tokB); return err == nil })
	if _, err := v.Verify(tokA); err == nil {
		t.Fatal("the removed key must stop being trusted")
	}

	// 2. A broken file must not lock everyone out.
	rewrite([]byte("this is not a key"))
	waitFor("the bad file to be noticed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range logs {
			if strings.Contains(l, "not reloading") {
				return true
			}
		}
		return false
	})
	if _, err := v.Verify(tokB); err != nil {
		t.Fatalf("the previous keys must stay in force after a bad reload: %v", err)
	}

	// 3. A rotation bundle with both keys trusts both.
	rewrite(append(pubPEM(t, &a.ec.PublicKey), pubPEM(t, &b.ec.PublicKey)...))
	waitFor("both keys trusted", func() bool { _, err := v.Verify(tokA); return err == nil })
	if _, err := v.Verify(tokB); err != nil {
		t.Fatal(err)
	}
}
