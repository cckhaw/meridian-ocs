package nchf

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/oauth/oauthtest"
)

var e2eTariff = engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: {Num: 2, Den: 1}}}

// h2cClient speaks HTTP/2 with prior knowledge over plain TCP.
func h2cClient() *http.Client {
	return &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}}
}

func serveOn(t *testing.T, srv *http.Server, tlsMode bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if tlsMode {
			_ = srv.ServeTLS(ln, "", "")
		} else {
			_ = srv.Serve(ln)
		}
	}()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func doJSON(t *testing.T, c *http.Client, url string, body any) (*http.Response, []byte) {
	t.Helper()
	resp, err := c.Post(url, "application/json", bytes.NewReader([]byte(mustJSON(body))))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

// A whole 5G charging session over HTTP/2 through the real engine and Redis.
func TestSessionOverHTTP2EndToEnd(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "262011234567890", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	e, err := engine.New(engine.Config{Workers: 2}, engine.Deps{Store: store, Tariff: e2eTariff})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	h, _ := NewHandler(Config{AllowUnauthenticated: true}, e, nil)
	addr := serveOn(t, NewServer("", h, nil), false)
	base := "http://" + addr + prefix
	c := h2cClient()
	bal := func() int64 {
		v, err := store.Balance(ctx, "262011234567890", "bundle")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	// create
	resp, body := doJSON(t, c, base+"/chargingdata", baseCreate())
	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", resp.Proto)
	}
	if resp.StatusCode != 201 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	loc := resp.Header.Get("Location")
	_ = refFromLocation(t, loc) // must be well formed
	if bal() != 4000 {
		t.Fatalf("bundle %d", bal())
	}

	// retransmitted create: same reference, same grant, no second reservation
	resp2, body2 := doJSON(t, c, base+"/chargingdata", baseCreate())
	if resp2.StatusCode != 201 || resp2.Header.Get("Location") != loc || bal() != 4000 {
		t.Fatalf("retransmit: %d loc=%q bundle=%d", resp2.StatusCode, resp2.Header.Get("Location"), bal())
	}
	_ = body2

	// update: 400 used, ask for 1000 more
	resp, body = doJSON(t, c, loc+"/update", baseUpdate(2, 400, 1000))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if bal() != 3600 {
		t.Fatalf("bundle %d", bal())
	}
	// retransmitted update: same answer, no extra debit
	resp, again := doJSON(t, c, loc+"/update", baseUpdate(2, 400, 1000))
	if resp.StatusCode != 200 || bal() != 3600 {
		t.Fatalf("update retransmit: %d bundle=%d", resp.StatusCode, bal())
	}
	var first, second chargingResponse
	_ = jsonUnmarshal(body, &first)
	_ = jsonUnmarshal(again, &second)
	if *first.MUI[0].Granted.TotalVolume != *second.MUI[0].Granted.TotalVolume {
		t.Fatal("a replayed update must return the same grant")
	}

	// release: settles 300
	resp, _ = doJSON(t, c, loc+"/release", baseUpdate(3, 300, 0))
	if resp.StatusCode != 204 {
		t.Fatalf("%d", resp.StatusCode)
	}
	if got := bal(); got != 5000-400-300 {
		t.Fatalf("bundle %d, want %d", got, 5000-400-300)
	}

	// a released session is gone: it must not reserve quota again
	resp, _ = doJSON(t, c, loc+"/update", baseUpdate(4, 1, 1))
	if resp.StatusCode != 404 || bal() != 5000-400-300 {
		t.Fatalf("update after release: %d bundle=%d", resp.StatusCode, bal())
	}
	// a reference the store has never seen is not refused: TS 32.290 5.5.1.2 makes it a valid request that creates
	// the session (the next test checks the usage is charged)
	ghost := base + "/chargingdata/r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef/update"
	if resp, _ = doJSON(t, c, ghost, baseUpdate(2, 1, 1)); resp.StatusCode != 200 {
		t.Fatalf("ghost: %d", resp.StatusCode)
	}

	// plain HTTP/1.1 reaches the same handler
	h1 := &http.Client{}
	if resp, _ = doJSON(t, h1, base+"/chargingdata", with(baseCreate(), func(o obj) { o["invocationTimeStamp"] = "2026-10-19T09:00:00Z" })); resp.ProtoMajor != 1 || resp.StatusCode != 201 {
		t.Fatalf("HTTP/1.1: %s %d", resp.Proto, resp.StatusCode)
	}
}

func TestPartialGrantAndQuotaLimitOverHTTP(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "262019999999999", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 300, Order: 1})
	e, _ := engine.New(engine.Config{Workers: 1}, engine.Deps{Store: store, Tariff: e2eTariff})
	defer e.Close()
	h, _ := NewHandler(Config{AllowUnauthenticated: true}, e, nil)
	addr := serveOn(t, NewServer("", h, nil), false)
	c := h2cClient()
	mk := func(pdu int) obj {
		return with(baseCreate(), func(o obj) {
			o["subscriberIdentifier"] = "imsi-262019999999999"
			o["pDUSessionChargingInformation"].(obj)["pduSessionInformation"].(obj)["pduSessionID"] = pdu
		})
	}
	resp, body := doJSON(t, c, "http://"+addr+prefix+"/chargingdata", mk(1))
	var r chargingResponse
	_ = jsonUnmarshal(body, &r)
	m := r.MUI[0]
	if resp.StatusCode != 201 || m.ResultCode != "SUCCESS" || *m.Granted.TotalVolume != 300 || m.FUI == nil || m.FUI.Action != "TERMINATE" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	resp, body = doJSON(t, c, "http://"+addr+prefix+"/chargingdata", mk(2))
	var r2 chargingResponse // fresh: json.Unmarshal keeps fields that are absent from the input
	_ = jsonUnmarshal(body, &r2)
	if resp.StatusCode != 201 || r2.MUI[0].ResultCode != "QUOTA_LIMIT_REACHED" || r2.MUI[0].Granted != nil {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

// ---- TLS / mutual TLS ------------------------------------------------------

type testPKI struct {
	dir                     string
	caPEM                   []byte
	serverCert, serverKey   string
	clientCertPEM, clientKP []byte
	clientTLS               tls.Certificate
	caFile                  string
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	p := &testPKI{dir: dir, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}

	issue := func(cn string, usage x509.ExtKeyUsage, ips []net.IP) (certPEM, keyPEM []byte) {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Hour),
			NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: ips}
		der, _ := x509.CreateCertificate(rand.Reader, tpl, caCert, &k.PublicKey, caKey)
		kd, _ := x509.MarshalECPrivateKey(k)
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})
	}
	write := func(name string, b []byte) string {
		f := filepath.Join(dir, name)
		if err := os.WriteFile(f, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return f
	}
	sc, sk := issue("chf", x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	p.serverCert, p.serverKey = write("s.crt", sc), write("s.key", sk)
	p.caFile = write("ca.crt", p.caPEM)
	cc, ck := issue("smf", x509.ExtKeyUsageClientAuth, nil)
	var err error
	if p.clientTLS, err = tls.X509KeyPair(cc, ck); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *testPKI) client(withCert bool) *http.Client {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(p.caPEM)
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if withCert {
		cfg.Certificates = []tls.Certificate{p.clientTLS}
	}
	return &http.Client{Transport: &http2.Transport{TLSClientConfig: cfg}, Timeout: 5 * time.Second}
}

func TestTLSWithAndWithoutClientCertificates(t *testing.T) {
	pki := newPKI(t)
	h, _ := NewHandler(Config{AllowUnauthenticated: true}, raterFunc(grantAll), nil)

	// Server TLS only.
	cfg, err := TLSConfig(pki.serverCert, pki.serverKey, "")
	if err != nil {
		t.Fatal(err)
	}
	addr := serveOn(t, NewServer("", h, cfg), true)
	resp, _ := doJSON(t, pki.client(false), "https://"+addr+prefix+"/chargingdata", baseCreate())
	if resp.ProtoMajor != 2 || resp.StatusCode != 201 || resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("%s %d", resp.Proto, resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); len(loc) < 8 || loc[:8] != "https://" {
		t.Fatalf("Location should be https, got %q", loc)
	}

	// Mutual TLS: a client without a certificate is refused during the handshake.
	cfg, err = TLSConfig(pki.serverCert, pki.serverKey, pki.caFile)
	if err != nil {
		t.Fatal(err)
	}
	addr = serveOn(t, NewServer("", h, cfg), true)
	if _, err := pki.client(false).Post("https://"+addr+prefix+"/chargingdata", "application/json", bytes.NewReader([]byte(mustJSON(baseCreate())))); err == nil {
		t.Fatal("a client without a certificate must be rejected")
	}
	if resp, _ = doJSON(t, pki.client(true), "https://"+addr+prefix+"/chargingdata", baseCreate()); resp.StatusCode != 201 {
		t.Fatalf("with a client certificate: %d", resp.StatusCode)
	}

	if _, err := TLSConfig(pki.serverCert, pki.serverKey, filepath.Join(pki.dir, "missing")); err == nil {
		t.Fatal("a missing CA file must be an error")
	}
	bad := filepath.Join(pki.dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("not pem"), 0o600)
	if _, err := TLSConfig(pki.serverCert, pki.serverKey, bad); err == nil {
		t.Fatal("a CA file without certificates must be an error")
	}
}

// TLS 1.2+, a verified client certificate AND a valid access token, over HTTP/2,
// through the real engine and Redis.
func TestTokenOverMutualTLSEndToEnd(t *testing.T) {
	pki := newPKI(t)
	iss := oauthtest.NewIssuer(t)
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "262011234567890", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	e, _ := engine.New(engine.Config{Workers: 1}, engine.Deps{Store: store, Tariff: e2eTariff})
	defer e.Close()
	h, err := NewHandler(Config{Auth: newVerifier(t, iss)}, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := TLSConfig(pki.serverCert, pki.serverKey, pki.caFile)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveOn(t, NewServer("", h, tlsCfg), true)
	c := pki.client(true)
	url := "https://" + addr + prefix + "/chargingdata"

	post := func(client *http.Client, token string) (*http.Response, error) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(mustJSON(baseCreate()))))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		return client.Do(req)
	}

	resp, err := post(c, iss.Token(t))
	if err != nil || resp.StatusCode != 201 || resp.ProtoMajor != 2 {
		t.Fatalf("authorised request: %v %v", resp, err)
	}
	resp, err = post(c, "")
	if err != nil || resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("no token: %v %v", resp, err)
	}
	if resp, err = post(c, oauthtest.NewIssuer(t).Token(t)); err != nil || resp.StatusCode != 401 {
		t.Fatalf("token from an unknown issuer: %v %v", resp, err)
	}
	// A valid token is not enough without the client certificate.
	if _, err := post(pki.client(false), iss.Token(t)); err == nil {
		t.Fatal("a client without a certificate must not get through, whatever token it holds")
	}
	if bal, _ := store.Balance(ctx, "262011234567890", "bundle"); bal != 4000 {
		t.Fatalf("only the authorised request may have reserved quota: balance %d", bal)
	}
}

// 3GPP TS 32.290 5.5.1.2: a Charging Data Request [Update] or [Termination] the CHF cannot associate with any charging
// session "shall be handled as a valid request with the associated resource creation and quota usage handling". If
// the session's state was lost (a failover, a flush, an expiry) the usage in its next request is still charged.
func TestUpdateAndReleaseOfAnUnknownSessionAreChargedPerTS32290(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	const sub = "262015550000001"
	_ = store.PutBucket(ctx, sub, datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 10000, Order: 1})
	e, err := engine.New(engine.Config{Workers: 2}, engine.Deps{Store: store, Tariff: e2eTariff})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	h, _ := NewHandler(Config{AllowUnauthenticated: true}, e, nil)
	addr := serveOn(t, NewServer("", h, nil), false)
	base := "http://" + addr + prefix
	c := h2cClient()
	bal := func() int64 {
		v, err := store.Balance(ctx, sub, "bundle")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	ref := func(hash string) string {
		return base + "/chargingdata/r1." + base64.RawURLEncoding.EncodeToString([]byte(sub)) + "." + hash
	}
	req := func(sub string) func(o obj) {
		return func(o obj) { o["subscriberIdentifier"] = "imsi-" + sub }
	}

	// An update for a session this store has never heard of: 400 used are charged and a new slice is granted.
	u := ref("aaaaaaaaaaaaaaaa") + "/update"
	resp, body := doJSON(t, c, u, with(baseUpdate(5, 400, 1000), req(sub)))
	if resp.StatusCode != 200 {
		t.Fatalf("update of an unknown session: %d %s", resp.StatusCode, body)
	}
	var first chargingResponse
	_ = jsonUnmarshal(body, &first)
	if len(first.MUI) != 1 || first.MUI[0].Granted == nil || *first.MUI[0].Granted.TotalVolume != 1000 {
		t.Fatalf("a new slice is granted: %s", body)
	}
	if got := bal(); got != 10000-400-1000 {
		t.Fatalf("400 used and 1000 reserved: bundle %d, want %d", got, 10000-400-1000)
	}
	// The network element did not get the answer and retransmits with the same Invocation Sequence Number
	// (TS 32.290 5.5.2): the pair is unique, so nothing is charged twice.
	resp, again := doJSON(t, c, u, with(baseUpdate(5, 400, 1000), req(sub)))
	var second chargingResponse
	_ = jsonUnmarshal(again, &second)
	if resp.StatusCode != 200 || bal() != 10000-400-1000 || len(second.MUI) != 1 || second.MUI[0].Granted == nil ||
		*second.MUI[0].Granted.TotalVolume != *first.MUI[0].Granted.TotalVolume {
		t.Fatalf("retransmission: %d bundle=%d %s", resp.StatusCode, bal(), again)
	}
	// The release that follows settles the slice it was given.
	resp, _ = doJSON(t, c, ref("aaaaaaaaaaaaaaaa")+"/release", with(baseUpdate(6, 300, 0), req(sub)))
	if resp.StatusCode != 204 || bal() != 10000-400-300 {
		t.Fatalf("release: %d bundle=%d, want %d", resp.StatusCode, bal(), 10000-400-300)
	}

	// A release for a session that was never seen: the usage in it is charged, once.
	r := ref("bbbbbbbbbbbbbbbb") + "/release"
	before := bal()
	resp, _ = doJSON(t, c, r, with(baseUpdate(9, 250, 0), req(sub)))
	if resp.StatusCode != 204 || bal() != before-250 {
		t.Fatalf("release of an unknown session: %d bundle=%d, want %d", resp.StatusCode, bal(), before-250)
	}
	if resp, _ = doJSON(t, c, r, with(baseUpdate(9, 250, 0), req(sub))); resp.StatusCode != 204 || bal() != before-250 {
		t.Fatalf("retransmitted release charged again: %d bundle=%d", resp.StatusCode, bal())
	}
	// A session that was terminated is not an unknown one: it stays closed and cannot be reopened.
	if resp, _ = doJSON(t, c, ref("bbbbbbbbbbbbbbbb")+"/update", with(baseUpdate(10, 1, 1), req(sub))); resp.StatusCode != 404 || bal() != before-250 {
		t.Fatalf("update after release: %d bundle=%d", resp.StatusCode, bal())
	}
}
