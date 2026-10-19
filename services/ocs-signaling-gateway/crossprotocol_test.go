package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/gy"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/nchf"
)

// A Diameter exchange written independently of the gy package's own helpers.
type diamPeer struct {
	t *testing.T
	c net.Conn
}

func (p *diamPeer) exchange(msg []byte) (diameter.Header, []byte) {
	p.t.Helper()
	if _, err := p.c.Write(msg); err != nil {
		p.t.Fatal(err)
	}
	_ = p.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var hdr [diameter.HeaderLen]byte
	if _, err := io.ReadFull(p.c, hdr[:]); err != nil {
		p.t.Fatal(err)
	}
	h, err := diameter.ParseHeader(hdr[:])
	if err != nil {
		p.t.Fatal(err)
	}
	out := make([]byte, h.Length)
	copy(out, hdr[:])
	if _, err := io.ReadFull(p.c, out[diameter.HeaderLen:]); err != nil {
		p.t.Fatal(err)
	}
	return h, out
}

func resultCode(t *testing.T, msg []byte) uint32 {
	t.Helper()
	data := msg[diameter.HeaderLen:]
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			break
		}
		if a.Code == diameter.AVPResultCode && a.Flags&diameter.AVPFlagVendor == 0 {
			v, _ := a.Uint32()
			return v
		}
		data = rest
	}
	t.Fatal("no Result-Code")
	return 0
}

func gyOpen(t *testing.T, addr string) *diamPeer {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	p := &diamPeer{t: t, c: c}
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest, diameter.CmdCER, 0, 1, 1)
	b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, "pgw.example.net")
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, "example.net")
	b.Address(diameter.AVPHostIPAddress, diameter.AVPFlagMandatory, net.ParseIP("10.0.0.1"))
	b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, diameter.AppCreditControl)
	msg, _ := b.Finish()
	if _, cea := p.exchange(msg); resultCode(t, cea) != diameter.ResultSuccess {
		t.Fatal("CER rejected")
	}
	return p
}

// gyCCR builds a CCR-Initial (type 1) for the identifiers, asking for n bytes.
func gyCCR(session string, no, typ uint32, msisdn, imsi string, requested uint64) []byte {
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest|diameter.FlagProxiable, diameter.CmdCCR, diameter.AppCreditControl, 100+no, 200+no)
	b.String(diameter.AVPSessionID, diameter.AVPFlagMandatory, session)
	b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, "pgw.example.net")
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, "example.net")
	b.String(diameter.AVPDestinationRealm, diameter.AVPFlagMandatory, "example.net")
	b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, diameter.AppCreditControl)
	b.Uint32(416, diameter.AVPFlagMandatory, typ)
	b.Uint32(415, diameter.AVPFlagMandatory, no)
	sub := func(kind uint32, v string) {
		g := b.BeginGrouped(443, diameter.AVPFlagMandatory)
		b.Uint32(450, diameter.AVPFlagMandatory, kind)
		b.String(444, diameter.AVPFlagMandatory, v)
		b.EndGrouped(g)
	}
	if msisdn != "" {
		sub(0, msisdn)
	}
	if imsi != "" {
		sub(1, imsi)
	}
	m := b.BeginGrouped(456, diameter.AVPFlagMandatory)
	b.Uint32(432, diameter.AVPFlagMandatory, 100)
	r := b.BeginGrouped(437, diameter.AVPFlagMandatory)
	b.Uint64(421, diameter.AVPFlagMandatory, requested)
	b.EndGrouped(r)
	b.EndGrouped(m)
	msg, _ := b.Finish()
	return append([]byte(nil), msg...)
}

func nchfCreate(t *testing.T, c *http.Client, base, supi string, pdu int, requested uint64) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"subscriberIdentifier":          supi,
		"nfConsumerIdentification":      map[string]any{"nodeFunctionality": "SMF"},
		"invocationTimeStamp":           "2026-10-19T08:00:00Z",
		"invocationSequenceNumber":      1,
		"multipleUnitUsage":             []any{map[string]any{"ratingGroup": 100, "requestedUnit": map[string]any{"totalVolume": requested}}},
		"pDUSessionChargingInformation": map[string]any{"pduSessionInformation": map[string]any{"pduSessionID": pdu}},
	})
	resp, err := c.Post(base+"/chargingdata", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header.Get("Location")
}

// The point of the identity directory: one subscriber, however it is
// identified and over whichever interface, is charged against one balance.
func TestOneSubscriberOneBalanceAcrossGyAndNchf(t *testing.T) {
	rdb := redistest.Start(t)
	store := datalayer.New(rdb)
	ctx := context.Background()

	dir := identity.NewRedisDirectory(rdb, "")
	for _, b := range []struct {
		t identity.Type
		v string
	}{{identity.MSISDN, "491701234567"}, {identity.IMSI, "262011234567890"}} {
		if err := dir.Bind(ctx, identity.Identity{Type: b.t, Value: b.v}, "acct-1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutBucket(ctx, "acct-1", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 10_000, Order: 1}); err != nil {
		t.Fatal(err)
	}
	resolver := identity.NewResolver(identity.NewCachedDirectory(dir, identity.CacheConfig{TTL: 50 * time.Millisecond, NegativeTTL: 10 * time.Millisecond}), identity.ConflictReject)

	eng, err := engine.New(engine.Config{Workers: 2}, engine.Deps{Store: store,
		Tariff: engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: {Num: 1, Den: 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	gySrv, err := gy.NewServer(gy.Config{OriginHost: "ocs.example.net", OriginRealm: "example.net", HostIP: net.ParseIP("10.9.9.9"), Identity: resolver}, eng, nil)
	if err != nil {
		t.Fatal(err)
	}
	gyLn, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = gySrv.Serve(gyLn) }()
	defer gySrv.Close()

	h, err := nchf.NewHandler(nchf.Config{Identity: resolver}, eng, nil)
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := nchf.NewServer("", h, nil)
	httpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = httpSrv.Serve(httpLn) }()
	defer httpSrv.Close()
	h2c := &http.Client{Transport: &http2.Transport{AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, n, a string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, n, a)
		}}}
	nchfBase := "http://" + httpLn.Addr().String() + "/nchf-convergedcharging/v3"

	bal := func(sub string) int64 {
		v, err := store.Balance(ctx, sub, "bundle")
		if err != nil {
			t.Fatalf("%s: %v", sub, err)
		}
		return v
	}

	// 5G: IMSI-based SUPI reserves 1000.
	if code, _ := nchfCreate(t, h2c, nchfBase, "imsi-262011234567890", 1, 1000); code != 201 || bal("acct-1") != 9000 {
		t.Fatalf("nchf: %d balance=%d", code, bal("acct-1"))
	}
	// 4G: MSISDN reserves 2000 from the same balance.
	p := gyOpen(t, gyLn.Addr().String())
	if _, cca := p.exchange(gyCCR("pgw;1", 0, 1, "491701234567", "", 2000)); resultCode(t, cca) != diameter.ResultSuccess || bal("acct-1") != 7000 {
		t.Fatalf("gy by MSISDN: result %d balance=%d", resultCode(t, cca), bal("acct-1"))
	}
	// 4G: IMSI alone, and MSISDN+IMSI together, also reach it.
	if _, cca := p.exchange(gyCCR("pgw;2", 0, 1, "", "262011234567890", 500)); resultCode(t, cca) != diameter.ResultSuccess || bal("acct-1") != 6500 {
		t.Fatalf("gy by IMSI: result %d balance=%d", resultCode(t, cca), bal("acct-1"))
	}
	if _, cca := p.exchange(gyCCR("pgw;3", 0, 1, "491701234567", "262011234567890", 500)); resultCode(t, cca) != diameter.ResultSuccess || bal("acct-1") != 6000 {
		t.Fatalf("gy by both: result %d balance=%d", resultCode(t, cca), bal("acct-1"))
	}

	// No state exists under the raw identifiers any more.
	for _, raw := range []string{"491701234567", "262011234567890"} {
		if _, err := store.Balance(ctx, raw, "bundle"); err == nil {
			t.Fatalf("a balance was created under the raw identifier %s", raw)
		}
	}

	// Unknown subscribers are refused on both interfaces, before rating.
	if _, cca := p.exchange(gyCCR("pgw;4", 0, 1, "491700000000", "", 10)); resultCode(t, cca) != diameter.ResultUserUnknown {
		t.Fatalf("gy unknown: %d", resultCode(t, cca))
	}
	if code, _ := nchfCreate(t, h2c, nchfBase, "imsi-262019999999999", 1, 10); code != 404 {
		t.Fatalf("nchf unknown: %d", code)
	}

	// SIM swap: the IMSI moves to another subscriber; the MSISDN stays. Until
	// the cache entry expires the old binding is used (a documented window);
	// afterwards the two identifiers disagree and the request is refused
	// instead of being charged to a guess.
	if err := store.PutBucket(ctx, "acct-2", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 10_000, Order: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := dir.Rebind(ctx, identity.Identity{Type: identity.IMSI, Value: "262011234567890"}, "acct-2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond) // cache TTL is 50ms in this test
	if _, cca := p.exchange(gyCCR("pgw;5", 0, 1, "491701234567", "262011234567890", 10)); resultCode(t, cca) != diameter.ResultAuthRejected {
		t.Fatalf("conflicting identities: %d", resultCode(t, cca))
	}
	if code, _ := nchfCreate(t, h2c, nchfBase, "imsi-262011234567890", 9, 700); code != 201 || bal("acct-2") != 9300 || bal("acct-1") != 6000 {
		t.Fatalf("after the SIM swap the IMSI must charge acct-2: %d acct-2=%d acct-1=%d", code, bal("acct-2"), bal("acct-1"))
	}
}
