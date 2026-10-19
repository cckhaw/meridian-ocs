package gy

import (
	"context"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

var e2eTariff = engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: {Num: 2, Den: 1}}}

func gsuBytes(t *testing.T, cca []byte) uint64 {
	t.Helper()
	m, ok := find(cca, avpMSCC)
	if !ok {
		t.Fatal("no MSCC")
	}
	inner := append(make([]byte, diameter.HeaderLen), m.Data...)
	g, ok := find(inner, avpGrantedServiceUnit)
	if !ok {
		return 0
	}
	tot, _, _ := diameter.NextAVP(g.Data)
	v, _ := tot.Uint64()
	return v
}

// Full Gy session over TCP through the real engine, scripts and Redis.
func TestGySessionEndToEnd(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "4917", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 5000, Order: 1})
	_ = store.PutBucket(ctx, "4917", datalayer.Bucket{ID: "cash", Class: datalayer.ClassCash, AnyKind: true, Balance: 100, Order: 2})
	e, err := engine.New(engine.Config{Workers: 2}, engine.Deps{Store: store, Tariff: e2eTariff})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, addr := startServer(t, e, nil, nil)
	p := openPeer(t, addr)

	bal := func(id string) int64 {
		v, err := store.Balance(ctx, "4917", id)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	send := func(o ccrOpts) (diameter.Header, []byte) {
		p.send(buildCCR(o))
		return p.mustRecv()
	}
	rsu := func(n uint64) []msccOpts {
		return []msccOpts{{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(n)}}}
	}

	// CCR-Initial reserves 1000 bytes.
	_, cca := send(ccrOpts{session: "pgw;1", typ: ccInitial, no: 0, msisdn: "4917", mscc: rsu(1000)})
	if u32(t, cca, diameter.AVPResultCode) != diameter.ResultSuccess || gsuBytes(t, cca) != 1000 {
		t.Fatalf("initial: %d bytes", gsuBytes(t, cca))
	}
	if bal("bundle") != 4000 {
		t.Fatalf("bundle %d", bal("bundle"))
	}

	// CCR-Update reports 400 used and asks for 1000 more.
	upd := ccrOpts{session: "pgw;1", typ: ccUpdate, no: 1, msisdn: "4917",
		mscc: []msccOpts{{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(1000)}, usus: [][]unitFn{{totalOctets(400)}}}}}
	_, cca = send(upd)
	if gsuBytes(t, cca) != 1000 || bal("bundle") != 3600 {
		t.Fatalf("update: granted %d, bundle %d", gsuBytes(t, cca), bal("bundle"))
	}

	// A retransmitted CCR-Update gets the same answer and no second debit.
	_, again := send(upd)
	if gsuBytes(t, again) != 1000 || bal("bundle") != 3600 {
		t.Fatalf("retransmit: granted %d, bundle %d", gsuBytes(t, again), bal("bundle"))
	}

	// CCR-Terminate settles 300 used.
	_, cca = send(ccrOpts{session: "pgw;1", typ: ccTerminate, no: 2, msisdn: "4917",
		mscc: []msccOpts{{rg: 100, usus: [][]unitFn{{totalOctets(300)}}}}})
	if u32(t, cca, diameter.AVPResultCode) != diameter.ResultSuccess {
		t.Fatal("terminate failed")
	}
	if got := bal("bundle"); got != 5000-400-300 {
		t.Fatalf("bundle %d, want %d", got, 5000-400-300)
	}

	// An update for a session that does not exist.
	_, cca = send(ccrOpts{session: "ghost", typ: ccUpdate, no: 1, msisdn: "4917", mscc: rsu(10)})
	if got := u32(t, cca, diameter.AVPResultCode); got != diameter.ResultUnknownSessionID {
		t.Fatalf("got %d", got)
	}
}

func TestGyPartialGrantAndCreditLimit(t *testing.T) {
	store := datalayer.New(redistest.Start(t))
	ctx := context.Background()
	_ = store.PutBucket(ctx, "5000", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 300, Order: 1})
	e, _ := engine.New(engine.Config{Workers: 1}, engine.Deps{Store: store, Tariff: e2eTariff})
	defer e.Close()
	_, addr := startServer(t, e, nil, nil)
	p := openPeer(t, addr)

	req := func(session string) []byte {
		return buildCCR(ccrOpts{session: session, typ: ccInitial, msisdn: "5000",
			mscc: []msccOpts{{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(1000)}}}})
	}
	p.send(req("a"))
	_, cca := p.mustRecv()
	if u32(t, cca, diameter.AVPResultCode) != diameter.ResultSuccess || gsuBytes(t, cca) != 300 {
		t.Fatalf("granted %d", gsuBytes(t, cca))
	}
	m, _ := find(cca, avpMSCC)
	inner := append(make([]byte, diameter.HeaderLen), m.Data...)
	fui, ok := find(inner, avpFinalUnitIndication)
	if !ok {
		t.Fatal("a partial grant must carry Final-Unit-Indication")
	}
	if act, _, _ := diameter.NextAVP(fui.Data); act.Code != avpFinalUnitAction {
		t.Fatalf("%+v", act)
	} else if v, _ := act.Uint32(); v != finalUnitActionTerminate {
		t.Fatalf("action %d", v)
	}

	p.send(req("b"))
	_, cca = p.mustRecv()
	if got := u32(t, cca, diameter.AVPResultCode); got != diameter.ResultCreditLimitReached {
		t.Fatalf("got %d, want 4012", got)
	}
}
