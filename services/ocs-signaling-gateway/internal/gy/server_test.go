package gy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

func grantAll(_ context.Context, req *canonical.Request, resp *canonical.Response) error {
	resp.Result = canonical.ResultGranted
	for _, u := range req.Units {
		resp.Grants = append(resp.Grants, canonical.UnitGrant{
			RatingGroup: u.RatingGroup, Result: canonical.ResultGranted, Granted: u.Requested, ValiditySeconds: 30})
	}
	return nil
}

func initialOpts(session string, no uint32) ccrOpts {
	return ccrOpts{session: session, typ: ccInitial, no: no, msisdn: "4917",
		mscc: []msccOpts{{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(1000)}}}}
}

func TestCapabilitiesExchangeWatchdogAndDisconnect(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, nil)
	p := dial(t, addr)

	p.send(buildCER(diameter.AppCreditControl, true))
	h, cea := p.mustRecv()
	if h.IsRequest || h.Command != diameter.CmdCER || h.HopByHop != 7 || h.EndToEnd != 8 {
		t.Fatalf("%+v", h)
	}
	if u32(t, cea, diameter.AVPResultCode) != diameter.ResultSuccess || u32(t, cea, diameter.AVPAuthApplicationID) != diameter.AppCreditControl {
		t.Fatal("bad CEA")
	}
	if host, _ := find(cea, diameter.AVPOriginHost); string(host.Data) != "ocs.example.net" {
		t.Fatalf("%q", host.Data)
	}
	if _, ok := find(cea, diameter.AVPHostIPAddress); !ok {
		t.Fatal("Host-IP-Address missing")
	}

	p.send(buildSimple(diameter.CmdDWR, 0x55, 0x66))
	h, dwa := p.mustRecv()
	if h.Command != diameter.CmdDWR || h.IsRequest || h.HopByHop != 0x55 || h.EndToEnd != 0x66 ||
		u32(t, dwa, diameter.AVPResultCode) != diameter.ResultSuccess {
		t.Fatalf("%+v", h)
	}

	p.send(buildSimple(diameter.CmdDPR, 1, 2))
	h, dpa := p.mustRecv()
	if h.Command != diameter.CmdDPR || u32(t, dpa, diameter.AVPResultCode) != diameter.ResultSuccess {
		t.Fatalf("%+v", h)
	}
	p.expectClosed()
}

func TestCapabilitiesExchangeRejections(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, nil)

	p := dial(t, addr)
	p.send(buildCER(0, true)) // advertises no application
	_, cea := p.mustRecv()
	if got := u32(t, cea, diameter.AVPResultCode); got != diameter.ResultNoCommonApplication {
		t.Fatalf("got %d", got)
	}
	p.expectClosed()

	p = dial(t, addr)
	p.send(buildCER(diameter.AppCreditControl, false)) // no Origin-Host
	_, cea = p.mustRecv()
	if got := u32(t, cea, diameter.AVPResultCode); got != diameter.ResultMissingAVP {
		t.Fatalf("got %d", got)
	}
	p.expectClosed()
}

func TestNothingButCERBeforeOpen(t *testing.T) {
	var calls atomic.Int64
	_, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls.Add(1)
		return grantAll(ctx, r, s)
	}), nil, nil)
	p := dial(t, addr)
	p.send(buildCCR(initialOpts("s", 0)))
	p.expectClosed()
	if calls.Load() != 0 {
		t.Fatal("a CCR before CER must not reach the rating engine")
	}
}

func TestUnknownCommandGets3001WithErrorBit(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, nil)
	p := openPeer(t, addr)
	p.send(buildSimple(999, 5, 6))
	h, m := p.mustRecv()
	if h.Command != 999 || u32(t, m, diameter.AVPResultCode) != diameter.ResultCommandUnsupported || h.Flags&diameter.FlagError == 0 {
		t.Fatalf("%+v", h)
	}
}

func TestGrantedAnswerEchoesIdentifiers(t *testing.T) {
	var seen *canonical.Request
	var mu sync.Mutex
	_, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		mu.Lock()
		c := *r
		c.Units = append([]canonical.UnitRequest(nil), r.Units...)
		seen = &c
		mu.Unlock()
		return grantAll(ctx, r, s)
	}), nil, nil)
	p := openPeer(t, addr)

	o := initialOpts("pgw;9;9", 0)
	o.hbh, o.e2e = 0xAABB, 0xCCDD
	p.send(buildCCR(o))
	h, cca := p.mustRecv()

	if h.IsRequest || h.HopByHop != 0xAABB || h.EndToEnd != 0xCCDD || h.Command != diameter.CmdCCR || h.AppID != diameter.AppCreditControl {
		t.Fatalf("%+v", h)
	}
	if h.Flags&diameter.FlagError != 0 {
		t.Fatal("E bit set on a success answer")
	}
	if sid, _ := find(cca, diameter.AVPSessionID); string(sid.Data) != "pgw;9;9" {
		t.Fatalf("%q", sid.Data)
	}
	if u32(t, cca, diameter.AVPResultCode) != diameter.ResultSuccess || u32(t, cca, avpCCType) != ccInitial || u32(t, cca, avpCCNumber) != 0 {
		t.Fatal("bad CCA header AVPs")
	}
	m, _ := find(cca, avpMSCC)
	inner := append(make([]byte, diameter.HeaderLen), m.Data...)
	if u32(t, inner, avpRatingGroup) != 100 || u32(t, inner, avpValidityTime) != 30 {
		t.Fatal("rating group / validity missing")
	}
	g, _ := find(inner, avpGrantedServiceUnit)
	tot, _, _ := diameter.NextAVP(g.Data)
	if v, _ := tot.Uint64(); tot.Code != avpCCTotalOctets || v != 1000 {
		t.Fatalf("GSU %+v %d", tot, v)
	}

	mu.Lock()
	defer mu.Unlock()
	if seen == nil || seen.SubscriberID != "4917" || seen.SessionID != "pgw;9;9" || seen.Type != canonical.TypeInitial || seen.Units[0].Requested != 1000 {
		t.Fatalf("rater saw %+v", seen)
	}
}

func TestResultMappingToDiameter(t *testing.T) {
	tests := []struct {
		res  canonical.Result
		want uint32
		eBit bool
	}{
		{canonical.ResultCreditLimitReached, diameter.ResultCreditLimitReached, false},
		{canonical.ResultNoBucket, diameter.ResultRatingFailed, false},
		{canonical.ResultUnknownSession, diameter.ResultUnknownSessionID, false},
		{canonical.ResultStaleRequest, diameter.ResultUnableToComply, false},
		{canonical.ResultUnavailable, diameter.ResultEndUserDenied, false},
		{canonical.ResultBarred, diameter.ResultEndUserDenied, false},
		{canonical.ResultTooBusy, diameter.ResultTooBusy, true},
	}
	var current atomic.Uint32
	_, addr := startServer(t, raterFunc(func(_ context.Context, _ *canonical.Request, s *canonical.Response) error {
		s.Result = canonical.Result(current.Load())
		return nil
	}), nil, nil)
	p := openPeer(t, addr)
	for i, tc := range tests {
		current.Store(uint32(tc.res))
		p.send(buildCCR(initialOpts("s", uint32(i))))
		h, m := p.mustRecv()
		if got := u32(t, m, diameter.AVPResultCode); got != tc.want {
			t.Errorf("%d: got %d want %d", tc.res, got, tc.want)
		}
		if (h.Flags&diameter.FlagError != 0) != tc.eBit {
			t.Errorf("%d: E bit = %v", tc.res, !tc.eBit)
		}
		if _, has := find(m, avpMSCC); has {
			t.Errorf("%d: failures must not carry MSCC", tc.res)
		}
	}
}

func TestTerminateAnswerCarriesNoServiceUnits(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, nil)
	p := openPeer(t, addr)
	p.send(buildCCR(ccrOpts{session: "s", typ: ccTerminate, no: 2, msisdn: "4917",
		mscc: []msccOpts{{rg: 100, usus: [][]unitFn{{totalOctets(5)}}}}}))
	_, m := p.mustRecv()
	if u32(t, m, diameter.AVPResultCode) != diameter.ResultSuccess {
		t.Fatal("terminate must succeed")
	}
	if _, has := find(m, avpMSCC); has {
		t.Fatal("CCA-T must not grant units")
	}
}

func TestEngineErrorsMapToDiameter(t *testing.T) {
	tests := []struct {
		err  error
		want uint32
	}{
		{engine.ErrOverloaded, diameter.ResultTooBusy},
		{engine.ErrInvalid, diameter.ResultInvalidAVPValue},
		{engine.ErrClosed, diameter.ResultUnableToDeliver},
	}
	var cur atomic.Pointer[error]
	_, addr := startServer(t, raterFunc(func(context.Context, *canonical.Request, *canonical.Response) error {
		return *cur.Load()
	}), nil, nil)
	p := openPeer(t, addr)
	for i, tc := range tests {
		e := tc.err
		cur.Store(&e)
		p.send(buildCCR(initialOpts("s", uint32(i))))
		_, m := p.mustRecv()
		if got := u32(t, m, diameter.AVPResultCode); got != tc.want {
			t.Errorf("%v: got %d want %d", tc.err, got, tc.want)
		}
	}
}

func TestMalformedCCRGetsErrorAnswerAndConnectionSurvives(t *testing.T) {
	var calls atomic.Int64
	srv, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls.Add(1)
		return grantAll(ctx, r, s)
	}), nil, nil)
	p := openPeer(t, addr)

	bad := initialOpts("", 0) // no Session-Id
	p.send(buildCCR(bad))
	_, m := p.mustRecv()
	if got := u32(t, m, diameter.AVPResultCode); got != diameter.ResultMissingAVP {
		t.Fatalf("got %d", got)
	}
	if _, ok := find(m, diameter.AVPErrorMessage); !ok {
		t.Fatal("Error-Message missing")
	}

	p.send(buildCCR(initialOpts("good", 0)))
	_, m = p.mustRecv()
	if got := u32(t, m, diameter.AVPResultCode); got != diameter.ResultSuccess {
		t.Fatalf("connection should keep working, got %d", got)
	}
	if calls.Load() != 1 || srv.Stats().Rejected != 1 {
		t.Fatalf("calls=%d stats=%+v", calls.Load(), srv.Stats())
	}
}

func TestAdmissionControlShedsWith3004BeforeRating(t *testing.T) {
	var calls atomic.Int64
	adm := admission.New(1, 1, 0) // one request, then nothing for a long time
	srv, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls.Add(1)
		return grantAll(ctx, r, s)
	}), adm, nil)
	p := openPeer(t, addr)
	p.send(buildCCR(initialOpts("a", 0)))
	_, m := p.mustRecv()
	if u32(t, m, diameter.AVPResultCode) != diameter.ResultSuccess {
		t.Fatal("first request must pass")
	}
	p.send(buildCCR(initialOpts("b", 0)))
	h, m := p.mustRecv()
	if u32(t, m, diameter.AVPResultCode) != diameter.ResultTooBusy || h.Flags&diameter.FlagError == 0 {
		t.Fatal("second request must be shed with 3004")
	}
	if sid, _ := find(m, diameter.AVPSessionID); string(sid.Data) != "b" {
		t.Fatal("shed answer must still echo the Session-Id")
	}
	if calls.Load() != 1 || srv.Stats().Shed != 1 {
		t.Fatalf("calls=%d %+v", calls.Load(), srv.Stats())
	}
}

func TestPerConnectionInflightLimitSheds(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	_, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		started <- struct{}{}
		<-release
		return grantAll(ctx, r, s)
	}), nil, func(c *Config) { c.MaxInflightPerConn = 1 })
	p := openPeer(t, addr)

	p.send(buildCCR(initialOpts("a", 0)))
	<-started
	p.send(buildCCR(initialOpts("b", 0)))
	_, m := p.mustRecv() // answered immediately while "a" is still being rated
	if got := u32(t, m, diameter.AVPResultCode); got != diameter.ResultTooBusy {
		t.Fatalf("got %d", got)
	}
	close(release)
	_, m = p.mustRecv()
	if got := u32(t, m, diameter.AVPResultCode); got != diameter.ResultSuccess {
		t.Fatalf("the first request must still complete, got %d", got)
	}
}

func TestOversizedMessageDropsConnection(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, func(c *Config) { c.MaxMessage = 256 })
	p := openPeer(t, addr)
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest, diameter.CmdCCR, diameter.AppCreditControl, 1, 1)
	b.Bytes(diameter.AVPSessionID, diameter.AVPFlagMandatory, make([]byte, 1000))
	m, _ := b.Finish()
	p.send(m)
	p.expectClosed()
}

func TestSilentPeerGetsWatchdogThenIsDropped(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, func(c *Config) { c.WatchdogInterval = 100 * time.Millisecond })
	p := openPeer(t, addr)
	h, m := p.mustRecv() // the server asks "are you alive?"
	if h.Command != diameter.CmdDWR || !h.IsRequest {
		t.Fatalf("%+v", h)
	}
	if _, ok := find(m, diameter.AVPOriginHost); !ok {
		t.Fatal("DWR needs Origin-Host")
	}
	// Never answer: the connection must be closed after ~3 intervals.
	for {
		_, _, ok := p.recv()
		if !ok {
			return
		}
	}
}

func TestCloseDisconnectsPeersAndStopsServing(t *testing.T) {
	srv, addr := startServer(t, raterFunc(grantAll), nil, nil)
	p := openPeer(t, addr)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	p.expectClosed()
	if srv.Stats().PeersOpen != 0 {
		t.Fatalf("%+v", srv.Stats())
	}
}

func TestConcurrentPeersAndPipelinedRequests(t *testing.T) {
	_, addr := startServer(t, raterFunc(grantAll), nil, nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p := openPeer(t, addr)
			const n = 50
			for i := 0; i < n; i++ { // pipeline without waiting for answers
				o := initialOpts("s", 0)
				o.session = o.session + string(rune('a'+g)) + string(rune('A'+i%26))
				o.hbh = uint32(g*1000 + i + 1)
				p.send(buildCCR(o))
			}
			seen := map[uint32]bool{}
			for i := 0; i < n; i++ {
				h, m := p.mustRecv()
				if u32(t, m, diameter.AVPResultCode) != diameter.ResultSuccess {
					t.Error("failed answer")
				}
				seen[h.HopByHop] = true
			}
			if len(seen) != n {
				t.Errorf("peer %d: %d distinct hop-by-hop ids in answers, want %d", g, len(seen), n)
			}
		}(g)
	}
	wg.Wait()
}

// ---- subscriber identity resolution ---------------------------------------

type fakeDir struct {
	m   map[identity.Identity]string
	err error
}

func (d *fakeDir) Lookup(_ context.Context, id identity.Identity) (string, error) {
	if d.err != nil {
		return "", d.err
	}
	if k, ok := d.m[id]; ok {
		return k, nil
	}
	return "", identity.ErrUnknown
}

func TestSubscriberIsResolvedToTheInternalKey(t *testing.T) {
	dir := &fakeDir{m: map[identity.Identity]string{
		{Type: identity.MSISDN, Value: "491701234567"}:  "acct-1",
		{Type: identity.IMSI, Value: "262011234567890"}: "acct-1",
		{Type: identity.MSISDN, Value: "491709999999"}:  "acct-2",
	}}
	var calls atomic.Int64
	var seen atomic.Value
	_, addr := startServer(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls.Add(1)
		seen.Store(r.SubscriberID)
		return grantAll(ctx, r, s)
	}), nil, func(c *Config) { c.Identity = identity.NewResolver(dir, identity.ConflictReject) })
	p := openPeer(t, addr)

	units := []msccOpts{{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(10)}}}
	resultOf := func(o ccrOpts) (uint32, bool) {
		p.send(buildCCR(o))
		h, m := p.mustRecv()
		return u32(t, m, diameter.AVPResultCode), h.Flags&diameter.FlagError != 0
	}

	// MSISDN only, IMSI only, and both: the same subscriber key every time.
	for i, o := range []ccrOpts{
		{msisdn: "491701234567"},
		{imsi: "262011234567890"},
		{msisdn: "+491701234567", imsi: "262011234567890"},
	} {
		o.session, o.typ, o.no, o.mscc = "s", ccInitial, uint32(i), units
		o.session += string(rune('a' + i))
		if rc, _ := resultOf(o); rc != diameter.ResultSuccess || seen.Load() != "acct-1" {
			t.Fatalf("case %d: result %d, key %v", i, rc, seen.Load())
		}
	}

	before := calls.Load()
	tests := []struct {
		name  string
		o     ccrOpts
		want  uint32
		eBit  bool
		setup func()
	}{
		{"unknown subscriber", ccrOpts{msisdn: "491700000000"}, diameter.ResultUserUnknown, false, nil},
		{"malformed MSISDN", ccrOpts{msisdn: "12ab"}, diameter.ResultInvalidAVPValue, false, nil},
		{"identities of two subscribers", ccrOpts{msisdn: "491709999999", imsi: "262011234567890"}, diameter.ResultAuthRejected, false, nil},
		{"directory down", ccrOpts{msisdn: "491701234567"}, diameter.ResultUnableToDeliver, true, func() { dir.err = errors.New("redis down") }},
	}
	for i, tc := range tests {
		if tc.setup != nil {
			tc.setup()
		}
		tc.o.session, tc.o.typ, tc.o.no, tc.o.mscc = "f"+string(rune('a'+i)), ccInitial, 0, units
		rc, eBit := resultOf(tc.o)
		if rc != tc.want || eBit != tc.eBit {
			t.Errorf("%s: result %d (E=%v), want %d (E=%v)", tc.name, rc, eBit, tc.want, tc.eBit)
		}
	}
	if calls.Load() != before {
		t.Fatalf("%d requests with unresolvable subscribers reached the rating engine", calls.Load()-before)
	}
}
