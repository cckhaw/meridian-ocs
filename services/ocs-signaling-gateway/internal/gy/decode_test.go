package gy

import (
	"net"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
)

func testCfg() *Config {
	c := Config{OriginHost: "ocs", OriginRealm: "r", HostIP: net.ParseIP("10.0.0.1")}
	if err := c.setDefaults(); err != nil {
		panic(err)
	}
	return &c
}

func decode(t *testing.T, o ccrOpts) (*canonical.Request, ccrInfo, *DecodeError) {
	t.Helper()
	msg := buildCCR(o)
	h, err := diameter.ParseHeader(msg)
	if err != nil {
		t.Fatal(err)
	}
	req := &canonical.Request{}
	var info ccrInfo
	derr := decodeCCR(msg, h, testCfg(), req, &info)
	return req, info, derr
}

func TestDecodeInitial(t *testing.T) {
	req, info, derr := decode(t, ccrOpts{
		session: "pgw;1;2", typ: ccInitial, no: 0, msisdn: "491701234567", imsi: "262011234567890",
		mscc:   []msccOpts{{rg: 100, sid: 7, hasRSU: true, rsu: []unitFn{totalOctets(1_000_000)}}},
		ratSet: true, rat: 6,
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if req.Source != canonical.SourceDiameterGy || req.Type != canonical.TypeInitial ||
		req.SubscriberID != "491701234567" || req.SessionID != "pgw;1;2" || req.RequestNumber != 0 || req.RATType != "6" {
		t.Fatalf("%+v", req)
	}
	u := req.Units[0]
	if u.RatingGroup != 100 || u.ServiceID != 7 || u.Kind != canonical.UnitBytes || u.Requested != 1_000_000 || u.Used != 0 {
		t.Fatalf("%+v", u)
	}
	if string(info.sessionID) != "pgw;1;2" || info.reqType != ccInitial {
		t.Fatalf("%+v", info)
	}
}

func TestDecodeUnitKindsAndUsage(t *testing.T) {
	tests := []struct {
		name string
		m    msccOpts
		want canonical.UnitRequest
	}{
		{"input+output when no total", msccOpts{rg: 1, hasRSU: true, rsu: []unitFn{inOutOctets(300, 200)}},
			canonical.UnitRequest{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 500}},
		{"total wins over input+output", msccOpts{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(900), inOutOctets(1, 1)}},
			canonical.UnitRequest{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 900}},
		{"seconds", msccOpts{rg: 2, hasRSU: true, rsu: []unitFn{ccTime(60)}},
			canonical.UnitRequest{RatingGroup: 2, Kind: canonical.UnitSeconds, Requested: 60}},
		{"events", msccOpts{rg: 3, hasRSU: true, rsu: []unitFn{events(1)}},
			canonical.UnitRequest{RatingGroup: 3, Kind: canonical.UnitEvents, Requested: 1}},
		{"empty RSU uses the default for the usage kind", msccOpts{rg: 4, hasRSU: true, usus: [][]unitFn{{ccTime(5)}}},
			canonical.UnitRequest{RatingGroup: 4, Kind: canonical.UnitSeconds, Requested: 300, Used: 5}},
		{"empty RSU defaults to bytes", msccOpts{rg: 5, hasRSU: true},
			canonical.UnitRequest{RatingGroup: 5, Kind: canonical.UnitBytes, Requested: 10 << 20}},
		{"usage only asks for nothing", msccOpts{rg: 6, usus: [][]unitFn{{totalOctets(77)}}},
			canonical.UnitRequest{RatingGroup: 6, Kind: canonical.UnitBytes, Used: 77}},
		{"several USU are summed", msccOpts{rg: 7, hasRSU: true, rsu: []unitFn{totalOctets(10)},
			usus: [][]unitFn{{totalOctets(100)}, {inOutOctets(20, 30)}}},
			canonical.UnitRequest{RatingGroup: 7, Kind: canonical.UnitBytes, Requested: 10, Used: 150}},
	}
	for _, tc := range tests {
		req, _, derr := decode(t, ccrOpts{session: "s", typ: ccUpdate, no: 1, msisdn: "1", mscc: []msccOpts{tc.m}})
		if derr != nil {
			t.Errorf("%s: %v", tc.name, derr)
			continue
		}
		if got := req.Units[0]; got != tc.want {
			t.Errorf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	ok := msccOpts{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1)}}
	tests := []struct {
		name string
		o    ccrOpts
		want uint32
	}{
		{"no session", ccrOpts{typ: ccInitial, msisdn: "1", mscc: []msccOpts{ok}}, diameter.ResultMissingAVP},
		{"no type", ccrOpts{session: "s", msisdn: "1", mscc: []msccOpts{ok}}, diameter.ResultMissingAVP},
		{"no number", ccrOpts{session: "s", typ: ccInitial, noNo: true, msisdn: "1", mscc: []msccOpts{ok}}, diameter.ResultMissingAVP},
		{"no subscriber", ccrOpts{session: "s", typ: ccInitial, mscc: []msccOpts{ok}}, diameter.ResultMissingAVP},
		{"bad type", ccrOpts{session: "s", typ: 9, msisdn: "1", mscc: []msccOpts{ok}}, diameter.ResultInvalidAVPValue},
		{"initial without MSCC", ccrOpts{session: "s", typ: ccInitial, msisdn: "1"}, diameter.ResultMissingAVP},
		{"wrong application", ccrOpts{app: 16777238, session: "s", typ: ccInitial, msisdn: "1", mscc: []msccOpts{ok}}, diameter.ResultApplicationUnsup},
		{"mixed kinds in one unit", ccrOpts{session: "s", typ: ccInitial, msisdn: "1",
			mscc: []msccOpts{{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1), ccTime(1)}}}}, diameter.ResultInvalidAVPValue},
		{"RSU and USU of different kinds", ccrOpts{session: "s", typ: ccUpdate, msisdn: "1",
			mscc: []msccOpts{{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1)}, usus: [][]unitFn{{ccTime(1)}}}}}, diameter.ResultInvalidAVPValue},
	}
	for _, tc := range tests {
		_, _, derr := decode(t, tc.o)
		if derr == nil || derr.Result != tc.want {
			t.Errorf("%s: got %v, want result %d", tc.name, derr, tc.want)
		}
	}
	// Terminate needs no MSCC.
	if _, _, derr := decode(t, ccrOpts{session: "s", typ: ccTerminate, no: 2, msisdn: "1"}); derr != nil {
		t.Errorf("terminate without MSCC: %v", derr)
	}
}

func TestDecodeSubscriberSelection(t *testing.T) {
	ok := []msccOpts{{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1)}}}
	req, _, _ := decode(t, ccrOpts{session: "s", typ: ccInitial, imsi: "262011111", mscc: ok})
	if req.SubscriberID != "262011111" {
		t.Fatalf("IMSI fallback: %q", req.SubscriberID)
	}
	req, _, _ = decode(t, ccrOpts{session: "s", typ: ccInitial, imsi: "262011111", msisdn: "4917", mscc: ok})
	if req.SubscriberID != "4917" {
		t.Fatalf("MSISDN must win: %q", req.SubscriberID)
	}
}

func TestDecodeEventTimestampAndLimits(t *testing.T) {
	ok := msccOpts{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1)}}
	ntp := uint32(1_700_000_000 + ntpToUnix)
	req, _, derr := decode(t, ccrOpts{session: "s", typ: ccInitial, msisdn: "1", ntpTime: ntp, mscc: []msccOpts{ok}})
	if derr != nil || !req.EventTime.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("%v %v", req.EventTime, derr)
	}

	many := make([]msccOpts, 17)
	for i := range many {
		many[i] = msccOpts{rg: uint32(i + 1), hasRSU: true, rsu: []unitFn{totalOctets(1)}}
	}
	if _, _, derr := decode(t, ccrOpts{session: "s", typ: ccInitial, msisdn: "1", mscc: many}); derr == nil {
		t.Fatal("17 MSCC must be rejected (MaxMSCC=16)")
	}
}

func TestHugeUnitsAreFlaggedNotWrapped(t *testing.T) {
	req, _, derr := decode(t, ccrOpts{session: "s", typ: ccInitial, msisdn: "1",
		mscc: []msccOpts{{rg: 1, hasRSU: true, rsu: []unitFn{totalOctets(1 << 63)}}}})
	if derr != nil {
		t.Fatal(derr)
	}
	if got := req.Units[0].Requested; got <= datalayer.MaxUnits {
		t.Fatalf("a uint64 above range must stay above the limit so the data layer rejects it, got %d", got)
	}
}

func TestEncodeCCAShape(t *testing.T) {
	s, _ := NewServer(*testCfg(), raterFunc(nil), nil)
	req := &canonical.Request{Units: []canonical.UnitRequest{
		{RatingGroup: 100, Kind: canonical.UnitBytes, Requested: 1000},
		{RatingGroup: 200, Kind: canonical.UnitSeconds, Requested: 60},
		{RatingGroup: 300, Kind: canonical.UnitEvents, Requested: 1},
		{RatingGroup: 400, Kind: canonical.UnitBytes, Requested: 5},
	}}
	resp := &canonical.Response{Result: canonical.ResultGranted, Grants: []canonical.UnitGrant{
		{RatingGroup: 100, Result: canonical.ResultGranted, Granted: 1000, ValiditySeconds: 30},
		{RatingGroup: 200, Result: canonical.ResultPartialFinal, Granted: 40, ValiditySeconds: 30, FinalUnit: true},
		{RatingGroup: 300, Result: canonical.ResultGranted, Granted: 1, ValiditySeconds: 30},
		{RatingGroup: 400, Result: canonical.ResultCreditLimitReached},
	}}
	info := &ccrInfo{h: diameter.Header{Flags: diameter.FlagRequest | diameter.FlagProxiable, AppID: 4, HopByHop: 11, EndToEnd: 22},
		sessionID: []byte("sess"), reqType: ccUpdate, reqNo: 3, haveType: true, haveNo: true}
	var b diameter.Builder
	out, err := s.encodeCCA(&b, info, req, resp, diameter.ResultSuccess, "")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := diameter.ParseHeader(out)
	if h.IsRequest || h.Flags&diameter.FlagError != 0 || h.Flags&diameter.FlagProxiable == 0 || h.HopByHop != 11 || h.EndToEnd != 22 || h.Command != diameter.CmdCCR {
		t.Fatalf("%+v", h)
	}
	mscc := findAll(out[diameter.HeaderLen:], avpMSCC)
	if len(mscc) != 4 {
		t.Fatalf("%d MSCC", len(mscc))
	}
	gsu := func(i int) diameter.AVP {
		a, ok := find(append(make([]byte, diameter.HeaderLen), mscc[i].Data...), avpGrantedServiceUnit)
		if !ok {
			t.Fatalf("MSCC %d has no GSU", i)
		}
		inner, _, _ := diameter.NextAVP(a.Data)
		return inner
	}
	if in := gsu(0); in.Code != avpCCTotalOctets {
		t.Errorf("bytes must use CC-Total-Octets, got %d", in.Code)
	}
	if in := gsu(1); in.Code != avpCCTime {
		t.Errorf("seconds must use CC-Time, got %d", in.Code)
	} else if v, _ := in.Uint32(); v != 40 {
		t.Errorf("%d", v)
	}
	if in := gsu(2); in.Code != avpCCSpecificUnits {
		t.Errorf("events must use CC-Service-Specific-Units, got %d", in.Code)
	}
	wrap := func(i int) []byte { return append(make([]byte, diameter.HeaderLen), mscc[i].Data...) }
	if _, ok := find(wrap(1), avpFinalUnitIndication); !ok {
		t.Error("final unit indication missing on the partial grant")
	}
	if _, ok := find(wrap(0), avpFinalUnitIndication); ok {
		t.Error("final unit indication on a full grant")
	}
	if _, ok := find(wrap(3), avpGrantedServiceUnit); ok {
		t.Error("GSU on a refused unit")
	}
	if rc, ok := find(wrap(3), diameter.AVPResultCode); !ok {
		t.Error("per-unit Result-Code missing")
	} else if v, _ := rc.Uint32(); v != diameter.ResultCreditLimitReached {
		t.Errorf("per-unit result %d", v)
	}
}

func TestEventsOnlyAcceptDirectDebiting(t *testing.T) {
	ev := func(action *uint32) ccrOpts {
		return ccrOpts{session: "e1", typ: ccEvent, msisdn: "1", action: action,
			mscc: []msccOpts{{rg: 1, hasRSU: true, rsu: []unitFn{events(1)}}}}
	}
	act := func(v uint32) *uint32 { return &v }

	req, _, derr := decode(t, ev(act(0)))
	if derr != nil || req.Type != canonical.TypeEvent || req.Units[0].Kind != canonical.UnitEvents || req.Units[0].Requested != 1 {
		t.Fatalf("%+v %v", req, derr)
	}
	for _, a := range []uint32{1, 2, 3, 99} { // refund, check balance, price enquiry, unknown
		if _, _, derr := decode(t, ev(act(a))); derr == nil || derr.Result != diameter.ResultUnableToComply {
			t.Errorf("action %d must be refused with 5012, got %v", a, derr)
		}
	}
	if _, _, derr := decode(t, ev(nil)); derr == nil || derr.Result != diameter.ResultMissingAVP {
		t.Errorf("missing Requested-Action: %v", derr)
	}
}
