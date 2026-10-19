package gy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
)

type raterFunc func(ctx context.Context, req *canonical.Request, resp *canonical.Response) error

func (f raterFunc) Rate(ctx context.Context, req *canonical.Request, resp *canonical.Response) error {
	return f(ctx, req, resp)
}

// ---- CCR construction ------------------------------------------------------

type unitFn func(b *diameter.Builder)

func totalOctets(n uint64) unitFn {
	return func(b *diameter.Builder) { b.Uint64(avpCCTotalOctets, diameter.AVPFlagMandatory, n) }
}
func inOutOctets(in, out uint64) unitFn {
	return func(b *diameter.Builder) {
		b.Uint64(avpCCInputOctets, diameter.AVPFlagMandatory, in)
		b.Uint64(avpCCOutputOctets, diameter.AVPFlagMandatory, out)
	}
}
func ccTime(n uint32) unitFn {
	return func(b *diameter.Builder) { b.Uint32(avpCCTime, diameter.AVPFlagMandatory, n) }
}
func events(n uint64) unitFn {
	return func(b *diameter.Builder) { b.Uint64(avpCCSpecificUnits, diameter.AVPFlagMandatory, n) }
}

type msccOpts struct {
	rg     uint32
	sid    uint32
	hasRSU bool
	rsu    []unitFn // empty with hasRSU => empty Requested-Service-Unit
	usus   [][]unitFn
}

type ccrOpts struct {
	session string
	typ     uint32 // 0 = omit
	no      uint32
	noNo    bool
	msisdn  string
	imsi    string
	app     uint32 // default 4
	mscc    []msccOpts
	rat     int // 3GPP-RAT-Type value, used when ratSet
	ratSet  bool
	ntpTime uint32
	action  *uint32 // Requested-Action for events
	hbh     uint32
	e2e     uint32
}

func buildCCR(o ccrOpts) []byte {
	app := o.app
	if app == 0 {
		app = diameter.AppCreditControl
	}
	hbh, e2e := o.hbh, o.e2e
	if hbh == 0 {
		hbh = 0x1000 + o.no
	}
	if e2e == 0 {
		e2e = 0x2000 + o.no
	}
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest|diameter.FlagProxiable, diameter.CmdCCR, app, hbh, e2e)
	if o.session != "" {
		b.String(diameter.AVPSessionID, diameter.AVPFlagMandatory, o.session)
	}
	b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, "pgw.example.net")
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, "example.net")
	b.String(diameter.AVPDestinationRealm, diameter.AVPFlagMandatory, "ocs.example.net")
	b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, diameter.AppCreditControl)
	if o.typ != 0 {
		b.Uint32(avpCCType, diameter.AVPFlagMandatory, o.typ)
	}
	if !o.noNo {
		b.Uint32(avpCCNumber, diameter.AVPFlagMandatory, o.no)
	}
	if o.action != nil {
		b.Uint32(avpRequestedAction, diameter.AVPFlagMandatory, *o.action)
	}
	if o.ntpTime != 0 {
		b.Uint32(diameter.AVPEventTimestamp, diameter.AVPFlagMandatory, o.ntpTime)
	}
	sub := func(typ uint32, v string) {
		g := b.BeginGrouped(avpSubscriptionID, diameter.AVPFlagMandatory)
		b.Uint32(avpSubscriptionIDType, diameter.AVPFlagMandatory, typ)
		b.String(avpSubscriptionIDData, diameter.AVPFlagMandatory, v)
		b.EndGrouped(g)
	}
	if o.imsi != "" {
		sub(subIMSI, o.imsi)
	}
	if o.msisdn != "" {
		sub(subE164, o.msisdn)
	}
	for _, m := range o.mscc {
		g := b.BeginGrouped(avpMSCC, diameter.AVPFlagMandatory)
		if m.rg != 0 {
			b.Uint32(avpRatingGroup, diameter.AVPFlagMandatory, m.rg)
		}
		if m.sid != 0 {
			b.Uint32(avpServiceIdentifier, diameter.AVPFlagMandatory, m.sid)
		}
		if m.hasRSU {
			r := b.BeginGrouped(avpRequestedServiceUnit, diameter.AVPFlagMandatory)
			for _, f := range m.rsu {
				f(&b)
			}
			b.EndGrouped(r)
		}
		for _, u := range m.usus {
			r := b.BeginGrouped(avpUsedServiceUnit, diameter.AVPFlagMandatory)
			for _, f := range u {
				f(&b)
			}
			b.EndGrouped(r)
		}
		b.EndGrouped(g)
	}
	if o.ratSet {
		si := b.BeginGroupedVendor(avpServiceInformation, diameter.AVPFlagMandatory, diameter.Vendor3GPP)
		ps := b.BeginGroupedVendor(avpPSInformation, diameter.AVPFlagMandatory, diameter.Vendor3GPP)
		b.VendorBytes(avp3GPPRATType, diameter.AVPFlagMandatory, diameter.Vendor3GPP, []byte{byte(o.rat)})
		b.EndGrouped(ps)
		b.EndGrouped(si)
	}
	msg, _ := b.Finish()
	return append([]byte(nil), msg...)
}

// ---- test peer -------------------------------------------------------------

type peer struct {
	t *testing.T
	c net.Conn
}

func dial(t *testing.T, addr string) *peer {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &peer{t: t, c: c}
}

func (p *peer) send(msg []byte) {
	p.t.Helper()
	if _, err := p.c.Write(msg); err != nil {
		p.t.Fatal(err)
	}
}

// recv reads one message; ok is false if the peer closed the connection.
func (p *peer) recv() (h diameter.Header, msg []byte, ok bool) {
	p.t.Helper()
	_ = p.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var hdr [diameter.HeaderLen]byte
	if _, err := io.ReadFull(p.c, hdr[:]); err != nil {
		return h, nil, false
	}
	h, err := diameter.ParseHeader(hdr[:])
	if err != nil {
		p.t.Fatal(err)
	}
	msg = make([]byte, h.Length)
	copy(msg, hdr[:])
	if _, err := io.ReadFull(p.c, msg[diameter.HeaderLen:]); err != nil {
		p.t.Fatal(err)
	}
	return h, msg, true
}

func (p *peer) mustRecv() (diameter.Header, []byte) {
	p.t.Helper()
	h, m, ok := p.recv()
	if !ok {
		p.t.Fatal("connection closed, expected a message")
	}
	return h, m
}

func (p *peer) expectClosed() {
	p.t.Helper()
	if _, _, ok := p.recv(); ok {
		p.t.Fatal("expected the connection to be closed")
	}
}

func buildCER(app uint32, withHost bool) []byte {
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest, diameter.CmdCER, diameter.AppBase, 7, 8)
	if withHost {
		b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, "pgw.example.net")
	}
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, "example.net")
	b.Address(diameter.AVPHostIPAddress, diameter.AVPFlagMandatory, net.ParseIP("10.0.0.1"))
	b.Uint32(diameter.AVPVendorID, diameter.AVPFlagMandatory, 0)
	b.String(diameter.AVPProductName, 0, "pgw")
	if app != 0 {
		b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, app)
	}
	m, _ := b.Finish()
	return append([]byte(nil), m...)
}

func buildSimple(cmd uint32, hbh, e2e uint32) []byte {
	var b diameter.Builder
	b.StartMessage(diameter.FlagRequest, cmd, diameter.AppBase, hbh, e2e)
	b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, "pgw.example.net")
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, "example.net")
	m, _ := b.Finish()
	return append([]byte(nil), m...)
}

// find returns the first top-level AVP with the code.
func find(msg []byte, code uint32) (diameter.AVP, bool) {
	data := msg[diameter.HeaderLen:]
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			return diameter.AVP{}, false
		}
		if a.Code == code && a.Flags&diameter.AVPFlagVendor == 0 {
			return a, true
		}
		data = rest
	}
	return diameter.AVP{}, false
}

func findAll(data []byte, code uint32) []diameter.AVP {
	var out []diameter.AVP
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			break
		}
		if a.Code == code {
			out = append(out, a)
		}
		data = rest
	}
	return out
}

func u32(t *testing.T, msg []byte, code uint32) uint32 {
	t.Helper()
	a, ok := find(msg, code)
	if !ok {
		t.Fatalf("AVP %d missing", code)
	}
	v, err := a.Uint32()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func startServer(t *testing.T, rater Rater, adm *Admission, mod func(*Config)) (*Server, string) {
	t.Helper()
	cfg := Config{OriginHost: "ocs.example.net", OriginRealm: "example.net", HostIP: net.ParseIP("10.9.9.9"),
		WatchdogInterval: 30 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	s, err := NewServer(cfg, rater, adm)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })
	return s, ln.Addr().String()
}

// openPeer dials and completes the capabilities exchange.
func openPeer(t *testing.T, addr string) *peer {
	t.Helper()
	p := dial(t, addr)
	p.send(buildCER(diameter.AppCreditControl, true))
	_, cea := p.mustRecv()
	if got := u32(t, cea, diameter.AVPResultCode); got != diameter.ResultSuccess {
		t.Fatalf("CER failed: %d", got)
	}
	return p
}
