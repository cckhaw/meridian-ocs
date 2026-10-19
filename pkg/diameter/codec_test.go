package diameter

import (
	"bytes"
	"net"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var b Builder
	b.StartMessage(FlagRequest|FlagProxiable, CmdCCR, AppCreditControl, 0xAABBCCDD, 0x11223344)
	b.String(AVPSessionID, AVPFlagMandatory, "pgw;1;2") // 7 bytes: needs padding
	b.Uint32(AVPResultCode, AVPFlagMandatory, 2001)
	g := b.BeginGrouped(456, AVPFlagMandatory)
	b.Uint64(421, AVPFlagMandatory, 1<<40)
	b.EndGrouped(g)
	b.Address(AVPHostIPAddress, AVPFlagMandatory, net.ParseIP("10.1.2.3"))
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(msg)%4 != 0 {
		t.Fatalf("length %d not a multiple of 4", len(msg))
	}

	h, err := ParseHeader(msg)
	if err != nil || h.Length != uint32(len(msg)) || !h.IsRequest || h.Command != CmdCCR ||
		h.AppID != AppCreditControl || h.HopByHop != 0xAABBCCDD || h.EndToEnd != 0x11223344 {
		t.Fatalf("%+v %v", h, err)
	}

	rest := msg[HeaderLen:]
	var seen []uint32
	for len(rest) > 0 {
		var a AVP
		a, rest, err = NextAVP(rest)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, a.Code)
		switch a.Code {
		case AVPSessionID:
			if string(a.Data) != "pgw;1;2" {
				t.Fatalf("%q", a.Data)
			}
		case AVPResultCode:
			if v, _ := a.Uint32(); v != 2001 {
				t.Fatal(v)
			}
		case 456:
			inner, _, err := NextAVP(a.Data)
			if err != nil {
				t.Fatal(err)
			}
			if v, err := inner.Uint64(); err != nil || v != 1<<40 {
				t.Fatal(v, err)
			}
		case AVPHostIPAddress:
			if !bytes.Equal(a.Data, []byte{0, 1, 10, 1, 2, 3}) {
				t.Fatalf("%v", a.Data)
			}
		}
	}
	if len(seen) != 4 {
		t.Fatalf("%v", seen)
	}
}

func TestVendorAVP(t *testing.T) {
	// 3GPP-RAT-Type (21), vendor 10415, one data byte plus padding.
	raw := []byte{0, 0, 0, 21, AVPFlagVendor, 0, 0, 13, 0, 0, 0x28, 0xAF, 6, 0, 0, 0}
	a, rest, err := NextAVP(raw)
	if err != nil || a.Code != 21 || a.Vendor != Vendor3GPP || len(a.Data) != 1 || a.Data[0] != 6 || len(rest) != 0 {
		t.Fatalf("%+v %v %v", a, rest, err)
	}
}

func TestMalformed(t *testing.T) {
	good := func() []byte {
		var b Builder
		b.StartMessage(FlagRequest, CmdDWR, 0, 1, 2)
		b.String(AVPOriginHost, AVPFlagMandatory, "host")
		m, _ := b.Finish()
		return append([]byte(nil), m...)
	}
	if _, err := ParseHeader(good()[:10]); err != ErrShort {
		t.Error(err)
	}
	m := good()
	m[0] = 2
	if _, err := ParseHeader(m); err != ErrVersion {
		t.Error(err)
	}
	m = good()
	m[3] = 21 // not a multiple of 4
	if _, err := ParseHeader(m); err != ErrLength {
		t.Error(err)
	}
	m = good()
	m[3] = 4 // shorter than the header
	if _, err := ParseHeader(m); err != ErrLength {
		t.Error(err)
	}
	// AVP claiming more bytes than are present.
	body := good()[HeaderLen:]
	body[7] = 200
	if _, _, err := NextAVP(body); err != ErrAVPLength {
		t.Error(err)
	}
	// AVP shorter than its own header.
	body = good()[HeaderLen:]
	body[7] = 4
	if _, _, err := NextAVP(body); err != ErrAVPLength {
		t.Error(err)
	}
	if _, _, err := NextAVP([]byte{1, 2, 3}); err != ErrAVPLength {
		t.Error(err)
	}
	if _, err := (AVP{Data: []byte{1, 2, 3}}).Uint32(); err != ErrAVPData {
		t.Error(err)
	}
}

func TestParsingDoesNotAllocate(t *testing.T) {
	var b Builder
	b.StartMessage(FlagRequest, CmdCCR, AppCreditControl, 1, 2)
	for i := 0; i < 10; i++ {
		b.Uint32(uint32(400+i), AVPFlagMandatory, uint32(i))
	}
	msg, _ := b.Finish()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := ParseHeader(msg); err != nil {
			t.Fatal(err)
		}
		rest := msg[HeaderLen:]
		for len(rest) > 0 {
			var a AVP
			var err error
			a, rest, err = NextAVP(rest)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = a.Uint32()
		}
	})
	if allocs != 0 {
		t.Fatalf("%v allocs", allocs)
	}
}

func FuzzParse(f *testing.F) {
	var b Builder
	b.StartMessage(FlagRequest, CmdCCR, AppCreditControl, 1, 2)
	b.String(AVPSessionID, AVPFlagMandatory, "abc")
	g := b.BeginGrouped(456, AVPFlagMandatory)
	b.Uint64(421, AVPFlagMandatory, 5)
	b.EndGrouped(g)
	m, _ := b.Finish()
	f.Add(m)
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 40))
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := ParseHeader(data); err != nil || len(data) < HeaderLen {
			return
		}
		var walk func([]byte, int)
		walk = func(b []byte, depth int) {
			for len(b) > 0 {
				a, rest, err := NextAVP(b)
				if err != nil {
					return
				}
				if depth < 4 {
					walk(a.Data, depth+1)
				}
				b = rest
			}
		}
		walk(data[HeaderLen:], 0)
	})
}
