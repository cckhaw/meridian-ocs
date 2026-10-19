// Package cdrtest builds BER-encoded charging records for tests. It encodes
// what the decoder in package cdr is written to read; it is a round-trip aid,
// not an independent check of the 3GPP tag assignments.
package cdrtest

import "bytes"

// TLV encodes one element. class: 0 universal, 1 application, 2 context, 3 private.
func TLV(class byte, constructed bool, number uint32, content []byte) []byte {
	var out []byte
	first := class << 6
	if constructed {
		first |= 0x20
	}
	if number < 31 {
		out = append(out, first|byte(number))
	} else {
		out = append(out, first|0x1f)
		var tmp []byte
		for n := number; n > 0; n >>= 7 {
			tmp = append([]byte{byte(n & 0x7f)}, tmp...)
		}
		for i := range tmp[:len(tmp)-1] {
			tmp[i] |= 0x80
		}
		out = append(out, tmp...)
	}
	switch n := len(content); {
	case n < 128:
		out = append(out, byte(n))
	case n < 256:
		out = append(out, 0x81, byte(n))
	default:
		out = append(out, 0x82, byte(n>>8), byte(n))
	}
	return append(out, content...)
}

// Prim is a primitive context-class element.
func Prim(n uint32, v ...byte) []byte { return TLV(2, false, n, v) }

// Cons is a constructed context-class element.
func Cons(n uint32, parts ...[]byte) []byte { return TLV(2, true, n, bytes.Join(parts, nil)) }

// BE is a minimal unsigned big-endian integer (with a leading zero if the top bit is set).
func BE(v uint64) []byte {
	var b []byte
	for ; v > 0; v >>= 8 {
		b = append([]byte{byte(v)}, b...)
	}
	if len(b) == 0 {
		return []byte{0}
	}
	if b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	return b
}

// TBCD encodes digits as telephony BCD (low nibble first, 0xF filler).
func TBCD(d string) []byte {
	var out []byte
	for i := 0; i < len(d); i += 2 {
		lo := d[i] - '0'
		hi := byte(0x0f)
		if i+1 < len(d) {
			hi = d[i+1] - '0'
		}
		out = append(out, hi<<4|lo)
	}
	return out
}

// Stamp encodes the 3GPP TimeStamp octets.
func Stamp(y, mo, d, h, mi, s int, sign byte, oh, om int) []byte {
	b := func(n int) byte { return byte(n/10<<4 | n%10) }
	return []byte{b(y), b(mo), b(d), b(h), b(mi), b(s), sign, b(oh), b(om)}
}

// PGW describes a PGW record to encode.
type PGW struct {
	IMSI, MSISDN, Node, APN string
	ChargingID              uint64
	Sequence                uint64
	Y, Mo, D, H, Mi, S      int // opening time, UTC
	Duration                uint64
	Containers              [][3]uint64 // rating group, uplink, downlink
}

// Encode returns the record as a top-level [79] element.
func (p PGW) Encode() []byte {
	parts := [][]byte{
		Prim(3, TBCD(p.IMSI)...),
		Prim(5, BE(p.ChargingID)...),
		Prim(13, Stamp(p.Y, p.Mo, p.D, p.H, p.Mi, p.S, '+', 0, 0)...),
		Prim(14, BE(p.Duration)...),
		Prim(17, BE(p.Sequence)...),
		Prim(18, []byte(p.Node)...),
	}
	if p.APN != "" {
		parts = append(parts, Prim(7, []byte(p.APN)...))
	}
	if p.MSISDN != "" {
		parts = append(parts, Prim(22, append([]byte{0x91}, TBCD(p.MSISDN)...)...))
	}
	if len(p.Containers) > 0 {
		var list [][]byte
		for _, c := range p.Containers {
			list = append(list, Cons(0, Prim(1, BE(c[0])...), Prim(12, BE(c[1])...), Prim(13, BE(c[2])...)))
		}
		parts = append(parts, Cons(34, list...))
	}
	return Cons(79, parts...)
}
