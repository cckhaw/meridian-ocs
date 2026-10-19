// Package diameter is a minimal RFC 6733 message codec: header and AVP
// parsing without allocation (AVP data aliases the message buffer) and an
// append-style builder for answers. It knows nothing about applications; see
// the gy package for Credit-Control.
package diameter

import (
	"encoding/binary"
	"errors"
	"net"
)

const (
	Version   = 1
	HeaderLen = 20

	FlagRequest   = 0x80
	FlagProxiable = 0x40
	FlagError     = 0x20
	FlagRetrans   = 0x10

	AVPFlagVendor    = 0x80
	AVPFlagMandatory = 0x40
)

// Commands.
const (
	CmdCER = 257
	CmdCCR = 272
	CmdDWR = 280
	CmdDPR = 282
)

// Applications.
const (
	AppBase          = 0
	AppCreditControl = 4
)

// Vendor ids.
const Vendor3GPP = 10415

// Base AVP codes.
const (
	AVPEventTimestamp      = 55
	AVPHostIPAddress       = 257
	AVPAuthApplicationID   = 258
	AVPVendorSpecificAppID = 260
	AVPSessionID           = 263
	AVPOriginHost          = 264
	AVPSupportedVendorID   = 265
	AVPVendorID            = 266
	AVPFirmwareRevision    = 267
	AVPResultCode          = 268
	AVPProductName         = 269
	AVPDisconnectCause     = 273
	AVPOriginStateID       = 278
	AVPErrorMessage        = 281
	AVPDestinationRealm    = 283
	AVPOriginRealm         = 296
)

// Result codes (RFC 6733 §7.1, RFC 4006 §9).
const (
	ResultSuccess             = 2001
	ResultCommandUnsupported  = 3001
	ResultUnableToDeliver     = 3002
	ResultApplicationUnsup    = 3007
	ResultTooBusy             = 3004
	ResultEndUserDenied       = 4010
	ResultCreditLimitReached  = 4012
	ResultAuthRejected        = 5003
	ResultUnknownSessionID    = 5002
	ResultInvalidAVPValue     = 5004
	ResultMissingAVP          = 5005
	ResultInvalidAVPLength    = 5014
	ResultNoCommonApplication = 5010
	ResultUnableToComply      = 5012
	ResultRatingFailed        = 5031
)

var (
	ErrShort      = errors.New("diameter: short buffer")
	ErrVersion    = errors.New("diameter: bad version")
	ErrLength     = errors.New("diameter: bad length")
	ErrAVPLength  = errors.New("diameter: bad AVP length")
	ErrAVPData    = errors.New("diameter: bad AVP data")
	ErrTooLarge   = errors.New("diameter: message too large")
	errNeedHeader = errors.New("diameter: builder has no header")
)

// Header is the fixed 20-byte message header.
type Header struct {
	Length    uint32
	Flags     byte
	Command   uint32
	AppID     uint32
	HopByHop  uint32
	EndToEnd  uint32
	IsRequest bool
}

// ParseHeader decodes and sanity-checks the header at the start of b.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderLen {
		return Header{}, ErrShort
	}
	if b[0] != Version {
		return Header{}, ErrVersion
	}
	h := Header{
		Length:   uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]),
		Flags:    b[4],
		Command:  uint32(b[5])<<16 | uint32(b[6])<<8 | uint32(b[7]),
		AppID:    binary.BigEndian.Uint32(b[8:]),
		HopByHop: binary.BigEndian.Uint32(b[12:]),
		EndToEnd: binary.BigEndian.Uint32(b[16:]),
	}
	h.IsRequest = h.Flags&FlagRequest != 0
	if h.Length < HeaderLen || h.Length%4 != 0 {
		return Header{}, ErrLength
	}
	return h, nil
}

// AVP is one attribute. Data aliases the buffer it was parsed from.
type AVP struct {
	Code   uint32
	Flags  byte
	Vendor uint32
	Data   []byte
}

// NextAVP parses the first AVP in b and returns the remainder.
func NextAVP(b []byte) (a AVP, rest []byte, err error) {
	if len(b) < 8 {
		return a, nil, ErrAVPLength
	}
	a.Code = binary.BigEndian.Uint32(b)
	a.Flags = b[4]
	n := int(b[5])<<16 | int(b[6])<<8 | int(b[7])
	hdr := 8
	if a.Flags&AVPFlagVendor != 0 {
		hdr = 12
		if len(b) < 12 {
			return a, nil, ErrAVPLength
		}
		a.Vendor = binary.BigEndian.Uint32(b[8:])
	}
	if n < hdr || n > len(b) {
		return a, nil, ErrAVPLength
	}
	a.Data = b[hdr:n:n]
	padded := (n + 3) &^ 3
	if padded > len(b) {
		padded = len(b)
	}
	return a, b[padded:], nil
}

// Uint32 decodes a 4-byte unsigned (or Enumerated/Integer32) value.
func (a AVP) Uint32() (uint32, error) {
	if len(a.Data) != 4 {
		return 0, ErrAVPData
	}
	return binary.BigEndian.Uint32(a.Data), nil
}

// Uint64 decodes an 8-byte unsigned value.
func (a AVP) Uint64() (uint64, error) {
	if len(a.Data) != 8 {
		return 0, ErrAVPData
	}
	return binary.BigEndian.Uint64(a.Data), nil
}

// Builder appends a message into a reusable buffer.
type Builder struct{ B []byte }

// Reset empties the builder, keeping capacity.
func (b *Builder) Reset() { b.B = b.B[:0] }

// StartMessage writes a header with a zero length; call Finish when done.
func (b *Builder) StartMessage(flags byte, cmd, app, hopByHop, endToEnd uint32) {
	b.B = append(b.B[:0], Version, 0, 0, 0, flags, byte(cmd>>16), byte(cmd>>8), byte(cmd))
	b.B = binary.BigEndian.AppendUint32(b.B, app)
	b.B = binary.BigEndian.AppendUint32(b.B, hopByHop)
	b.B = binary.BigEndian.AppendUint32(b.B, endToEnd)
}

// Finish patches the message length and returns the encoded message.
func (b *Builder) Finish() ([]byte, error) {
	if len(b.B) < HeaderLen {
		return nil, errNeedHeader
	}
	n := len(b.B)
	if n > 0xFFFFFF {
		return nil, ErrTooLarge
	}
	b.B[1], b.B[2], b.B[3] = byte(n>>16), byte(n>>8), byte(n)
	return b.B, nil
}

func (b *Builder) avpHeader(code uint32, flags byte) int {
	start := len(b.B)
	b.B = binary.BigEndian.AppendUint32(b.B, code)
	b.B = append(b.B, flags, 0, 0, 0)
	return start
}

func (b *Builder) endAVP(start int) {
	n := len(b.B) - start
	b.B[start+5], b.B[start+6], b.B[start+7] = byte(n>>16), byte(n>>8), byte(n)
	for len(b.B)%4 != 0 {
		b.B = append(b.B, 0)
	}
}

// Uint32 appends a 4-byte AVP.
func (b *Builder) Uint32(code uint32, flags byte, v uint32) {
	s := b.avpHeader(code, flags)
	b.B = binary.BigEndian.AppendUint32(b.B, v)
	b.endAVP(s)
}

// Uint64 appends an 8-byte AVP.
func (b *Builder) Uint64(code uint32, flags byte, v uint64) {
	s := b.avpHeader(code, flags)
	b.B = binary.BigEndian.AppendUint64(b.B, v)
	b.endAVP(s)
}

// Bytes appends an OctetString/UTF8String AVP.
func (b *Builder) Bytes(code uint32, flags byte, v []byte) {
	s := b.avpHeader(code, flags)
	b.B = append(b.B, v...)
	b.endAVP(s)
}

// String appends a string AVP.
func (b *Builder) String(code uint32, flags byte, v string) {
	s := b.avpHeader(code, flags)
	b.B = append(b.B, v...)
	b.endAVP(s)
}

// Address appends an Address-typed AVP (RFC 6733 §4.3.1).
func (b *Builder) Address(code uint32, flags byte, ip net.IP) {
	s := b.avpHeader(code, flags)
	if v4 := ip.To4(); v4 != nil {
		b.B = append(b.B, 0, 1)
		b.B = append(b.B, v4...)
	} else {
		b.B = append(b.B, 0, 2)
		b.B = append(b.B, ip.To16()...)
	}
	b.endAVP(s)
}

// BeginGrouped starts a Grouped AVP and returns a token for EndGrouped.
func (b *Builder) BeginGrouped(code uint32, flags byte) int { return b.avpHeader(code, flags) }

// EndGrouped closes the Grouped AVP opened at start.
func (b *Builder) EndGrouped(start int) { b.endAVP(start) }

func (b *Builder) avpHeaderVendor(code uint32, flags byte, vendor uint32) int {
	start := len(b.B)
	b.B = binary.BigEndian.AppendUint32(b.B, code)
	b.B = append(b.B, flags|AVPFlagVendor, 0, 0, 0)
	b.B = binary.BigEndian.AppendUint32(b.B, vendor)
	return start
}

// BeginGroupedVendor starts a vendor-specific Grouped AVP; close it with EndGrouped.
func (b *Builder) BeginGroupedVendor(code uint32, flags byte, vendor uint32) int {
	return b.avpHeaderVendor(code, flags, vendor)
}

// VendorBytes appends a vendor-specific OctetString AVP.
func (b *Builder) VendorBytes(code uint32, flags byte, vendor uint32, v []byte) {
	s := b.avpHeaderVendor(code, flags, vendor)
	b.B = append(b.B, v...)
	b.endAVP(s)
}
