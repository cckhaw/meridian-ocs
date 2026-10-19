package cdr

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ber"
)

// Kind says how a field's bytes are decoded.
type Kind string

const (
	KindInt       Kind = "int"       // INTEGER
	KindString    Kind = "string"    // IA5String / UTF8String / octets as text
	KindTBCD      Kind = "tbcd"      // TBCD-STRING (swapped nibbles, filler 0xF)
	KindAddress   Kind = "address"   // AddressString: TON/NPI octet, then TBCD
	KindTimestamp Kind = "timestamp" // 3GPP TimeStamp: YYMMDDhhmmss[+-]hhmm in BCD
)

// FieldMap maps a context tag of the record SET to a normalized field.
type FieldMap struct {
	Tag   uint32 `json:"tag"`
	Field string `json:"field"` // chargingId, sequence, imsi, msisdn, apn, nodeId, openTime, duration, cause, ratType
	Kind  Kind   `json:"kind"`
}

// ContainerMap maps the list of service data containers.
type ContainerMap struct {
	ListTag     uint32 `json:"listTag"`     // the SEQUENCE OF container
	RatingGroup uint32 `json:"ratingGroup"` // tag inside a container
	Uplink      uint32 `json:"uplink"`
	Downlink    uint32 `json:"downlink"`
}

// Mapping tells the decoder how to read one record family.
type Mapping struct {
	// Types maps the top-level (context) tag of a record to its type name.
	Types      map[uint32]string `json:"types"`
	Fields     []FieldMap        `json:"fields"`
	Containers ContainerMap      `json:"containers"`
}

// PGWMapping is the default mapping for PGW-CDR (3GPP TS 32.298, PGWRecord).
//
// IMPORTANT: these tag numbers were written from the specification as
// remembered, not checked against operator sample files, and vendors differ in
// details. Treat them as a starting point: validate with real files from each
// network element and override with OCS_CDR_MAPPING (a JSON Mapping) where they
// differ.
func PGWMapping() Mapping {
	return Mapping{
		Types: map[uint32]string{79: "PGW"},
		Fields: []FieldMap{
			{3, "imsi", KindTBCD},
			{5, "chargingId", KindInt},
			{7, "apn", KindString},
			{13, "openTime", KindTimestamp},
			{14, "duration", KindInt},
			{15, "cause", KindInt},
			{17, "sequence", KindInt},
			{18, "nodeId", KindString},
			{22, "msisdn", KindAddress},
			{30, "ratType", KindInt},
		},
		Containers: ContainerMap{ListTag: 34, RatingGroup: 1, Uplink: 12, Downlink: 13},
	}
}

// FromBER converts one top-level BER element into a normalized record.
func FromBER(e *ber.Element, m *Mapping) (Record, error) {
	var r Record
	name, ok := m.Types[e.Tag.Number]
	if !ok || e.Tag.Class != ber.Context || !e.Constructed {
		return r, fmt.Errorf("%w: unknown record tag %d", ErrInvalid, e.Tag.Number)
	}
	r.Type = name
	for _, f := range m.Fields {
		c := e.Find(f.Tag)
		if c == nil {
			continue
		}
		if err := setField(&r, f, c); err != nil {
			return r, fmt.Errorf("%w: field %s (tag %d): %v", ErrInvalid, f.Field, f.Tag, err)
		}
	}
	if list := e.Find(m.Containers.ListTag); list != nil {
		for i := range list.Children {
			if len(r.Containers) >= MaxContainers {
				return r, fmt.Errorf("%w: more than %d containers", ErrInvalid, MaxContainers)
			}
			ct := &list.Children[i]
			var c Container
			if x := ct.Find(m.Containers.RatingGroup); x != nil {
				v, err := x.Uint()
				if err != nil || v > 0xffffffff {
					return r, fmt.Errorf("%w: container rating group", ErrInvalid)
				}
				c.RatingGroup = uint32(v)
			}
			var err error
			if c.Uplink, err = volume(ct.Find(m.Containers.Uplink)); err != nil {
				return r, fmt.Errorf("%w: container uplink: %v", ErrInvalid, err)
			}
			if c.Downlink, err = volume(ct.Find(m.Containers.Downlink)); err != nil {
				return r, fmt.Errorf("%w: container downlink: %v", ErrInvalid, err)
			}
			r.Containers = append(r.Containers, c)
		}
	}
	return r, r.Validate()
}

func volume(e *ber.Element) (int64, error) {
	if e == nil {
		return 0, nil
	}
	v, err := e.Uint()
	if err != nil {
		return 0, err
	}
	if v > 1<<53 {
		return 0, fmt.Errorf("volume %d too large", v)
	}
	return int64(v), nil
}

func setField(r *Record, f FieldMap, e *ber.Element) error {
	switch f.Kind {
	case KindInt:
		u, err := e.Uint()
		if err != nil {
			return err
		}
		switch f.Field {
		case "chargingId":
			r.ChargingID = u
		case "sequence":
			r.Sequence = int64(u)
		case "duration":
			r.Duration = int64(u)
		case "cause":
			r.Cause = strconv.FormatUint(u, 10)
		case "ratType":
			r.RATType = strconv.FormatUint(u, 10)
		default:
			return fmt.Errorf("field %q is not numeric", f.Field)
		}
	case KindString:
		s := string(e.Value)
		switch f.Field {
		case "apn":
			r.APN = s
		case "nodeId":
			r.NodeID = s
		default:
			return fmt.Errorf("field %q is not text", f.Field)
		}
	case KindTBCD, KindAddress:
		b := e.Value
		if f.Kind == KindAddress {
			if len(b) < 1 {
				return fmt.Errorf("empty address")
			}
			b = b[1:]
		}
		d, err := TBCD(b)
		if err != nil {
			return err
		}
		switch f.Field {
		case "imsi":
			r.IMSI = d
		case "msisdn":
			r.MSISDN = d
		default:
			return fmt.Errorf("field %q is not an identifier", f.Field)
		}
	case KindTimestamp:
		t, err := Timestamp(e.Value)
		if err != nil {
			return err
		}
		if f.Field != "openTime" {
			return fmt.Errorf("field %q is not a time", f.Field)
		}
		r.OpenTime = t
	default:
		return fmt.Errorf("unknown kind %q", f.Kind)
	}
	return nil
}

// TBCD decodes a telephony BCD string: two digits per octet, low nibble first,
// 0xF as filler for an odd length.
func TBCD(b []byte) (string, error) {
	out := make([]byte, 0, len(b)*2)
	for i, c := range b {
		for j, n := range [2]byte{c & 0x0f, c >> 4} {
			switch {
			case n <= 9:
				out = append(out, '0'+n)
			case n == 0x0f && i == len(b)-1 && j == 1:
				// trailing filler
			default:
				return "", fmt.Errorf("invalid TBCD digit 0x%x", n)
			}
		}
	}
	return string(out), nil
}

func bcd(b byte) (int, error) {
	hi, lo := int(b>>4), int(b&0x0f)
	if hi > 9 || lo > 9 {
		return 0, fmt.Errorf("invalid BCD octet 0x%02x", b)
	}
	return hi*10 + lo, nil
}

// Timestamp decodes the 3GPP TimeStamp octet string: nine octets
// YY MM DD hh mm ss S hh mm, the first six and the last two BCD (tens digit in
// the high nibble) and S an ASCII '+' or '-' giving the local offset from UTC.
// The result is converted to UTC. Two-digit years are 2000-2099.
func Timestamp(b []byte) (time.Time, error) {
	if len(b) != 9 {
		return time.Time{}, fmt.Errorf("timestamp must be 9 octets, got %d", len(b))
	}
	var v [9]int
	for _, i := range []int{0, 1, 2, 3, 4, 5, 7, 8} {
		n, err := bcd(b[i])
		if err != nil {
			return time.Time{}, err
		}
		v[i] = n
	}
	sign := 1
	switch b[6] {
	case '+':
	case '-':
		sign = -1
	default:
		return time.Time{}, fmt.Errorf("invalid offset sign 0x%02x", b[6])
	}
	if v[1] < 1 || v[1] > 12 || v[2] < 1 || v[2] > 31 || v[3] > 23 || v[4] > 59 || v[5] > 59 || v[7] > 23 || v[8] > 59 {
		return time.Time{}, fmt.Errorf("timestamp out of range")
	}
	local := time.Date(2000+v[0], time.Month(v[1]), v[2], v[3], v[4], v[5], 0, time.UTC)
	if local.Day() != v[2] { // e.g. 31 February rolled over
		return time.Time{}, fmt.Errorf("timestamp is not a real date")
	}
	return local.Add(-time.Duration(sign) * (time.Duration(v[7])*time.Hour + time.Duration(v[8])*time.Minute)), nil
}
