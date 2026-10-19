package cdr

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ber"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr/cdrtest"
)

func pgw(extra ...[]byte) []byte {
	parts := [][]byte{
		cdrtest.Prim(3, cdrtest.TBCD("262011234567890")...),
		cdrtest.Prim(5, cdrtest.BE(0x01020304)...),
		cdrtest.Prim(7, []byte("internet.example")...),
		cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 14, 30, 0, '+', 2, 0)...),
		cdrtest.Prim(14, cdrtest.BE(600)...),
		cdrtest.Prim(17, cdrtest.BE(7)...),
		cdrtest.Prim(18, []byte("pgw-1")...),
		cdrtest.Prim(22, append([]byte{0x91}, cdrtest.TBCD("491701234567")...)...),
		cdrtest.Cons(34,
			cdrtest.Cons(0, cdrtest.Prim(1, cdrtest.BE(100)...), cdrtest.Prim(12, cdrtest.BE(1000)...), cdrtest.Prim(13, cdrtest.BE(5000)...)),
			cdrtest.Cons(0, cdrtest.Prim(1, cdrtest.BE(200)...), cdrtest.Prim(12, cdrtest.BE(1)...), cdrtest.Prim(13, cdrtest.BE(2)...))),
	}
	return cdrtest.Cons(79, append(parts, extra...)...)
}

func decode(t *testing.T, b []byte) (Record, error) {
	t.Helper()
	e, err := ber.NewReader(bytes.NewReader(b)).Next()
	if err != nil {
		t.Fatal(err)
	}
	m := PGWMapping()
	return FromBER(&e, &m)
}

func TestPGWRecordFromBER(t *testing.T) {
	r, err := decode(t, pgw())
	if err != nil {
		t.Fatal(err)
	}
	if r.Type != "PGW" || r.IMSI != "262011234567890" || r.MSISDN != "491701234567" || r.NodeID != "pgw-1" ||
		r.APN != "internet.example" || r.ChargingID != 0x01020304 || r.Sequence != 7 || r.Duration != 600 {
		t.Fatalf("%+v", r)
	}
	// 14:30 at +02:00 is 12:30 UTC.
	if want := time.Date(2030, 6, 1, 12, 30, 0, 0, time.UTC); !r.OpenTime.Equal(want) {
		t.Fatalf("open time %v, want %v", r.OpenTime, want)
	}
	if len(r.Containers) != 2 || r.Containers[0] != (Container{RatingGroup: 100, Uplink: 1000, Downlink: 5000}) {
		t.Fatalf("%+v", r.Containers)
	}
	if up, down := r.Totals(); up != 1001 || down != 5002 {
		t.Fatalf("%d %d", up, down)
	}
}

func TestBERErrors(t *testing.T) {
	// unknown record type
	e, _ := ber.NewReader(bytes.NewReader(cdrtest.Cons(80, cdrtest.Prim(3, cdrtest.TBCD("1")...)))).Next()
	m := PGWMapping()
	if _, err := FromBER(&e, &m); !errors.Is(err, ErrInvalid) {
		t.Fatalf("%v", err)
	}
	bad := map[string][]byte{
		"bad TBCD digit":   cdrtest.Cons(79, cdrtest.Prim(3, 0xaa), cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n')),
		"short timestamp":  cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, 1, 2, 3), cdrtest.Prim(18, 'n')),
		"february 31st":    cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, cdrtest.Stamp(30, 2, 31, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n')),
		"bad sign":         cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, 'x', 0, 0)...), cdrtest.Prim(18, 'n')),
		"month 13":         cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, cdrtest.Stamp(30, 13, 1, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n')),
		"no identity":      cdrtest.Cons(79, cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n')),
		"no node":          cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, '+', 0, 0)...)),
		"no time":          cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(18, 'n')),
		"huge counter":     cdrtest.Cons(79, cdrtest.Prim(3, cdrtest.TBCD("123")...), cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n'), cdrtest.Prim(14, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
		"negative address": cdrtest.Cons(79, cdrtest.Prim(22), cdrtest.Prim(13, cdrtest.Stamp(30, 6, 1, 0, 0, 0, '+', 0, 0)...), cdrtest.Prim(18, 'n')),
	}
	for name, b := range bad {
		e, err := ber.NewReader(bytes.NewReader(b)).Next()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := FromBER(&e, &m); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestTBCDOddAndEven(t *testing.T) {
	for _, d := range []string{"1", "12", "123", "0000", "49170"} {
		got, err := TBCD(cdrtest.TBCD(d))
		if err != nil || got != d {
			t.Errorf("%q -> %q %v", d, got, err)
		}
	}
	if _, err := TBCD([]byte{0xf1, 0x21}); err == nil { // filler in the middle
		t.Error("a filler nibble before the end was accepted")
	}
}

func TestTimestampNegativeOffset(t *testing.T) {
	got, err := Timestamp(cdrtest.Stamp(30, 1, 1, 0, 30, 0, '-', 5, 30))
	if err != nil || !got.Equal(time.Date(2030, 1, 1, 6, 0, 0, 0, time.UTC)) {
		t.Fatalf("%v %v", got, err)
	}
}

const goodJSON = `{"type":"PGW","nodeId":"pgw-1","chargingId":1,"sequence":2,"imsi":"262011234567890","openTime":"2030-06-01T12:30:00Z","durationSeconds":60,"containers":[{"ratingGroup":100,"uplink":1,"downlink":2}]}`

func TestJSONIsStrict(t *testing.T) {
	r, err := FromJSON([]byte(goodJSON))
	if err != nil || r.Containers[0].Downlink != 2 {
		t.Fatalf("%v", err)
	}
	for name, in := range map[string]string{
		"unknown field":   strings.Replace(goodJSON, `"sequence"`, `"sequnce"`, 1),
		"trailing data":   goodJSON + `{}`,
		"not json":        `nope`,
		"negative volume": strings.Replace(goodJSON, `"uplink":1`, `"uplink":-1`, 1),
		"letters in imsi": strings.Replace(goodJSON, `262011234567890`, `26201x`, 1),
		"no time":         strings.Replace(goodJSON, `"openTime":"2030-06-01T12:30:00Z",`, ``, 1),
	} {
		if _, err := FromJSON([]byte(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := FromJSON(make([]byte, MaxJSONSize+1)); !errors.Is(err, ErrInvalid) {
		t.Errorf("oversized: %v", err)
	}
}

func TestKeyIdentifiesTheSessionRecordNotTheCopy(t *testing.T) {
	a, _ := FromJSON([]byte(goodJSON))
	b := a
	b.Source = "other-file#9"
	b.Containers = []Container{{RatingGroup: 100, Uplink: 99}} // a resend may differ in detail
	b.Duration = 61
	if a.Key() != b.Key() {
		t.Fatal("copies of one record must share a key")
	}
	for name, mut := range map[string]func(*Record){
		"sequence": func(r *Record) { r.Sequence++ },
		"charging": func(r *Record) { r.ChargingID++ },
		"node":     func(r *Record) { r.NodeID = "pgw-2" },
		"time":     func(r *Record) { r.OpenTime = r.OpenTime.Add(time.Millisecond) },
		"imsi":     func(r *Record) { r.IMSI = "262019999999999" },
		"type":     func(r *Record) { r.Type = "SMF" },
	} {
		c := a
		mut(&c)
		if c.Key() == a.Key() {
			t.Errorf("a different %s must change the key", name)
		}
	}
}

// One stream of 1000 records, as a file is read: the reader (and its buffer) is reused.
func BenchmarkBERDecode(b *testing.B) {
	one := cdrtest.PGW{IMSI: "262011234567890", MSISDN: "491701234567", Node: "pgw-1", APN: "internet", ChargingID: 1, Sequence: 1,
		Y: 30, Mo: 6, D: 1, H: 12, Duration: 60, Containers: [][3]uint64{{100, 1000, 5000}, {200, 1, 2}}}.Encode()
	stream := bytes.Repeat(one, 1000)
	m := PGWMapping()
	b.SetBytes(int64(len(one)))
	b.ReportAllocs()
	rd := ber.NewReader(bytes.NewReader(stream))
	for i := 0; i < b.N; i++ {
		e, err := rd.Next()
		if err != nil {
			rd = ber.NewReader(bytes.NewReader(stream))
			e, _ = rd.Next()
		}
		if _, err := FromBER(&e, &m); err != nil {
			b.Fatal(err)
		}
	}
}
