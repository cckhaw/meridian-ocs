package ber

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"
)

func read(t *testing.T, b []byte) (Element, error) {
	t.Helper()
	return NewReader(bytes.NewReader(b)).Next()
}

func TestPrimitiveAndNested(t *testing.T) {
	// [79] { [3] 01 02 ; [5] 00 00 04 d2 ; [34] { [1] 00 64 } }
	b := []byte{0xbf, 0x4f, 0x10, // [79] constructed, 16 bytes
		0x83, 0x02, 0x01, 0x02,
		0x85, 0x04, 0x00, 0x00, 0x04, 0xd2,
		0xa2, 0x04, 0x81, 0x02, 0x00, 0x64}
	e, err := read(t, b)
	if err != nil {
		t.Fatal(err)
	}
	if e.Tag != (Tag{Context, 79}) || !e.Constructed || len(e.Children) != 3 {
		t.Fatalf("%+v", e)
	}
	if v, _ := e.Find(5).Int(); v != 1234 {
		t.Fatalf("%d", v)
	}
	if !bytes.Equal(e.Find(3).Value, []byte{1, 2}) {
		t.Fatal("value")
	}
	if v, _ := e.Find(2).Find(1).Uint(); v != 100 {
		t.Fatalf("%d", v)
	}
	if e.Find(99) != nil || len(e.FindAll(5)) != 1 {
		t.Fatal("find")
	}
}

func TestLongFormLengthAndIndefinite(t *testing.T) {
	val := bytes.Repeat([]byte{7}, 300)
	b := append([]byte{0x84, 0x82, 0x01, 0x2c}, val...)
	e, err := read(t, b)
	if err != nil || len(e.Value) != 300 {
		t.Fatalf("%v", err)
	}
	// Indefinite: [0] { [1] 05 } 00 00
	e, err = read(t, []byte{0xa0, 0x80, 0x81, 0x01, 0x05, 0x00, 0x00})
	if err != nil || len(e.Children) != 1 || e.Children[0].Value[0] != 5 {
		t.Fatalf("%v %+v", err, e)
	}
}

func TestSignedAndUnsigned(t *testing.T) {
	e := Element{Value: []byte{0xff, 0xfe}}
	if v, _ := e.Int(); v != -2 {
		t.Fatalf("%d", v)
	}
	e = Element{Value: []byte{0x00, 0xff, 0xfe}}
	if v, _ := e.Uint(); v != 0xfffe {
		t.Fatalf("%d", v)
	}
	for _, bad := range []Element{{}, {Value: make([]byte, 9)}, {Constructed: true}} {
		if _, err := bad.Int(); err == nil {
			t.Errorf("Int accepted %+v", bad)
		}
	}
}

func TestStreamOfRecords(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		buf.Write([]byte{0xa1, 0x03, 0x80, 0x01, byte(i)})
	}
	rd := NewReader(&buf)
	for i := 0; i < 3; i++ {
		e, err := rd.Next()
		if err != nil || e.Children[0].Value[0] != byte(i) {
			t.Fatalf("%d: %v", i, err)
		}
	}
	if _, err := rd.Next(); err != io.EOF {
		t.Fatalf("clean end: %v", err)
	}
}

func TestMalformedInput(t *testing.T) {
	cases := map[string][]byte{
		"truncated header":          {0x30},
		"truncated value":           {0x80, 0x05, 0x01},
		"truncated indefinite":      {0xa0, 0x80, 0x81, 0x01, 0x05},
		"reserved length":           {0x80 | 0x20, 0xff, 0x00},
		"length of length 5":        {0x80, 0x85, 0, 0, 0, 0, 1},
		"primitive indefinite":      {0x80, 0x80, 0x00, 0x00},
		"child overruns parent":     {0xa0, 0x03, 0x80, 0x05, 0x01, 0x02, 0x03, 0x04},
		"tag number too long":       {0x9f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00},
		"non-minimal tag":           {0x9f, 0x80, 0x01, 0x00},
		"declared length too large": {0x80, 0x84, 0xff, 0xff, 0xff, 0xff},
	}
	for name, b := range cases {
		if _, err := read(t, b); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrTooLarge) {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
}

func TestDepthIsBounded(t *testing.T) {
	var b []byte
	for i := 0; i < MaxDepth+5; i++ {
		b = append(b, 0xa0, 0x80)
	}
	if _, err := read(t, b); !errors.Is(err, ErrMalformed) {
		t.Fatalf("%v", err)
	}
}

func TestHugeLengthDoesNotAllocate(t *testing.T) {
	// Declares almost 4 MiB but supplies 3 bytes: must fail (the allocation is
	// bounded by MaxElementSize either way).
	b := []byte{0x80, 0x83, 0x3f, 0xff, 0xff, 1, 2, 3}
	if _, err := read(t, b); err == nil {
		t.Fatal("accepted")
	}
	b = []byte{0x80, 0x84, 0x7f, 0xff, 0xff, 0xff}
	if _, err := read(t, b); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("%v", err)
	}
}

func FuzzReader(f *testing.F) {
	f.Add([]byte{0xbf, 0x4f, 0x04, 0x83, 0x02, 0x01, 0x02})
	f.Add([]byte{0xa0, 0x80, 0x81, 0x01, 0x05, 0x00, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		rd := NewReader(bytes.NewReader(b))
		for i := 0; i < 100; i++ {
			if _, err := rd.Next(); err != nil {
				return
			}
		}
	})
}

func TestRandomGarbageNeverPanics(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := make([]byte, rng.Intn(64))
		rng.Read(b)
		rd := NewReader(bytes.NewReader(b))
		for j := 0; j < 10; j++ {
			if _, err := rd.Next(); err != nil {
				break
			}
		}
	}
}
