// Package ber is a streaming decoder for ASN.1 Basic Encoding Rules, enough to
// read 3GPP charging data record files (TS 32.297 / 32.298): definite and
// indefinite lengths, multi-byte tags, nested constructed values.
//
// It is built for hostile input. Every length is checked against the bytes
// that actually remain and against MaxElementSize before anything is
// allocated, nesting is bounded by MaxDepth, and a truncated or malformed
// element is an error, never a panic or an unbounded allocation.
package ber

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// Limits.
const (
	MaxDepth       = 16
	MaxElementSize = 4 << 20 // one top-level record, including all children
)

// Class is the ASN.1 tag class.
type Class uint8

const (
	Universal Class = iota
	Application
	Context
	Private
)

// Tag identifies an element.
type Tag struct {
	Class  Class
	Number uint32
}

// Element is one decoded TLV. Constructed elements have Children and no Value;
// primitive elements have Value.
type Element struct {
	Tag         Tag
	Constructed bool
	Value       []byte
	Children    []Element
}

var (
	// ErrMalformed marks input that is not valid BER.
	ErrMalformed = errors.New("ber: malformed element")
	// ErrTooLarge marks an element over MaxElementSize.
	ErrTooLarge = errors.New("ber: element too large")
)

// Reader reads consecutive top-level elements.
type Reader struct {
	r    *bufio.Reader
	left int // bytes this element may still consume
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 64<<10)} }

// Next reads the next top-level element. It returns io.EOF only at a clean
// element boundary; a stream that ends inside an element is io.ErrUnexpectedEOF
// wrapped in ErrMalformed.
func (rd *Reader) Next() (Element, error) {
	if _, err := rd.r.Peek(1); err != nil {
		return Element{}, err // io.EOF at a boundary
	}
	rd.left = MaxElementSize
	e, err := rd.element(0)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Element{}, fmt.Errorf("%w: truncated", ErrMalformed)
		}
		return Element{}, err
	}
	return e, nil
}

func (rd *Reader) byte() (byte, error) {
	if rd.left <= 0 {
		return 0, ErrTooLarge
	}
	b, err := rd.r.ReadByte()
	if err != nil {
		return 0, err
	}
	rd.left--
	return b, nil
}

func (rd *Reader) readTag() (Tag, bool, error) {
	b, err := rd.byte()
	if err != nil {
		return Tag{}, false, err
	}
	t := Tag{Class: Class(b >> 6), Number: uint32(b & 0x1f)}
	constructed := b&0x20 != 0
	if t.Number == 0x1f { // high-tag-number form
		t.Number = 0
		for i := 0; ; i++ {
			if i >= 4 {
				return t, false, fmt.Errorf("%w: tag number too long", ErrMalformed)
			}
			b, err = rd.byte()
			if err != nil {
				return t, false, err
			}
			if i == 0 && b == 0x80 {
				return t, false, fmt.Errorf("%w: non-minimal tag", ErrMalformed)
			}
			t.Number = t.Number<<7 | uint32(b&0x7f)
			if b&0x80 == 0 {
				break
			}
		}
	}
	return t, constructed, nil
}

// readLen returns the content length, or -1 for the indefinite form.
func (rd *Reader) readLen() (int, error) {
	b, err := rd.byte()
	if err != nil {
		return 0, err
	}
	if b < 0x80 {
		return int(b), nil
	}
	if b == 0x80 {
		return -1, nil
	}
	n := int(b & 0x7f)
	if n > 4 { // 0xff is reserved; > 4 bytes could never fit MaxElementSize
		return 0, fmt.Errorf("%w: length of length %d", ErrMalformed, n)
	}
	v := 0
	for i := 0; i < n; i++ {
		c, err := rd.byte()
		if err != nil {
			return 0, err
		}
		v = v<<8 | int(c)
	}
	if v < 0 || v > rd.left {
		return 0, ErrTooLarge
	}
	return v, nil
}

func (rd *Reader) element(depth int) (Element, error) {
	if depth > MaxDepth {
		return Element{}, fmt.Errorf("%w: nested deeper than %d", ErrMalformed, MaxDepth)
	}
	tag, constructed, err := rd.readTag()
	if err != nil {
		return Element{}, err
	}
	n, err := rd.readLen()
	if err != nil {
		return Element{}, err
	}
	e := Element{Tag: tag, Constructed: constructed}
	switch {
	case n == -1:
		if !constructed {
			return e, fmt.Errorf("%w: indefinite length on a primitive element", ErrMalformed)
		}
		for {
			// End-of-contents octets: 00 00.
			p, err := rd.r.Peek(2)
			if err != nil {
				return e, err
			}
			if p[0] == 0 && p[1] == 0 {
				rd.r.Discard(2)
				rd.left -= 2
				return e, nil
			}
			c, err := rd.element(depth + 1)
			if err != nil {
				return e, err
			}
			e.Children = append(e.Children, c)
		}
	case constructed:
		if n > rd.left {
			return e, ErrTooLarge
		}
		end := rd.left - n
		for rd.left > end {
			c, err := rd.element(depth + 1)
			if err != nil {
				return e, err
			}
			e.Children = append(e.Children, c)
		}
		if rd.left != end { // a child ran past the parent's declared length
			return e, fmt.Errorf("%w: child overruns its parent", ErrMalformed)
		}
		return e, nil
	default:
		if n > rd.left {
			return e, ErrTooLarge
		}
		e.Value = make([]byte, n)
		if _, err := io.ReadFull(rd.r, e.Value); err != nil {
			return e, err
		}
		rd.left -= n
		return e, nil
	}
}

// Find returns the first direct child with the given context-class tag number.
func (e *Element) Find(number uint32) *Element {
	for i := range e.Children {
		if c := &e.Children[i]; c.Tag.Class == Context && c.Tag.Number == number {
			return c
		}
	}
	return nil
}

// FindAll returns every direct child with the given context-class tag number.
func (e *Element) FindAll(number uint32) []*Element {
	var out []*Element
	for i := range e.Children {
		if c := &e.Children[i]; c.Tag.Class == Context && c.Tag.Number == number {
			out = append(out, c)
		}
	}
	return out
}

// Int decodes a two's-complement big-endian INTEGER of up to 8 bytes.
func (e *Element) Int() (int64, error) {
	if e.Constructed || len(e.Value) == 0 || len(e.Value) > 8 {
		return 0, fmt.Errorf("%w: not an integer of 1-8 bytes", ErrMalformed)
	}
	var v int64
	if e.Value[0]&0x80 != 0 {
		v = -1
	}
	for _, b := range e.Value {
		v = v<<8 | int64(b)
	}
	return v, nil
}

// Uint decodes an unsigned big-endian value of up to 8 bytes (a leading 0x00
// pad byte is allowed).
func (e *Element) Uint() (uint64, error) {
	b := e.Value
	if e.Constructed || len(b) == 0 {
		return 0, fmt.Errorf("%w: not an unsigned integer", ErrMalformed)
	}
	if len(b) > 1 && b[0] == 0 {
		b = b[1:]
	}
	if len(b) > 8 {
		return 0, fmt.Errorf("%w: integer longer than 8 bytes", ErrMalformed)
	}
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v, nil
}
