package main

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// Hist is a lock-free latency histogram with microsecond resolution and a relative error under 1.6 %:
// values below 128 µs are exact, larger ones fall in one of 64 linear steps per power of two. A
// quantile is reported as the upper edge of its bucket, so it is never optimistic.
type Hist struct {
	c   [64 * 48]uint64
	n   atomic.Uint64
	sum atomic.Uint64 // µs
	max atomic.Uint64 // µs
}

func histIndex(us uint64) int {
	s := bits.Len64(us) - 7
	if s < 0 {
		s = 0
	}
	i := s*64 + int(us>>uint(s))
	if limit := 64*48 - 1; i > limit {
		return limit
	}
	return i
}

// histUpper is the largest value (µs) that falls in bucket i.
func histUpper(i int) uint64 {
	if i < 128 {
		return uint64(i)
	}
	s := uint(i/64 - 1)
	m := uint64(i%64 + 64)
	return ((m + 1) << s) - 1
}

// Record adds one observation.
func (h *Hist) Record(d time.Duration) {
	us := uint64(0)
	if d > 0 {
		us = uint64(d / time.Microsecond)
	}
	atomic.AddUint64(&h.c[histIndex(us)], 1)
	h.n.Add(1)
	h.sum.Add(us)
	for {
		m := h.max.Load()
		if us <= m || h.max.CompareAndSwap(m, us) {
			break
		}
	}
}

// Count is the number of observations.
func (h *Hist) Count() uint64 { return h.n.Load() }

// Max is the largest observation, exactly.
func (h *Hist) Max() time.Duration { return time.Duration(h.max.Load()) * time.Microsecond }

// Mean is the arithmetic mean.
func (h *Hist) Mean() time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sum.Load()/n) * time.Microsecond
}

// Quantile returns the q-quantile (0 < q ≤ 1); zero when nothing was recorded.
func (h *Hist) Quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	want := uint64(float64(n)*q + 0.999999)
	if want < 1 {
		want = 1
	}
	var seen uint64
	for i := range h.c {
		seen += atomic.LoadUint64(&h.c[i])
		if seen >= want {
			v := histUpper(i)
			if m := h.max.Load(); v > m {
				v = m // never report more than the largest value seen
			}
			return time.Duration(v) * time.Microsecond
		}
	}
	return h.Max()
}
