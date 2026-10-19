package gy

import (
	"sync/atomic"
	"time"
)

// Admission is lock-free admission control: a GCRA token bucket for rate and
// a counter for concurrent in-flight requests. Both limits can be changed at
// runtime (the AI Ops control plane drives SetRate / SetMaxInflight).
type Admission struct {
	intervalNs  atomic.Int64 // nanoseconds per token; 0 disables the rate limit
	burstNs     atomic.Int64 // tolerance: burst * interval
	tat         atomic.Int64 // theoretical arrival time (GCRA)
	inflight    atomic.Int64
	maxInflight atomic.Int64 // 0 disables
	now         func() int64
}

// NewAdmission creates an Admission. perSecond == 0 or maxInflight == 0
// disable the respective limit.
func NewAdmission(perSecond float64, burst int, maxInflight int) *Admission {
	a := &Admission{now: func() int64 { return time.Now().UnixNano() }}
	a.SetRate(perSecond, burst)
	a.SetMaxInflight(maxInflight)
	return a
}

// SetRate sets the sustained rate and burst size.
func (a *Admission) SetRate(perSecond float64, burst int) {
	if perSecond <= 0 {
		a.intervalNs.Store(0)
		return
	}
	if burst < 1 {
		burst = 1
	}
	iv := int64(float64(time.Second) / perSecond)
	if iv < 1 {
		iv = 1
	}
	a.burstNs.Store(iv * int64(burst))
	a.intervalNs.Store(iv)
}

// SetMaxInflight sets the concurrency ceiling (0 = unlimited).
func (a *Admission) SetMaxInflight(n int) { a.maxInflight.Store(int64(n)) }

// Acquire admits one request. Every successful Acquire must be paired with
// exactly one Release.
func (a *Admission) Acquire() bool {
	n := a.inflight.Add(1)
	if max := a.maxInflight.Load(); max > 0 && n > max {
		a.inflight.Add(-1)
		return false
	}
	iv := a.intervalNs.Load()
	if iv == 0 {
		return true
	}
	burst := a.burstNs.Load()
	now := a.now()
	for {
		tat := a.tat.Load()
		base := tat
		if now > base {
			base = now
		}
		next := base + iv
		if next-now > burst {
			a.inflight.Add(-1)
			return false
		}
		if a.tat.CompareAndSwap(tat, next) {
			return true
		}
	}
}

// Release marks an admitted request as finished.
func (a *Admission) Release() { a.inflight.Add(-1) }

// Inflight reports the current number of admitted requests.
func (a *Admission) Inflight() int64 { return a.inflight.Load() }
