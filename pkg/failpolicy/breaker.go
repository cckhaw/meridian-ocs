package failpolicy

import (
	"sync/atomic"
	"time"
)

// Breaker detects data-tier unavailability. While open, callers skip the data
// tier and use Engine.Decide; a single probe request is let through every
// probeEvery to detect recovery.
//
//	if b.AllowDataTier() {
//	    resp, err := dataLayer.Reserve(...)
//	    b.Record(err)
//	    if err == nil { return resp }
//	}
//	return engine.Decide(...)
type Breaker struct {
	threshold  int32
	probeEvery time.Duration
	now        func() time.Time
	onOpen     func()
	onClose    func()

	fails   atomic.Int32
	openAt  atomic.Int64 // unix nanos; 0 = closed
	probing atomic.Bool
}

// NewBreaker opens after `threshold` consecutive failures. onOpen/onClose may
// be nil.
func NewBreaker(threshold int, probeEvery time.Duration, onOpen, onClose func()) *Breaker {
	if onOpen == nil {
		onOpen = func() {}
	}
	if onClose == nil {
		onClose = func() {}
	}
	return &Breaker{
		threshold:  int32(threshold),
		probeEvery: probeEvery,
		now:        time.Now,
		onOpen:     onOpen,
		onClose:    onClose,
	}
}

// Open reports whether the breaker is currently open.
func (b *Breaker) Open() bool { return b.openAt.Load() != 0 }

// AllowDataTier reports whether this call should try the data tier. When the
// breaker is open it returns true for at most one in-flight probe per
// probeEvery.
func (b *Breaker) AllowDataTier() bool {
	t := b.openAt.Load()
	if t == 0 {
		return true
	}
	if b.now().UnixNano()-t >= int64(b.probeEvery) {
		return b.probing.CompareAndSwap(false, true)
	}
	return false
}

// Record reports the outcome of a data-tier call.
func (b *Breaker) Record(err error) {
	if err == nil {
		b.fails.Store(0)
		if b.openAt.Swap(0) != 0 {
			b.probing.Store(false)
			b.onClose()
		}
		return
	}
	if int32(b.fails.Add(1)) >= b.threshold {
		now := b.now().UnixNano()
		if b.openAt.CompareAndSwap(0, now) {
			b.onOpen()
			return
		}
	}
	// A failed probe restarts the probe timer.
	if b.probing.CompareAndSwap(true, false) {
		b.openAt.Store(b.now().UnixNano())
	}
}
