package failpolicy

import (
	"errors"
	"testing"
	"time"
)

func TestForcedBreakerBlocksTheDataTierAndIgnoresSuccesses(t *testing.T) {
	b := NewBreaker(3, time.Millisecond, nil, nil)
	if !b.AllowDataTier() || b.Open() || b.Forced() {
		t.Fatal("a fresh breaker is closed")
	}
	b.SetForced(true)
	if b.AllowDataTier() || !b.Open() || !b.Forced() {
		t.Fatal("a forced breaker must block")
	}
	time.Sleep(5 * time.Millisecond) // well past probeEvery: still no probe
	if b.AllowDataTier() {
		t.Fatal("a forced breaker must not let probes through")
	}
	b.Record(nil) // a success (from a call already in flight) must not release it
	if !b.Open() {
		t.Fatal("a success released a forced breaker")
	}
	b.SetForced(false)
	if !b.AllowDataTier() || b.Open() {
		t.Fatal("releasing returns the breaker to automatic behaviour")
	}
}

func TestForcedBreakerKeepsItsAutomaticStateUnderneath(t *testing.T) {
	b := NewBreaker(2, time.Hour, nil, nil)
	b.Record(errors.New("x"))
	b.Record(errors.New("x")) // opens by failures
	b.SetForced(true)
	b.SetForced(false)
	if !b.Open() || b.AllowDataTier() {
		t.Fatal("releasing the force must not close a breaker that failures opened")
	}
}
