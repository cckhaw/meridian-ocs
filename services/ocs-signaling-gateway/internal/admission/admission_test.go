package admission

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeClock(a *Admission, start int64) *int64 {
	now := start
	a.now = func() int64 { return atomic.LoadInt64(&now) }
	return &now
}

func TestAdmissionRateAndBurst(t *testing.T) {
	a := New(100, 5, 0) // 100/s, burst 5 => 10ms per token
	clock := fakeClock(a, 1_000_000_000)
	ok := 0
	for i := 0; i < 20; i++ {
		if a.Acquire() {
			ok++
			a.Release()
		}
	}
	if ok != 5 {
		t.Fatalf("burst admitted %d, want 5", ok)
	}
	atomic.AddInt64(clock, int64(25*time.Millisecond)) // 2.5 tokens refilled
	ok = 0
	for i := 0; i < 20; i++ {
		if a.Acquire() {
			ok++
			a.Release()
		}
	}
	if ok != 2 {
		t.Fatalf("after 25ms admitted %d, want 2", ok)
	}
}

func TestAdmissionInflightLimit(t *testing.T) {
	a := New(0, 0, 2)
	if !a.Acquire() || !a.Acquire() {
		t.Fatal("first two must pass")
	}
	if a.Acquire() {
		t.Fatal("third must be refused")
	}
	if a.Inflight() != 2 {
		t.Fatalf("a refused request must not stay counted: %d", a.Inflight())
	}
	a.Release()
	if !a.Acquire() {
		t.Fatal("slot should be free again")
	}
}

func TestAdmissionRateRefusalReleasesInflight(t *testing.T) {
	a := New(1, 1, 10)
	fakeClock(a, 1)
	if !a.Acquire() {
		t.Fatal()
	}
	if a.Acquire() {
		t.Fatal("rate limit should refuse")
	}
	if a.Inflight() != 1 {
		t.Fatalf("inflight %d", a.Inflight())
	}
}

func TestAdmissionRuntimeTuning(t *testing.T) {
	a := New(0, 0, 0)
	for i := 0; i < 1000; i++ {
		if !a.Acquire() {
			t.Fatal("unlimited must admit")
		}
	}
	a.SetMaxInflight(1000)
	if a.Acquire() {
		t.Fatal("tightened limit must apply immediately")
	}
}

func TestAdmissionConcurrentBurstIsExact(t *testing.T) {
	a := New(1, 100, 0) // effectively no refill during the test
	fakeClock(a, 1)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if a.Acquire() {
					admitted.Add(1)
					a.Release()
				}
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 100 {
		t.Fatalf("admitted %d, want exactly the burst of 100", admitted.Load())
	}
}

func TestRejectedIsCounted(t *testing.T) {
	a := New(0, 0, 1)
	if !a.Acquire() || a.Acquire() || a.Acquire() {
		t.Fatal("concurrency limit")
	}
	if a.Rejected() != 2 {
		t.Fatalf("%d", a.Rejected())
	}
	b := New(1, 1, 0)
	b.now = func() int64 { return 1_000_000_000 }
	b.Acquire()
	for i := 0; i < 5; i++ {
		b.Acquire()
	}
	if b.Rejected() == 0 {
		t.Fatal("rate rejections must be counted")
	}
}
