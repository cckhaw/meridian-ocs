package control

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeClock drives time and timers deterministically.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
	fired   bool
}

func (t *fakeTimer) Stop() bool { was := !t.stopped && !t.fired; t.stopped = true; return was }

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

// advance moves time forward and fires due timers (outside the lock, like real ones).
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for _, t := range c.timers {
		if !t.stopped && !t.fired && !t.at.After(c.now) {
			t.fired = true
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		t.f()
	}
}

type rig struct {
	c      *Controller
	clk    *fakeClock
	rate   float64
	inflt  int
	mode   string
	events []Event
	mu     sync.Mutex
	failOn string // value Set refuses
}

func newRig(t *testing.T, mut func(*Config)) *rig {
	t.Helper()
	r := &rig{clk: &fakeClock{now: time.Unix(1_800_000_000, 0)}, rate: 1000, inflt: 5000, mode: "auto"}
	cfg := Config{Service: "gw", Now: r.clk.Now, AfterFunc: r.clk.AfterFunc,
		Audit: func(e Event) { r.mu.Lock(); r.events = append(r.events, e); r.mu.Unlock() }}
	if mut != nil {
		mut(&cfg)
	}
	r.c = New(cfg)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(r.c.Register(Param{Name: "admission.rate", Kind: Float, Min: 100, Max: 100000,
		Get: func() string { return strconv.FormatFloat(r.rate, 'g', -1, 64) },
		Set: func(v string) error { r.rate, _ = strconv.ParseFloat(v, 64); return nil }}))
	must(r.c.Register(Param{Name: "admission.max_inflight", Kind: Int, Min: 100, Max: 50000,
		Get: func() string { return strconv.Itoa(r.inflt) },
		Set: func(v string) error {
			if v == r.failOn {
				return errors.New("refused")
			}
			r.inflt, _ = strconv.Atoi(v)
			return nil
		}}))
	must(r.c.Register(Param{Name: "breaker.mode", Kind: Enum, Values: []string{"auto", "open"}, Risky: true,
		Get: func() string { return r.mode }, Set: func(v string) error { r.mode = v; return nil }}))
	return r
}

func TestChangeIsClampedAppliedAndRevertsOnItsOwn(t *testing.T) {
	r := newRig(t, nil)
	ch, err := r.c.Propose("agent-1", "admission.rate", "9999999", 0, "shedding load")
	if err != nil {
		t.Fatal(err)
	}
	if ch.State != Applied || ch.Applied != "100000" || !ch.Clamped || ch.Previous != "1000" || ch.TTL != 5*time.Minute {
		t.Fatalf("%+v", ch)
	}
	if r.rate != 100000 {
		t.Fatalf("not applied: %v", r.rate)
	}
	r.clk.advance(4 * time.Minute)
	if r.rate != 100000 {
		t.Fatal("reverted early")
	}
	r.clk.advance(2 * time.Minute)
	if r.rate != 1000 {
		t.Fatalf("an unconfirmed change must revert: %v", r.rate)
	}
	if got := r.c.Changes()[0]; got.State != Expired {
		t.Fatalf("%+v", got)
	}
	// A below-minimum value is raised, an integer is rounded.
	ch, _ = r.c.Propose("agent-1", "admission.max_inflight", "3.6", time.Minute, "test")
	if ch.Applied != "100" || !ch.Clamped {
		t.Fatalf("%+v", ch)
	}
}

func TestConfirmMakesItLast(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("agent-1", "admission.rate", "2000", 0, "capacity added")
	if err := r.c.Confirm(ch.ID, "agent-1"); err != nil {
		t.Fatal(err)
	}
	r.clk.advance(2 * time.Hour)
	if r.rate != 2000 {
		t.Fatalf("a confirmed change reverted: %v", r.rate)
	}
	if err := r.c.Confirm(ch.ID, "agent-1"); !errors.Is(err, ErrState) {
		t.Fatalf("%v", err)
	}
	// And the parameter is free for the next change.
	if _, err := r.c.Propose("agent-1", "admission.rate", "3000", 0, "more"); err != nil {
		t.Fatal(err)
	}
}

func TestTTLIsCapped(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("a", "admission.rate", "2000", 48*time.Hour, "x")
	if ch.TTL != time.Hour {
		t.Fatalf("%v", ch.TTL)
	}
}

func TestManualRevert(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("a", "admission.rate", "2000", 0, "x")
	if err := r.c.Revert(ch.ID, "operator"); err != nil || r.rate != 1000 {
		t.Fatalf("%v %v", err, r.rate)
	}
	r.clk.advance(time.Hour) // the stopped timer must not fire a second revert over a later change
	ch2, _ := r.c.Propose("a", "admission.rate", "4000", time.Hour, "y")
	r.clk.advance(time.Minute)
	if r.rate != 4000 || ch2.State != Applied {
		t.Fatalf("a stale timer reverted a newer change: %v", r.rate)
	}
	if err := r.c.Revert("nope", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

func TestRefusals(t *testing.T) {
	r := newRig(t, nil)
	cases := []struct {
		name           string
		principal, par string
		value, reason  string
		want           error
	}{
		{"unknown parameter", "a", "engine.workers", "64", "x", ErrUnknownParam},
		{"no principal", "", "admission.rate", "2000", "x", ErrNoPrincipal},
		{"no reason", "a", "admission.rate", "2000", "  ", ErrReason},
		{"long reason", "a", "admission.rate", "2000", string(make([]byte, 300)) + "x", ErrReason},
		{"not a number", "a", "admission.rate", "fast", "x", ErrInvalidValue},
		{"NaN", "a", "admission.rate", "NaN", "x", ErrInvalidValue},
		{"infinity", "a", "admission.rate", "+Inf", "x", ErrInvalidValue},
		{"bad enum", "a", "breaker.mode", "half", "x", ErrInvalidValue},
	}
	for _, c := range cases {
		if _, err := r.c.Propose(c.principal, c.par, c.value, 0, c.reason); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
	if r.rate != 1000 || r.mode != "auto" || len(r.c.Changes()) != 0 {
		t.Fatal("a refused proposal changed something")
	}
	// Refusals are audited.
	rejected := 0
	for _, e := range r.c.Audit() {
		if e.Type == "reject" {
			rejected++
		}
	}
	if rejected != len(cases) {
		t.Fatalf("%d of %d refusals audited", rejected, len(cases))
	}
}

func TestOneUnconfirmedChangePerParameter(t *testing.T) {
	r := newRig(t, nil)
	first, _ := r.c.Propose("a", "admission.rate", "2000", 0, "x")
	if _, err := r.c.Propose("b", "admission.rate", "3000", 0, "y"); !errors.Is(err, ErrBusy) {
		t.Fatalf("%v", err)
	}
	if _, err := r.c.Propose("b", "admission.max_inflight", "900", 0, "y"); err != nil {
		t.Fatalf("other parameters are independent: %v", err)
	}
	r.c.Revert(first.ID, "a")
	if _, err := r.c.Propose("b", "admission.rate", "3000", 0, "y"); err != nil {
		t.Fatalf("free again after the revert: %v", err)
	}
}

func TestRiskyChangeNeedsASecondPerson(t *testing.T) {
	r := newRig(t, nil)
	ch, err := r.c.Propose("agent-1", "breaker.mode", "open", 0, "redis is flapping")
	if err != nil || ch.State != PendingApproval || r.mode != "auto" {
		t.Fatalf("%+v %v mode=%s", ch, err, r.mode)
	}
	if _, err := r.c.Approve(ch.ID, "agent-1"); !errors.Is(err, ErrSamePerson) {
		t.Fatalf("self-approval: %v", err)
	}
	if r.mode != "auto" {
		t.Fatal("self-approval applied the change")
	}
	ch, err = r.c.Approve(ch.ID, "oncall")
	if err != nil || ch.State != Applied || ch.Approver != "oncall" || r.mode != "open" {
		t.Fatalf("%+v %v", ch, err)
	}
	if err := r.c.Confirm(ch.ID, "agent-1"); !errors.Is(err, ErrSamePerson) {
		t.Fatalf("the proposer may not make its own risky change permanent: %v", err)
	}
	// Left alone it reverts.
	r.clk.advance(6 * time.Minute)
	if r.mode != "auto" {
		t.Fatal("a risky change must also revert")
	}
}

func TestRiskyConfirmByTheApprover(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("agent-1", "breaker.mode", "open", 0, "x")
	r.c.Approve(ch.ID, "oncall")
	if err := r.c.Confirm(ch.ID, "oncall"); err != nil {
		t.Fatal(err)
	}
	r.clk.advance(time.Hour)
	if r.mode != "open" {
		t.Fatal("confirmed")
	}
}

func TestUnapprovedProposalLapses(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("agent-1", "breaker.mode", "open", 0, "x")
	r.clk.advance(11 * time.Minute)
	if _, err := r.c.Approve(ch.ID, "oncall"); !errors.Is(err, ErrState) {
		t.Fatalf("a lapsed proposal was approved: %v", err)
	}
	if r.mode != "auto" {
		t.Fatal("applied after lapsing")
	}
	if _, err := r.c.Propose("agent-1", "breaker.mode", "open", 0, "again"); err != nil {
		t.Fatalf("the parameter must be free after a lapse: %v", err)
	}
	// Withdrawing a pending proposal also frees it.
	pending := r.c.Changes()[0]
	if err := r.c.Revert(pending.ID, "agent-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.c.Propose("agent-2", "breaker.mode", "open", 0, "third"); got.State != PendingApproval {
		t.Fatalf("%+v", got)
	}
}

func TestApprovalReadsThePreviousValueAgain(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("agent-1", "breaker.mode", "open", 0, "x")
	r.mode = "auto" // changed by someone else while waiting; here unchanged but re-read
	r.c.Approve(ch.ID, "oncall")
	got := r.c.Changes()[0]
	if got.Previous != "auto" {
		t.Fatalf("%+v", got)
	}
}

func TestFreezeStopsChangesButNotReverts(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("a", "admission.rate", "2000", 0, "x")
	r.c.Freeze(true, "oncall", "incident")
	if _, err := r.c.Propose("a", "admission.max_inflight", "900", 0, "y"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("%v", err)
	}
	pend := newRig(t, nil)
	p, _ := pend.c.Propose("a", "breaker.mode", "open", 0, "x")
	pend.c.Freeze(true, "oncall", "incident")
	if _, err := pend.c.Approve(p.ID, "b"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("approval while frozen: %v", err)
	}
	r.clk.advance(6 * time.Minute)
	if r.rate != 1000 {
		t.Fatal("a freeze must not hold changes in place")
	}
	if !r.c.Frozen() {
		t.Fatal("frozen")
	}
	r.c.Freeze(false, "oncall", "over")
	if _, err := r.c.Propose("a", "admission.max_inflight", "900", 0, "y"); err != nil {
		t.Fatal(err)
	}
	_ = ch
}

func TestRateLimitPerPrincipal(t *testing.T) {
	r := newRig(t, func(c *Config) { c.ChangesPerMinute = 3 })
	names := []string{"admission.rate", "admission.max_inflight"}
	n := 0
	for i := 0; i < 5; i++ {
		ch, err := r.c.Propose("noisy", names[i%2], strconv.Itoa(2000+i), time.Minute, "x")
		if err == nil {
			n++
			r.c.Revert(ch.ID, "noisy")
		} else if !errors.Is(err, ErrRateLimited) {
			t.Fatal(err)
		}
	}
	if n != 3 {
		t.Fatalf("%d changes allowed, want the burst of 3", n)
	}
	if _, err := r.c.Propose("quiet", "admission.rate", "2000", 0, "x"); err != nil {
		t.Fatalf("one principal's limit must not affect another: %v", err)
	}
	r.clk.advance(time.Minute)
	if _, err := r.c.Propose("noisy", "admission.max_inflight", "900", 0, "x"); err != nil {
		t.Fatalf("tokens refill: %v", err)
	}
}

func TestSetterFailureLeavesNothingActive(t *testing.T) {
	r := newRig(t, nil)
	r.failOn = "900"
	ch, err := r.c.Propose("a", "admission.max_inflight", "900", 0, "x")
	if err == nil || ch.State != Failed || r.inflt != 5000 {
		t.Fatalf("%+v %v", ch, err)
	}
	if _, err := r.c.Propose("a", "admission.max_inflight", "1000", 0, "x"); err != nil {
		t.Fatalf("a failed change must not block the parameter: %v", err)
	}
}

func TestFailedRevertIsLoudAndRetained(t *testing.T) {
	r := newRig(t, nil)
	ch, _ := r.c.Propose("a", "admission.max_inflight", "900", time.Minute, "x")
	r.failOn = "5000" // the restore value
	r.clk.advance(2 * time.Minute)
	found := false
	for _, e := range r.c.Audit() {
		if e.Type == "revert_failed" {
			found = true
		}
	}
	if !found || r.c.Changes()[0].State != Applied {
		t.Fatal("a failed restore must be audited and the change kept visible")
	}
	r.failOn = ""
	if err := r.c.Revert(ch.ID, "operator"); err != nil || r.inflt != 5000 {
		t.Fatalf("%v %d", err, r.inflt)
	}
}

func TestCloseRevertsEverything(t *testing.T) {
	r := newRig(t, nil)
	r.c.Propose("a", "admission.rate", "2000", 0, "x")
	r.c.Propose("a", "breaker.mode", "open", 0, "pending")
	r.c.Close()
	if r.rate != 1000 || r.mode != "auto" {
		t.Fatalf("%v %s", r.rate, r.mode)
	}
	if _, err := r.c.Propose("a", "admission.rate", "2000", 0, "x"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("%v", err)
	}
}

func TestRegisterValidation(t *testing.T) {
	c := New(Config{})
	get, set := func() string { return "5" }, func(string) error { return nil }
	for name, p := range map[string]Param{
		"bad name":        {Name: "Bad Name", Kind: Int, Min: 0, Max: 10, Get: get, Set: set},
		"no getter":       {Name: "a", Kind: Int, Min: 0, Max: 10, Set: set},
		"no kind":         {Name: "a", Get: get, Set: set},
		"inverted range":  {Name: "a", Kind: Int, Min: 10, Max: 0, Get: get, Set: set},
		"enum of one":     {Name: "a", Kind: Enum, Values: []string{"x"}, Get: get, Set: set},
		"current outside": {Name: "a", Kind: Int, Min: 10, Max: 20, Get: get, Set: set},
		"current not num": {Name: "a", Kind: Int, Min: 0, Max: 10, Get: func() string { return "x" }, Set: set},
	} {
		if err := c.Register(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok := Param{Name: "a.b_c", Kind: Int, Min: 0, Max: 10, Get: get, Set: set}
	if err := c.Register(ok); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(ok); err == nil {
		t.Fatal("duplicate accepted")
	}
	info := c.List()
	if len(info) != 1 || info[0].Current != "5" || info[0].Max != 10 {
		t.Fatalf("%+v", info)
	}
}

func TestConcurrentProposalsOnOneParameter(t *testing.T) {
	r := newRig(t, func(c *Config) { c.ChangesPerMinute = 1000 })
	var wg sync.WaitGroup
	var ok, busy int32
	var mu sync.Mutex
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := r.c.Propose(fmt.Sprintf("p%d", i), "admission.rate", "2000", 0, "x")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrBusy):
				busy++
			default:
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if ok != 1 || busy != 31 {
		t.Fatalf("ok=%d busy=%d", ok, busy)
	}
}
