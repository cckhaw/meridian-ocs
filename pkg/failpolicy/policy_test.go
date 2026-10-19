package failpolicy

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type mapProfiles map[string]Profile

func (m mapProfiles) Lookup(id string) (Profile, bool) { p, ok := m[id]; return p, ok }

func newTestEngine(cfg *Config) (*Engine, *ChanJournal) {
	profiles := mapProfiles{
		"prepaid":       {Payment: Prepaid, Status: StatusActive},
		"prepaid-hv":    {Payment: Prepaid, HighValue: true, Status: StatusActive},
		"postpaid":      {Payment: Postpaid, Status: StatusActive},
		"postpaid-susp": {Payment: Postpaid, Status: StatusSuspended},
		"postpaid-grc":  {Payment: Postpaid, Status: StatusGrace},
		"no-class":      {Status: StatusActive},
	}
	j := NewChanJournal(1024)
	return NewEngine(profiles, j, cfg), j
}

func req(id string, n int64) Request {
	return Request{SubscriberID: id, SessionID: "s-" + id, Kind: UnitBytes, Requested: n}
}

func TestDecideByClass(t *testing.T) {
	e, _ := newTestEngine(nil)
	tests := []struct {
		id     string
		want   bool
		reason Reason
	}{
		{"prepaid", false, ReasonPrepaid},
		{"prepaid-hv", true, ReasonGranted},
		{"postpaid", true, ReasonGranted},
		{"postpaid-susp", false, ReasonNotActive},
		{"postpaid-grc", false, ReasonNotActive},
		{"no-class", false, ReasonUnknownProfile},
		{"not-cached", false, ReasonUnknownProfile},
	}
	for _, tc := range tests {
		d := e.Decide(req(tc.id, 1<<20))
		if d.Allowed() != tc.want || d.Reason != tc.reason {
			t.Errorf("%s: got allowed=%v reason=%v, want allowed=%v reason=%v",
				tc.id, d.Allowed(), d.Reason, tc.want, tc.reason)
		}
	}
}

func TestKillSwitchDeniesEveryone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FailOpenEnabled = false
	e, _ := newTestEngine(cfg)
	if d := e.Decide(req("postpaid", 100)); d.Allowed() || d.Reason != ReasonDisabled {
		t.Fatalf("got %+v", d)
	}
}

func TestClassToggles(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowHighValue = false
	e, _ := newTestEngine(cfg)
	if d := e.Decide(req("prepaid-hv", 100)); d.Allowed() || d.Reason != ReasonPrepaid {
		t.Fatalf("hv prepaid with AllowHighValue=false: %+v", d)
	}
	cfg2 := DefaultConfig()
	cfg2.AllowPostpaid = false
	e2, _ := newTestEngine(cfg2)
	if d := e2.Decide(req("postpaid", 100)); d.Allowed() || d.Reason != ReasonClassNotAllowed {
		t.Fatalf("postpaid with AllowPostpaid=false: %+v", d)
	}
}

func TestPerGrantAndPerSubscriberCaps(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Caps[UnitBytes] = Caps{PerGrant: 10, PerSubscriber: 25, Global: 1000}
	e, _ := newTestEngine(cfg)

	d := e.Decide(req("postpaid", 100))
	if d.Granted != 10 || d.FinalUnit || d.Validity != cfg.Validity {
		t.Fatalf("1st: %+v", d)
	}
	d = e.Decide(req("postpaid", 100))
	if d.Granted != 10 || d.FinalUnit {
		t.Fatalf("2nd: %+v", d)
	}
	d = e.Decide(req("postpaid", 100))
	if d.Granted != 5 || !d.FinalUnit {
		t.Fatalf("3rd should be the final 5: %+v", d)
	}
	d = e.Decide(req("postpaid", 100))
	if d.Allowed() || d.Reason != ReasonSubscriberCap {
		t.Fatalf("4th should be denied: %+v", d)
	}
}

func TestGlobalBudgetPartialThenExhausted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Caps[UnitBytes] = Caps{PerGrant: 10, PerSubscriber: 100, Global: 15}
	e, _ := newTestEngine(cfg)
	if d := e.Decide(req("postpaid", 10)); d.Granted != 10 {
		t.Fatalf("%+v", d)
	}
	if d := e.Decide(req("prepaid-hv", 10)); d.Granted != 5 {
		t.Fatalf("expected partial 5, got %+v", d)
	}
	if d := e.Decide(req("postpaid", 10)); d.Allowed() || d.Reason != ReasonGlobalBudget {
		t.Fatalf("%+v", d)
	}
}

func TestNewOutageEpochResetsCounters(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Caps[UnitBytes] = Caps{PerGrant: 10, PerSubscriber: 10, Global: 10}
	e, _ := newTestEngine(cfg)
	if d := e.Decide(req("postpaid", 10)); d.Granted != 10 {
		t.Fatalf("%+v", d)
	}
	if d := e.Decide(req("postpaid", 10)); d.Allowed() {
		t.Fatalf("cap should be exhausted: %+v", d)
	}
	e.BeginOutage()
	if d := e.Decide(req("postpaid", 10)); d.Granted != 10 {
		t.Fatalf("new epoch should reset: %+v", d)
	}
}

func TestJournalRecordsGrantsAndFailureDenies(t *testing.T) {
	e, j := newTestEngine(nil)
	e.Decide(req("prepaid-hv", 100))
	e.Decide(req("postpaid", 100))
	g1, g2 := <-j.C, <-j.C
	if !g1.Prepaid || g2.Prepaid {
		t.Fatalf("Prepaid flag wrong: %+v %+v", g1, g2)
	}

	full := NewEngine(mapProfiles{"p": {Payment: Postpaid, Status: StatusActive}}, NewChanJournal(0), nil)
	d := full.Decide(req("p", 100))
	if d.Allowed() || d.Reason != ReasonJournalFailed {
		t.Fatalf("journal failure must deny: %+v", d)
	}
	// The reservation must have been returned to the global budget.
	if got := full.globalUsed[UnitBytes].Load(); got != 0 {
		t.Fatalf("global budget leaked %d after journal failure", got)
	}
}

func TestInvalidRequest(t *testing.T) {
	e, _ := newTestEngine(nil)
	for _, r := range []Request{{}, {SubscriberID: "postpaid"}, {SubscriberID: "postpaid", Requested: 1, Kind: numKinds}} {
		if d := e.Decide(r); d.Allowed() || d.Reason != ReasonInvalidRequest {
			t.Errorf("%+v -> %+v", r, d)
		}
	}
}

// The process-wide budget is a hard bound on leakage even under contention.
func TestGlobalBudgetNeverExceededConcurrently(t *testing.T) {
	const global = 10_000
	cfg := DefaultConfig()
	cfg.Caps[UnitBytes] = Caps{PerGrant: 7, PerSubscriber: 1 << 40, Global: global}
	profiles := mapProfiles{}
	for i := 0; i < 64; i++ {
		profiles[string(rune('A'+i))] = Profile{Payment: Postpaid, Status: StatusActive}
	}
	j := NewChanJournal(1 << 16)
	e := NewEngine(profiles, j, cfg)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var total int64
	for id := range profiles {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			var local int64
			for i := 0; i < 500; i++ {
				local += e.Decide(req(id, 7)).Granted
			}
			mu.Lock()
			total += local
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	if total != global {
		t.Fatalf("granted %d, want exactly the global budget %d", total, global)
	}
}

func TestBreakerLifecycle(t *testing.T) {
	now := time.Unix(1000, 0)
	var opened, closed int
	b := NewBreaker(3, 5*time.Second, func() { opened++ }, func() { closed++ })
	b.now = func() time.Time { return now }
	boom := errors.New("down")

	for i := 0; i < 2; i++ {
		b.Record(boom)
	}
	if b.Open() || !b.AllowDataTier() {
		t.Fatal("must stay closed below threshold")
	}
	b.Record(boom)
	if !b.Open() || opened != 1 {
		t.Fatalf("should be open once: open=%v opened=%d", b.Open(), opened)
	}
	if b.AllowDataTier() {
		t.Fatal("no probe before probeEvery")
	}

	now = now.Add(6 * time.Second)
	if !b.AllowDataTier() {
		t.Fatal("first probe should be allowed")
	}
	if b.AllowDataTier() {
		t.Fatal("only one probe in flight")
	}
	b.Record(boom) // probe fails
	if !b.Open() || b.AllowDataTier() {
		t.Fatal("failed probe must restart the timer")
	}

	now = now.Add(6 * time.Second)
	if !b.AllowDataTier() {
		t.Fatal("second probe should be allowed")
	}
	b.Record(nil)
	if b.Open() || closed != 1 || !b.AllowDataTier() {
		t.Fatalf("should be closed: open=%v closed=%d", b.Open(), closed)
	}
}

func BenchmarkDecide(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Caps[UnitBytes] = Caps{PerGrant: 1, PerSubscriber: 1 << 60, Global: 1 << 60}
	e := NewEngine(mapProfiles{"p": {Payment: Postpaid, Status: StatusActive}}, journalFunc(func(Grant) error { return nil }), cfg)
	r := req("p", 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Decide(r)
	}
}

type journalFunc func(Grant) error

func (f journalFunc) Record(g Grant) error { return f(g) }
