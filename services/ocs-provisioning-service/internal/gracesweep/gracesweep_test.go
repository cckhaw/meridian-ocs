package gracesweep_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/events"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/gracesweep"
)

var t0 = time.UnixMilli(1_700_000_000_000)

type rec struct {
	mu   sync.Mutex
	got  []events.Event
	fail error
}

func (r *rec) Produce(_ context.Context, topic, key string, v []byte, h map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	var e events.Event
	if err := json.Unmarshal(v, &e); err != nil {
		return err
	}
	if topic != "events" || e.Key != key || h["command-id"] != e.CommandID {
		return fmt.Errorf("bad message: %s %s %v", topic, key, h)
	}
	r.got = append(r.got, e)
	return nil
}

func (r *rec) events() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Event(nil), r.got...)
}

type env struct {
	c    *datalayer.Client
	p    *rec
	s    *gracesweep.Sweeper
	now  time.Time
	nmux sync.Mutex
}

func (e *env) clock() time.Time { e.nmux.Lock(); defer e.nmux.Unlock(); return e.now }
func (e *env) at(t time.Time)   { e.nmux.Lock(); e.now = t; e.nmux.Unlock() }

func setup(t *testing.T) *env {
	t.Helper()
	e := &env{c: datalayer.New(redistest.Start(t)), p: &rec{}, now: t0}
	e.s = gracesweep.New(gracesweep.Config{Data: e.c, Prod: e.p, Topic: "events", Interval: time.Second, Batch: 3, Now: e.clock, Logf: t.Logf})
	return e
}

func (e *env) prov(t *testing.T, op datalayer.ProvisionOp) datalayer.ProvisionResult {
	t.Helper()
	op.Now = e.clock()
	if op.Fingerprint == "" {
		op.Fingerprint = fmt.Sprintf("%c|%s|%s", op.Op, op.Key, op.Target)
	}
	r, err := e.c.Provision(context.Background(), op)
	if err != nil || r.Code != datalayer.ProvApplied {
		t.Fatalf("%+v %v", r, err)
	}
	return r
}

func (e *env) grace(t *testing.T, key string, until time.Time) {
	t.Helper()
	e.prov(t, datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: key, Token: "c-" + key, Payment: "postpaid"})
	e.prov(t, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: key, Token: "g-" + key, Target: "grace", Until: until})
}

func (e *env) status(t *testing.T, key string) string {
	t.Helper()
	p, err := e.c.GetProfile(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return p.Status
}

func TestSuspendsOnlyWhenTheGracePeriodHasEnded(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.grace(t, "a", t0.Add(time.Hour))
	e.grace(t, "b", t0.Add(2*time.Hour))

	if n, err := e.s.Once(ctx); err != nil || n != 0 || e.status(t, "a") != "grace" {
		t.Fatalf("before any deadline: %d %v %s", n, err, e.status(t, "a"))
	}
	e.at(t0.Add(time.Hour + time.Second))
	if n, err := e.s.Once(ctx); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if e.status(t, "a") != "suspended" || e.status(t, "b") != "grace" {
		t.Fatalf("a=%s b=%s", e.status(t, "a"), e.status(t, "b"))
	}
	evs := e.p.events()
	if len(evs) != 1 || evs[0].Type != events.EventLifecycle || evs[0].Key != "a" || evs[0].From != "grace" || evs[0].To != "suspended" ||
		evs[0].By != gracesweep.By || evs[0].CommandID != gracesweep.Token("a", t0.Add(time.Hour)) {
		t.Fatalf("%+v", evs)
	}
	if st := e.s.Stats(); st.Suspended != 1 || st.Overdue != 0 {
		t.Fatalf("%+v", st)
	}
	// Nothing left to do: a second pass changes nothing and publishes nothing.
	if n, _ := e.s.Once(ctx); n != 0 || len(e.p.events()) != 1 {
		t.Fatalf("not idempotent: %d %d", n, len(e.p.events()))
	}
}

func TestRecoveredOrExtendedSubscribersAreNeverSuspended(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	until := t0.Add(time.Hour)
	e.grace(t, "paid", until)
	e.grace(t, "extended", until)
	e.grace(t, "late", until)

	e.prov(t, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "paid", Token: "ok", Target: "active"})
	e.prov(t, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "extended", Token: "a", Target: "active"})
	e.prov(t, datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "extended", Token: "b", Target: "grace", Until: until.Add(24 * time.Hour)})

	e.at(until.Add(time.Minute))
	if n, err := e.s.Once(ctx); err != nil || n != 1 {
		t.Fatalf("only the one that stayed in grace: %d %v", n, err)
	}
	for k, want := range map[string]string{"paid": "active", "extended": "grace", "late": "suspended"} {
		if got := e.status(t, k); got != want {
			t.Errorf("%s: %s, want %s", k, got, want)
		}
	}
	// The extended grace is still tracked and is suspended when its own deadline comes.
	e.at(until.Add(25 * time.Hour))
	if n, _ := e.s.Once(ctx); n != 1 || e.status(t, "extended") != "suspended" {
		t.Fatalf("%d %s", n, e.status(t, "extended"))
	}
}

func TestSweepsMoreThanOneBatch(t *testing.T) {
	e := setup(t) // batch size 3
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		e.grace(t, fmt.Sprintf("s%d", i), t0.Add(time.Duration(i+1)*time.Minute))
	}
	e.at(t0.Add(time.Hour))
	if n, err := e.s.Once(ctx); err != nil || n != 8 {
		t.Fatalf("%d %v", n, err)
	}
	for i := 0; i < 8; i++ {
		if s := e.status(t, fmt.Sprintf("s%d", i)); s != "suspended" {
			t.Errorf("s%d: %s", i, s)
		}
	}
}

func TestAFailedPublishIsRetriedAndNeverSuspendsTwice(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	until := t0.Add(time.Hour)
	e.grace(t, "r", until)
	e.at(until.Add(time.Minute))

	e.p.fail = errors.New("kafka down")
	if n, err := e.s.Once(ctx); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	if e.status(t, "r") != "suspended" || len(e.p.events()) != 0 || e.s.Stats().Errors == 0 {
		t.Fatalf("suspension is applied, the event waits: %s %d", e.status(t, "r"), len(e.p.events()))
	}
	if st := e.s.Stats(); st.Overdue != 1 {
		t.Fatalf("the unpublished entry stays visible as overdue: %+v", st)
	}
	e.p.fail = nil
	if n, _ := e.s.Once(ctx); n != 1 || len(e.p.events()) != 1 {
		t.Fatalf("the event is published once Kafka is back: %d %d", n, len(e.p.events()))
	}
	p, _ := e.c.GetProfile(ctx, "r")
	if p.Version != 3 {
		t.Fatalf("applied twice: version %d", p.Version)
	}
	if st := e.s.Stats(); st.Suspended != 1 || st.Overdue != 0 {
		t.Fatalf("a replay is not a second suspension: %+v", st)
	}
}

func TestTwoSweepersDoNotSuspendTwice(t *testing.T) {
	e := setup(t)
	other := gracesweep.New(gracesweep.Config{Data: e.c, Prod: e.p, Topic: "events", Interval: time.Second, Now: e.clock, Logf: t.Logf})
	for i := 0; i < 20; i++ {
		e.grace(t, fmt.Sprintf("c%d", i), t0.Add(time.Minute))
	}
	e.at(t0.Add(time.Hour))
	var wg sync.WaitGroup
	for _, s := range []*gracesweep.Sweeper{e.s, other} {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.Once(context.Background()) }()
	}
	wg.Wait()
	for i := 0; i < 20; i++ {
		p, _ := e.c.GetProfile(context.Background(), fmt.Sprintf("c%d", i))
		if p.Status != "suspended" || p.Version != 3 {
			t.Errorf("c%d: %+v", i, p)
		}
	}
	if got := e.s.Stats().Suspended + other.Stats().Suspended; got != 20 {
		t.Errorf("counted %d suspensions for 20 subscribers", got)
	}
}
