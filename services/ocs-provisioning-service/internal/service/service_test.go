package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

type msg struct {
	topic, key string
	value      []byte
}

type fakeProducer struct {
	mu        sync.Mutex
	msgs      []msg
	err       error
	onProduce func() // called first; may cancel the caller's context
}

func (p *fakeProducer) Produce(ctx context.Context, topic, key string, value []byte, _ map[string]string) error {
	if p.onProduce != nil {
		p.onProduce()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, msg{topic, key, value})
	return nil
}

func (p *fakeProducer) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.msgs) }

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func setup(t *testing.T) (*Service, *fakeProducer, *clock, *status.Store) {
	t.Helper()
	rdb := redistest.Start(t)
	st := status.New(rdb, "", 0)
	prod := &fakeProducer{}
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	svc := New(Config{Limits: command.Limits{Currency: "EUR", MaxTopUp: 100_000, MaxAdjust: 5_000}, Now: clk.Now}, st, prod, datalayer.New(rdb))
	return svc, prod, clk, st
}

func topUp(id string, amount int64) *command.Command {
	return &command.Command{ID: id, Type: command.TopUp, Key: "acct-1", BucketID: "cash", Amount: amount, Currency: "EUR"}
}

func TestSubmitQueuesTheCommandKeyedBySubscriber(t *testing.T) {
	svc, prod, clk, st := setup(t)
	acc, err := svc.Submit(context.Background(), topUp("pay-1", 500))
	if err != nil || acc.CommandID != "pay-1" || acc.State != status.Accepted || acc.Duplicate {
		t.Fatalf("%+v %v", acc, err)
	}
	if prod.count() != 1 {
		t.Fatal(prod.count())
	}
	m := prod.msgs[0]
	if m.topic != "ocs.provisioning.commands" || m.key != "acct-1" {
		t.Fatalf("messages must be keyed by subscriber so they stay in order: %+v", m)
	}
	got, err := command.Decode(m.value)
	if err != nil || got.ID != "pay-1" || got.Amount != 500 || got.AcceptedAt != clk.Now().UnixMilli() {
		t.Fatalf("%+v %v", got, err)
	}
	rec, err := st.Get(context.Background(), "pay-1")
	if err != nil || rec.State != status.Accepted {
		t.Fatalf("%+v %v", rec, err)
	}
}

func TestSubmitRejectsInvalidCommandsWithoutSideEffects(t *testing.T) {
	svc, prod, _, st := setup(t)
	c := topUp("bad", -5)
	_, err := svc.Submit(context.Background(), c)
	if !command.IsValidation(err) {
		t.Fatalf("%v", err)
	}
	if prod.count() != 0 {
		t.Fatal("an invalid command was queued")
	}
	if _, err := st.Get(context.Background(), "bad"); !errors.Is(err, status.ErrNotFound) {
		t.Fatal("an invalid command left a status record")
	}
}

func TestRepeatedSubmissionIsOneCommand(t *testing.T) {
	svc, prod, _, _ := setup(t)
	ctx := context.Background()
	if _, err := svc.Submit(ctx, topUp("pay-2", 500)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		acc, err := svc.Submit(ctx, topUp("pay-2", 500))
		if err != nil || !acc.Duplicate || acc.State != status.Accepted {
			t.Fatalf("%+v %v", acc, err)
		}
	}
	if prod.count() != 1 {
		t.Fatalf("a retrying caller queued the command %d times", prod.count())
	}
}

func TestReusingAnIDForDifferentContentIsRefused(t *testing.T) {
	svc, prod, _, _ := setup(t)
	ctx := context.Background()
	svc.Submit(ctx, topUp("pay-3", 500))
	if _, err := svc.Submit(ctx, topUp("pay-3", 900)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("%v", err)
	}
	other := topUp("pay-3", 500)
	other.Key = "acct-2"
	if _, err := svc.Submit(ctx, other); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("%v", err)
	}
	if prod.count() != 1 {
		t.Fatal(prod.count())
	}
}

func TestACommandThatCouldNotBeQueuedCanBeRetried(t *testing.T) {
	svc, prod, _, st := setup(t)
	ctx := context.Background()
	prod.err = errors.New("kafka down")
	if _, err := svc.Submit(ctx, topUp("pay-4", 500)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("%v", err)
	}
	if _, err := st.Get(ctx, "pay-4"); !errors.Is(err, status.ErrNotFound) {
		t.Fatal("the failed command must be forgotten so the same id can be retried")
	}
	prod.err = nil
	acc, err := svc.Submit(ctx, topUp("pay-4", 500))
	if err != nil || acc.Duplicate || prod.count() != 1 {
		t.Fatalf("%+v %v %d", acc, err, prod.count())
	}
}

func TestACallCancelledWhileQueueingStillForgetsTheCommand(t *testing.T) {
	svc, prod, _, st := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	prod.onProduce = cancel // the caller gives up while the message is being produced
	_, err := svc.Submit(ctx, topUp("pay-5", 1))
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("the test must have cancelled the context")
	}
	// Forgetting must not depend on the cancelled context, or the id would be
	// stuck "accepted" with nothing queued.
	if _, err := st.Get(context.Background(), "pay-5"); !errors.Is(err, status.ErrNotFound) {
		t.Fatalf("not forgotten: %v", err)
	}
}

// If the process died after recording the command but before queueing it, the
// caller's retry must eventually queue it.
func TestAStuckCommandIsQueuedAgainOnRetry(t *testing.T) {
	svc, prod, clk, st := setup(t)
	ctx := context.Background()
	c := topUp("pay-6", 500)
	c.AcceptedAt = clk.Now().UnixMilli()
	if _, created, err := st.Begin(ctx, c); err != nil || !created { // recorded, never produced
		t.Fatal(err)
	}

	if _, err := svc.Submit(ctx, topUp("pay-6", 500)); err != nil || prod.count() != 0 {
		t.Fatalf("too soon to republish: %d %v", prod.count(), err)
	}
	clk.advance(time.Minute)
	acc, err := svc.Submit(ctx, topUp("pay-6", 500))
	if err != nil || !acc.Duplicate || prod.count() != 1 {
		t.Fatalf("%+v %v %d", acc, err, prod.count())
	}

	// A finished command is never queued again.
	st.Complete(ctx, "pay-6", status.Result{State: status.Applied, Outcome: "applied"})
	clk.advance(time.Hour)
	acc, _ = svc.Submit(ctx, topUp("pay-6", 500))
	if acc.State != status.Applied || prod.count() != 1 {
		t.Fatalf("%+v %d", acc, prod.count())
	}
}

func TestGetCommandAndGetSubscriber(t *testing.T) {
	svc, _, _, _ := setup(t)
	ctx := context.Background()
	if _, err := svc.GetCommand(ctx, "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := svc.GetCommand(ctx, "bad id/../"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a malformed id is simply not found")
	}
	svc.Submit(ctx, topUp("pay-7", 5))
	if rec, err := svc.GetCommand(ctx, "pay-7"); err != nil || rec.State != status.Accepted {
		t.Fatalf("%+v %v", rec, err)
	}
	if _, err := svc.GetSubscriber(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := svc.GetSubscriber(ctx, "a{b}"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an invalid key is not found, not a server error")
	}
}
