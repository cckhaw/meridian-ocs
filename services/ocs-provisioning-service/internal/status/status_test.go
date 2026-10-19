package status

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

func cmd(id string) *command.Command {
	return &command.Command{ID: id, Type: command.TopUp, Key: "acct-1", BucketID: "cash", Amount: 5, Currency: "EUR", AcceptedAt: 1_800_000_000_000}
}

func TestLifecycleOfARecord(t *testing.T) {
	s := New(redistest.Start(t), "", 0)
	ctx := context.Background()

	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	rec, created, err := s.Begin(ctx, cmd("c1"))
	if err != nil || !created || rec.State != Accepted {
		t.Fatalf("%+v %v %v", rec, created, err)
	}
	got, err := s.Get(ctx, "c1")
	if err != nil || got.State != Accepted || got.Type != command.TopUp || got.Key != "acct-1" || got.Fingerprint != cmd("c1").Fingerprint() ||
		!got.AcceptedAt.Equal(time.UnixMilli(1_800_000_000_000)) || got.Final() {
		t.Fatalf("%+v %v", got, err)
	}

	done, err := s.Complete(ctx, "c1", Result{State: Applied, Outcome: "applied", Balance: 105, Version: 4})
	if err != nil || !done {
		t.Fatalf("%v %v", done, err)
	}
	got, _ = s.Get(ctx, "c1")
	if got.State != Applied || got.Balance != 105 || got.Version != 4 || got.CompletedAt.IsZero() || !got.Final() {
		t.Fatalf("%+v", got)
	}

	// The first final outcome wins: a redelivered command cannot rewrite history.
	if done, _ = s.Complete(ctx, "c1", Result{State: Failed, Outcome: "something else"}); done {
		t.Fatal("a completed record must not be completed again")
	}
	if got, _ = s.Get(ctx, "c1"); got.State != Applied || got.Outcome != "applied" {
		t.Fatalf("%+v", got)
	}
}

func TestDuplicateBeginReturnsTheExistingRecord(t *testing.T) {
	s := New(redistest.Start(t), "", 0)
	ctx := context.Background()
	if _, created, _ := s.Begin(ctx, cmd("c2")); !created {
		t.Fatal()
	}
	rec, created, err := s.Begin(ctx, cmd("c2"))
	if err != nil || created || rec.State != Accepted || rec.Fingerprint != cmd("c2").Fingerprint() {
		t.Fatalf("%+v %v %v", rec, created, err)
	}
	other := cmd("c2")
	other.Amount = 999
	rec, created, _ = s.Begin(ctx, other)
	if created || rec.Fingerprint == other.Fingerprint() {
		t.Fatal("the stored fingerprint must be the original's so the caller can detect the clash")
	}
}

func TestConcurrentBeginsCreateExactlyOne(t *testing.T) {
	s := New(redistest.Start(t), "", 0)
	var created atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, c, err := s.Begin(context.Background(), cmd("race")); err != nil {
				t.Error(err)
			} else if c {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("%d creators", created.Load())
	}
}

func TestAbortOnlyForgetsAnAcceptedCommand(t *testing.T) {
	s := New(redistest.Start(t), "", 0)
	ctx := context.Background()
	s.Begin(ctx, cmd("a1"))
	if err := s.Abort(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "a1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an aborted command must be gone so the caller can retry")
	}
	s.Begin(ctx, cmd("a2"))
	s.Complete(ctx, "a2", Result{State: Applied, Outcome: "applied"})
	_ = s.Abort(ctx, "a2")
	if got, err := s.Get(ctx, "a2"); err != nil || got.State != Applied {
		t.Fatalf("a finished command must never be aborted: %+v %v", got, err)
	}
}

func TestRecordsExpire(t *testing.T) {
	s := New(redistest.Start(t), "", 50*time.Millisecond)
	ctx := context.Background()
	s.Begin(ctx, cmd("e1"))
	time.Sleep(120 * time.Millisecond)
	if _, err := s.Get(ctx, "e1"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
