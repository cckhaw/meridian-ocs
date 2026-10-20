package status

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

var t0 = time.UnixMilli(1_800_000_000_000)

func adjust(id, by string) *command.Command {
	return &command.Command{ID: id, Type: command.AdjustBalance, Key: "acct-1", BucketID: "cash", Amount: -2500, Currency: "EUR",
		Reason: "goodwill", Principal: by, AcceptedAt: t0.UnixMilli()}
}

func pendingStore(t *testing.T) (*Store, *time.Time) {
	s := New(redistest.Start(t), "", 0)
	now := t0
	s.now = func() time.Time { return now }
	return s, &now
}

func TestAPendingRequestIsApprovedOnceByAnotherPerson(t *testing.T) {
	s, _ := pendingStore(t)
	ctx := context.Background()
	rec, created, err := s.BeginPending(ctx, adjust("a1", "alice"), "adjust_balance>1000", time.Hour)
	if err != nil || !created || rec.State != PendingApproval || rec.RequestedBy != "alice" || rec.Rule != "adjust_balance>1000" {
		t.Fatalf("%+v %v %v", rec, created, err)
	}
	if again, created, _ := s.BeginPending(ctx, adjust("a1", "alice"), "x", time.Hour); created || again.State != PendingApproval {
		t.Fatal("the same id must return the existing request")
	}
	got, _ := s.Get(ctx, "a1")
	if got.State != PendingApproval || got.Final() || !got.ExpiresAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("%+v", got)
	}

	if _, out, err := s.Approve(ctx, "a1", "alice", false); err != nil || out != SelfApproval {
		t.Fatalf("self-approval: %v %v", out, err)
	}
	if got, _ = s.Get(ctx, "a1"); got.State != PendingApproval {
		t.Fatal("a refused self-approval must change nothing")
	}
	c, out, err := s.Approve(ctx, "a1", "bob", false)
	if err != nil || out != Done || c.ApprovedBy != "bob" || c.Principal != "alice" || c.Amount != -2500 || c.Reason != "goodwill" {
		t.Fatalf("%+v %v %v", c, out, err)
	}
	got, _ = s.Get(ctx, "a1")
	if got.State != Accepted || got.ApprovedBy != "bob" || got.RequestedBy != "alice" {
		t.Fatalf("%+v", got)
	}
	// Approved means queued: a worker can complete it like any accepted command.
	if done, err := s.Complete(ctx, "a1", Result{State: Applied, Outcome: "applied"}); err != nil || !done {
		t.Fatalf("%v %v", done, err)
	}
	if _, out, _ = s.Approve(ctx, "a1", "carol", false); out != NotPending {
		t.Fatalf("a second approval: %v", out)
	}
	if l, _ := s.ListPending(ctx, 10); len(l) != 0 {
		t.Fatalf("an approved request is still listed: %d", len(l))
	}
}

func TestOnlyOneOfManyConcurrentApproversWins(t *testing.T) {
	s, _ := pendingStore(t)
	ctx := context.Background()
	if _, _, err := s.BeginPending(ctx, adjust("race", "alice"), "r", time.Hour); err != nil {
		t.Fatal(err)
	}
	var wins, losses atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, out, err := s.Approve(ctx, "race", string(rune('b'+i)), false)
			switch {
			case err != nil:
				t.Error(err)
			case out == Done:
				wins.Add(1)
			case out == NotPending:
				losses.Add(1)
			default:
				t.Errorf("outcome %v", out)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 || losses.Load() != 15 {
		t.Fatalf("%d approvals won, %d lost", wins.Load(), losses.Load())
	}
}

func TestAnExpiredRequestCanNeitherBeApprovedNorRejectedAndSaysSo(t *testing.T) {
	s, now := pendingStore(t)
	ctx := context.Background()
	if _, _, err := s.BeginPending(ctx, adjust("old", "alice"), "r", time.Hour); err != nil {
		t.Fatal(err)
	}
	*now = t0.Add(time.Hour) // exactly at the expiry: too late
	if _, out, _ := s.Approve(ctx, "old", "bob", false); out != TimedOut {
		t.Fatalf("approve: %v", out)
	}
	if out, _ := s.Reject(ctx, "old", "bob", "no"); out != TimedOut {
		t.Fatalf("reject: %v", out)
	}
	got, _ := s.Get(ctx, "old")
	if got.State != Expired || !got.Final() {
		t.Fatalf("%+v", got)
	}
	if l, _ := s.ListPending(ctx, 10); len(l) != 0 {
		t.Fatal("an expired request is still listed")
	}
	// Just before the expiry it was still approvable.
	if _, _, err := s.BeginPending(ctx, adjust("late", "alice"), "r", time.Hour); err != nil {
		t.Fatal(err)
	}
	*now = t0.Add(time.Hour - time.Millisecond)
	if _, out, _ := s.Approve(ctx, "late", "bob", false); out != Done {
		t.Fatalf("one millisecond before the expiry: %v", out)
	}
}

func TestRejectingEndsARequestForGood(t *testing.T) {
	s, _ := pendingStore(t)
	ctx := context.Background()
	s.BeginPending(ctx, adjust("r1", "alice"), "r", time.Hour)
	if out, err := s.Reject(ctx, "r1", "bob", "not justified"); err != nil || out != Done {
		t.Fatalf("%v %v", out, err)
	}
	got, _ := s.Get(ctx, "r1")
	if got.State != Rejected || got.RejectedBy != "bob" || got.RejectReason != "not justified" || !got.Final() {
		t.Fatalf("%+v", got)
	}
	if _, out, _ := s.Approve(ctx, "r1", "carol", false); out != NotPending {
		t.Fatalf("approving a rejected request: %v", out)
	}
	if out, _ := s.Reject(ctx, "r1", "carol", "again"); out != NotPending {
		t.Fatalf("rejecting twice: %v", out)
	}
	if _, out, _ := s.Approve(ctx, "never-seen", "bob", false); out != NotFound {
		t.Fatalf("%v", out)
	}
	if out, _ := s.Reject(ctx, "never-seen", "bob", ""); out != NotFound {
		t.Fatalf("%v", out)
	}
}

func TestAnApprovalThatCouldNotBeQueuedGoesBackToPending(t *testing.T) {
	s, _ := pendingStore(t)
	ctx := context.Background()
	s.BeginPending(ctx, adjust("u1", "alice"), "r", time.Hour)
	if _, out, _ := s.Approve(ctx, "u1", "bob", false); out != Done {
		t.Fatal(out)
	}
	if err := s.Unapprove(ctx, "u1", "carol"); err != nil { // someone else's attempt: no effect
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, "u1"); got.State != Accepted {
		t.Fatalf("another person undid the approval: %+v", got)
	}
	if err := s.Unapprove(ctx, "u1", "bob"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "u1")
	if got.State != PendingApproval || got.ApprovedBy != "" {
		t.Fatalf("%+v", got)
	}
	if l, _ := s.ListPending(ctx, 10); len(l) != 1 || l[0].ID != "u1" {
		t.Fatalf("%+v", l)
	}
	if _, out, _ := s.Approve(ctx, "u1", "carol", false); out != Done {
		t.Fatalf("the request must be approvable again: %v", out)
	}
}

func TestListingAndStats(t *testing.T) {
	s, now := pendingStore(t)
	ctx := context.Background()
	if n, age, err := s.PendingStats(ctx); err != nil || n != 0 || age != 0 {
		t.Fatalf("%d %v %v", n, age, err)
	}
	for i, id := range []string{"p1", "p2", "p3"} {
		c := adjust(id, "alice")
		c.AcceptedAt = t0.Add(time.Duration(i) * time.Minute).UnixMilli()
		if _, _, err := s.BeginPending(ctx, c, "r", time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	*now = t0.Add(10 * time.Minute)
	l, err := s.ListPending(ctx, 10)
	if err != nil || len(l) != 3 || l[0].ID != "p1" || l[2].ID != "p3" || l[0].Command.Reason != "goodwill" {
		t.Fatalf("%+v %v", l, err)
	}
	if l, _ = s.ListPending(ctx, 2); len(l) != 2 {
		t.Fatalf("the limit: %d", len(l))
	}
	n, age, err := s.PendingStats(ctx)
	if err != nil || n != 3 || age != 10*time.Minute {
		t.Fatalf("%d %v %v", n, age, err)
	}
	// A request that is not the same as an ordinary command keeps its id usable.
	if _, created, _ := s.Begin(ctx, cmd("pending")); !created {
		t.Fatal("a command named 'pending' must not collide with the index")
	}
}

func TestSelfApprovalCanBeAllowedForDevelopment(t *testing.T) {
	s, _ := pendingStore(t)
	ctx := context.Background()
	s.BeginPending(ctx, adjust("dev", "anonymous"), "r", time.Hour)
	if _, out, _ := s.Approve(ctx, "dev", "anonymous", true); out != Done {
		t.Fatalf("%v", out)
	}
}
