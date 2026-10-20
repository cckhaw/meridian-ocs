package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/approval"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/events"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

func approvalSetup(t *testing.T, policy string) (*Service, *fakeProducer, *clock) {
	t.Helper()
	rdb := redistest.Start(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	st := status.New(rdb, "", 0).WithClock(clk.Now)
	prod := &fakeProducer{}
	p, err := approval.ParsePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Config{Limits: command.Limits{Currency: "EUR", MaxTopUp: 100_000, MaxAdjust: 50_000}, Now: clk.Now,
		Policy: p, ApprovalTTL: time.Hour, RepublishAfter: 30 * time.Second}, st, prod, datalayer.New(rdb))
	return svc, prod, clk
}

func adjustCmd(id, by string, amount int64) *command.Command {
	return &command.Command{ID: id, Type: command.AdjustBalance, Key: "acct-1", BucketID: "cash", Amount: amount,
		Currency: "EUR", Reason: "goodwill", Principal: by}
}

// messages returns the decoded messages on a topic.
func (p *fakeProducer) on(topic string) []msg {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []msg
	for _, m := range p.msgs {
		if m.topic == topic {
			out = append(out, m)
		}
	}
	return out
}

func eventTypes(t *testing.T, p *fakeProducer) []string {
	var out []string
	for _, m := range p.on("ocs.provisioning.events") {
		var e events.Event
		if err := json.Unmarshal(m.value, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e.Type)
	}
	return out
}

func TestACommandThatNeedsApprovalIsNotQueuedUntilAnotherPersonApproves(t *testing.T) {
	svc, prod, _ := approvalSetup(t, "adjust_balance>1000")
	ctx := context.Background()

	// Under the threshold, and routine commands, go straight through.
	if acc, err := svc.Submit(ctx, adjustCmd("small", "alice", -1000)); err != nil || acc.State != status.Accepted {
		t.Fatalf("%+v %v", acc, err)
	}
	if acc, err := svc.Submit(ctx, topUp("pay", 99_000)); err != nil || acc.State != status.Accepted {
		t.Fatalf("%+v %v", acc, err)
	}
	before := len(prod.on("ocs.provisioning.commands"))

	acc, err := svc.Submit(ctx, adjustCmd("big", "alice", -2500))
	if err != nil || acc.State != status.PendingApproval || acc.Rule != "adjust_balance>1000" || acc.ExpiresAt.IsZero() {
		t.Fatalf("%+v %v", acc, err)
	}
	if got := len(prod.on("ocs.provisioning.commands")); got != before {
		t.Fatalf("a command waiting for approval was queued (%d -> %d messages)", before, got)
	}
	rec, _ := svc.GetCommand(ctx, "big")
	if rec.State != status.PendingApproval || rec.RequestedBy != "alice" || rec.Final() {
		t.Fatalf("%+v", rec)
	}

	// The requester cannot approve it, and nothing changes.
	if _, err := svc.Approve(ctx, "big", "alice"); !errors.Is(err, ErrSelfApproval) {
		t.Fatalf("self-approval: %v", err)
	}
	if got := len(prod.on("ocs.provisioning.commands")); got != before {
		t.Fatal("a refused self-approval queued the command")
	}

	// Another person can; exactly what was requested is queued, with the approval attached.
	acc, err = svc.Approve(ctx, "big", "bob")
	if err != nil || acc.State != status.Accepted {
		t.Fatalf("%+v %v", acc, err)
	}
	msgs := prod.on("ocs.provisioning.commands")
	if len(msgs) != before+1 {
		t.Fatalf("%d messages", len(msgs))
	}
	c, err := command.Decode(msgs[len(msgs)-1].value)
	if err != nil || c.ID != "big" || c.Amount != -2500 || c.Reason != "goodwill" || c.Principal != "alice" || c.ApprovedBy != "bob" || c.ApprovedAt == 0 {
		t.Fatalf("%+v %v", c, err)
	}
	if msgs[len(msgs)-1].key != "acct-1" {
		t.Fatal("the approved command must be keyed by subscriber like any other")
	}
	if _, err := svc.Approve(ctx, "big", "carol"); !errors.Is(err, ErrNotPending) {
		t.Fatalf("approving twice: %v", err)
	}
	if got := eventTypes(t, prod); len(got) != 2 || got[0] != events.EventApprovalRequested || got[1] != events.EventApprovalApproved {
		t.Fatalf("the audit trail: %v", got)
	}
	st := svc.ApprovalStats()
	if st.Requested != 1 || st.Approved != 1 || st.RefusedSelf != 1 || st.RefusedNotPending != 1 {
		t.Fatalf("%+v", st)
	}
}

func TestRepeatingARequestNeverQueuesTheUnapprovedCopy(t *testing.T) {
	svc, prod, clk := approvalSetup(t, "change_lifecycle:terminated")
	ctx := context.Background()
	term := func() *command.Command {
		return &command.Command{ID: "t1", Type: command.ChangeLifecycle, Key: "acct-1", Target: "terminated", Principal: "alice"}
	}
	if _, err := svc.Submit(ctx, term()); err != nil {
		t.Fatal(err)
	}
	acc, err := svc.Submit(ctx, term()) // the same request again
	if err != nil || !acc.Duplicate || acc.State != status.PendingApproval {
		t.Fatalf("%+v %v", acc, err)
	}
	if _, err := svc.Approve(ctx, "t1", "bob"); err != nil {
		t.Fatal(err)
	}
	n := len(prod.on("ocs.provisioning.commands"))
	// Long after approval, the requester repeats the request (a retry): the service must not queue
	// their copy, which carries no approval.
	clk.advance(10 * time.Minute)
	if acc, err := svc.Submit(ctx, term()); err != nil || !acc.Duplicate {
		t.Fatalf("%+v %v", acc, err)
	}
	if got := len(prod.on("ocs.provisioning.commands")); got != n {
		t.Fatalf("the requester's copy was queued (%d -> %d)", n, got)
	}
	// A different request under the same id is a conflict.
	other := term()
	other.Target = "suspended"
	if _, err := svc.Submit(ctx, other); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("%v", err)
	}
}

func TestAnExpiredRequestCannotBeApproved(t *testing.T) {
	svc, prod, clk := approvalSetup(t, "adjust_balance")
	ctx := context.Background()
	if _, err := svc.Submit(ctx, adjustCmd("old", "alice", -10)); err != nil {
		t.Fatal(err)
	}
	clk.advance(61 * time.Minute)
	if _, err := svc.Approve(ctx, "old", "bob"); !errors.Is(err, ErrApprovalExpired) {
		t.Fatalf("%v", err)
	}
	if err := svc.Reject(ctx, "old", "bob", "late"); !errors.Is(err, ErrApprovalExpired) {
		t.Fatalf("%v", err)
	}
	rec, _ := svc.GetCommand(ctx, "old")
	if rec.State != status.Expired || !rec.Final() {
		t.Fatalf("%+v", rec)
	}
	if got := prod.on("ocs.provisioning.commands"); len(got) != 0 {
		t.Fatal("an expired request was queued")
	}
	if l, _ := svc.PendingApprovals(ctx, 10); len(l) != 0 {
		t.Fatal("an expired request is still listed")
	}
	if st := svc.ApprovalStats(); st.RefusedExpired != 2 {
		t.Fatalf("%+v", st)
	}
}

func TestRejectingEndsARequestAndLeavesATrail(t *testing.T) {
	svc, prod, _ := approvalSetup(t, "adjust_balance")
	ctx := context.Background()
	svc.Submit(ctx, adjustCmd("r1", "alice", -10))
	if err := svc.Reject(ctx, "r1", "bob", "not justified"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Approve(ctx, "r1", "carol"); !errors.Is(err, ErrNotPending) {
		t.Fatalf("approving a rejected request: %v", err)
	}
	if err := svc.Reject(ctx, "r1", "carol", ""); !errors.Is(err, ErrNotPending) {
		t.Fatalf("%v", err)
	}
	if err := svc.Reject(ctx, "nope", "bob", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("%v", err)
	}
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'x'
	}
	svc.Submit(ctx, adjustCmd("r2", "alice", -10))
	if err := svc.Reject(ctx, "r2", "bob", string(long)); !command.IsValidation(err) {
		t.Fatalf("an over-long reason: %v", err)
	}
	rec, _ := svc.GetCommand(ctx, "r1")
	if rec.State != status.Rejected || rec.RejectedBy != "bob" || rec.RejectReason != "not justified" {
		t.Fatalf("%+v", rec)
	}
	if got := len(prod.on("ocs.provisioning.commands")); got != 0 {
		t.Fatal("a rejected request was queued")
	}
	got := eventTypes(t, prod) // r1 requested and rejected; r2 requested
	if len(got) != 3 || got[0] != events.EventApprovalRequested || got[1] != events.EventApprovalRejected || got[2] != events.EventApprovalRequested {
		t.Fatalf("the audit trail: %v", got)
	}
}

func TestAnApprovalThatCannotBeQueuedIsNotLost(t *testing.T) {
	svc, prod, clk := approvalSetup(t, "adjust_balance")
	ctx := context.Background()
	svc.Submit(ctx, adjustCmd("k1", "alice", -10))

	prod.mu.Lock()
	prod.err = errors.New("kafka down")
	prod.mu.Unlock()
	if _, err := svc.Approve(ctx, "k1", "bob"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("%v", err)
	}
	rec, _ := svc.GetCommand(ctx, "k1")
	if rec.State != status.PendingApproval || rec.ApprovedBy != "" {
		t.Fatalf("a failed approval must leave the request pending: %+v", rec)
	}
	prod.mu.Lock()
	prod.err = nil
	prod.mu.Unlock()
	if _, err := svc.Approve(ctx, "k1", "carol"); err != nil {
		t.Fatalf("the approval must be repeatable: %v", err)
	}
	if got := len(prod.on("ocs.provisioning.commands")); got != 1 {
		t.Fatalf("%d messages", got)
	}

	// An approver whose process died after recording the approval repeats it later: the command is
	// queued again (applying is idempotent). Before the republish delay, nothing is queued twice.
	if _, err := svc.Approve(ctx, "k1", "carol"); !errors.Is(err, ErrNotPending) {
		t.Fatalf("too soon: %v", err)
	}
	clk.advance(time.Minute)
	if acc, err := svc.Approve(ctx, "k1", "carol"); err != nil || !acc.Duplicate {
		t.Fatalf("%+v %v", acc, err)
	}
	msgs := prod.on("ocs.provisioning.commands")
	if len(msgs) != 2 {
		t.Fatalf("%d messages", len(msgs))
	}
	c, _ := command.Decode(msgs[1].value)
	if c.ApprovedBy != "carol" {
		t.Fatalf("the republished command lost its approval: %+v", c)
	}
	if _, err := svc.Approve(ctx, "k1", "dave"); !errors.Is(err, ErrNotPending) {
		t.Fatalf("someone else cannot re-queue another approver's request: %v", err)
	}
}

func TestOnlyOneOfSeveralConcurrentApproversQueuesTheCommand(t *testing.T) {
	svc, prod, _ := approvalSetup(t, "adjust_balance")
	ctx := context.Background()
	svc.Submit(ctx, adjustCmd("c1", "alice", -10))
	var ok, refused atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Approve(ctx, "c1", string(rune('b'+i)))
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrNotPending):
				refused.Add(1)
			default:
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 1 || refused.Load() != 11 || len(prod.on("ocs.provisioning.commands")) != 1 {
		t.Fatalf("ok=%d refused=%d queued=%d", ok.Load(), refused.Load(), len(prod.on("ocs.provisioning.commands")))
	}
}

func TestAnApproverSeesWhatWillHappen(t *testing.T) {
	svc, _, _ := approvalSetup(t, "adjust_balance,change_lifecycle:terminated")
	ctx := context.Background()
	svc.Submit(ctx, adjustCmd("a", "alice", -2500))
	svc.Submit(ctx, &command.Command{ID: "t", Type: command.ChangeLifecycle, Key: "acct-9", Target: "terminated", Reason: "fraud", Principal: "alice"})
	l, err := svc.PendingApprovals(ctx, 10)
	if err != nil || len(l) != 2 {
		t.Fatalf("%+v %v", l, err)
	}
	if l[0].CommandID != "a" || l[0].Amount != -2500 || l[0].BucketID != "cash" || l[0].Reason != "goodwill" || l[0].RequestedBy != "alice" ||
		l[0].Summary == "" || l[0].ExpiresAt.Sub(l[0].RequestedAt) != time.Hour {
		t.Fatalf("%+v", l[0])
	}
	if l[1].Target != "terminated" || l[1].Key != "acct-9" {
		t.Fatalf("%+v", l[1])
	}
	n, age, err := svc.PendingStats(ctx)
	if err != nil || n != 2 || age < 0 {
		t.Fatalf("%d %v %v", n, age, err)
	}
}

func TestWithApprovalOffNothingIsHeldBack(t *testing.T) {
	svc, prod, _ := approvalSetup(t, "off")
	acc, err := svc.Submit(context.Background(), adjustCmd("x", "alice", -50_000))
	if err != nil || acc.State != status.Accepted || len(prod.on("ocs.provisioning.commands")) != 1 {
		t.Fatalf("%+v %v", acc, err)
	}
	if got := eventTypes(t, prod); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}
