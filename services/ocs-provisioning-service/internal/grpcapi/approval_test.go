package grpcapi

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/approval"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
)

func TestApprovalOverGRPC(t *testing.T) {
	p, _ := approval.ParsePolicy("adjust_balance>1000,change_lifecycle:terminated")
	az := auth.New([]string{"crm"}, []string{"care", "risk"}).WithApprovers([]string{"audit"})
	g := newRigWith(t, az, true, service.Config{Policy: p, ApprovalTTL: time.Hour})
	care, risk, audit, crm := g.client(t, "care"), g.client(t, "risk"), g.client(t, "audit"), g.client(t, "crm")
	ctx, cancel := ctx5()
	defer cancel()

	acc, err := care.AdjustBalance(ctx, &ocsv1.AdjustBalanceRequest{IdempotencyKey: "adj-1", SubscriberKey: "a", Delta: -2500, Currency: "EUR", Reason: "goodwill"})
	if err != nil || acc.State != ocsv1.CommandState_COMMAND_STATE_PENDING_APPROVAL || acc.Approval.GetRule() != "adjust_balance>1000" ||
		acc.Approval.GetRequestedBy() != "care" || acc.Approval.GetExpiresAt() == nil {
		t.Fatalf("%+v %v", acc, err)
	}
	if g.prod.last() != nil {
		t.Fatal("a request waiting for approval was queued")
	}
	st, err := care.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "adj-1"})
	if err != nil || st.State != ocsv1.CommandState_COMMAND_STATE_PENDING_APPROVAL || st.Approval.GetRequestedBy() != "care" {
		t.Fatalf("%+v %v", st, err)
	}

	// Who may look at the queue, and who may decide.
	if _, err := crm.ListApprovals(ctx, &ocsv1.ListApprovalsRequest{}); code(err) != codes.PermissionDenied {
		t.Fatalf("a standard caller lists approvals: %v", err)
	}
	if _, err := crm.ApproveCommand(ctx, &ocsv1.ApproveCommandRequest{CommandId: "adj-1"}); code(err) != codes.PermissionDenied {
		t.Fatalf("%v", err)
	}
	l, err := audit.ListApprovals(ctx, &ocsv1.ListApprovalsRequest{})
	if err != nil || len(l.Approvals) != 1 || l.Approvals[0].CommandId != "adj-1" || l.Approvals[0].Amount != -2500 || l.Approvals[0].Summary == "" {
		t.Fatalf("%+v %v", l, err)
	}
	if _, err := care.ApproveCommand(ctx, &ocsv1.ApproveCommandRequest{CommandId: "adj-1"}); code(err) != codes.PermissionDenied {
		t.Fatalf("self-approval: %v", err)
	}
	if g.prod.last() != nil {
		t.Fatal("self-approval queued the command")
	}
	acc, err = audit.ApproveCommand(ctx, &ocsv1.ApproveCommandRequest{CommandId: "adj-1"})
	if err != nil || acc.State != ocsv1.CommandState_COMMAND_STATE_ACCEPTED {
		t.Fatalf("%+v %v", acc, err)
	}
	if c := g.prod.last(); c == nil || c.ID != "adj-1" || c.Principal != "care" || c.ApprovedBy != "audit" || c.Amount != -2500 {
		t.Fatalf("%+v", c)
	}
	if _, err := risk.ApproveCommand(ctx, &ocsv1.ApproveCommandRequest{CommandId: "adj-1"}); code(err) != codes.FailedPrecondition {
		t.Fatalf("a second approval: %v", err)
	}

	// Reject.
	if _, err := care.ChangeLifecycle(ctx, &ocsv1.ChangeLifecycleRequest{IdempotencyKey: "t-1", SubscriberKey: "a", Target: ocsv1.LifecycleState_LIFECYCLE_STATE_TERMINATED}); err != nil {
		t.Fatal(err)
	}
	rej, err := risk.RejectCommand(ctx, &ocsv1.RejectCommandRequest{CommandId: "t-1", Reason: "wrong subscriber"})
	if err != nil || rej.State != ocsv1.CommandState_COMMAND_STATE_REJECTED {
		t.Fatalf("%+v %v", rej, err)
	}
	st, _ = care.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "t-1"})
	if st.State != ocsv1.CommandState_COMMAND_STATE_REJECTED || st.Approval.GetRejectedBy() != "risk" || st.Approval.GetRejectReason() != "wrong subscriber" {
		t.Fatalf("%+v", st)
	}
	if _, err := audit.ApproveCommand(ctx, &ocsv1.ApproveCommandRequest{CommandId: "nope"}); code(err) != codes.NotFound {
		t.Fatalf("%v", err)
	}
}
