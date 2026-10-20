package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/approval"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/events"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

func (s *Service) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}

// trail publishes an approval event for audit consumers. It is best effort: the
// decision itself is already recorded (status store, and the applied command's
// event names the approver), so a failure here is counted and logged, not
// allowed to undo or block a decision.
func (s *Service) trail(ctx context.Context, e events.Event) {
	e.V = events.EventVersion
	raw, err := e.Encode()
	if err == nil {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		err = s.prod.Produce(fctx, s.cfg.EventsTopic, e.Key, raw, map[string]string{"command-id": e.CommandID})
	}
	if err != nil {
		s.eventErrors.Add(1)
		s.logf("approval event %s for %s not published: %v", e.Type, e.CommandID, err)
	}
}

// Policy returns the approval policy in force.
func (s *Service) Policy() approval.Policy { return s.cfg.Policy }

// ApprovalSettings describe the approval set-up to clients (the console).
type ApprovalSettings struct {
	Enabled   bool
	Policy    string
	TTL       time.Duration
	AllowSelf bool
}

// Approval returns the approval settings.
func (s *Service) Approval() ApprovalSettings {
	return ApprovalSettings{Enabled: s.cfg.Policy.Enabled(), Policy: s.cfg.Policy.String(), TTL: s.cfg.ApprovalTTL, AllowSelf: s.cfg.AllowSelfApproval}
}

// Approve lets approver approve a request that is waiting. The command that is
// queued is exactly the one that was requested: the approver supplies nothing
// but their identity. A failure to queue puts the request back to pending, so
// the approval can be repeated; an approval whose message may not have reached
// Kafka (the process died after recording it) is queued again when the same
// approver repeats it.
func (s *Service) Approve(ctx context.Context, id, approver string) (Accepted, error) {
	if !command.ValidID(id) {
		return Accepted{}, ErrNotFound
	}
	c, out, err := s.status.Approve(ctx, id, approver, s.cfg.AllowSelfApproval)
	if err != nil {
		return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	switch out {
	case status.NotFound:
		return Accepted{}, ErrNotFound
	case status.TimedOut:
		s.refusedLate.Add(1)
		return Accepted{}, ErrApprovalExpired
	case status.SelfApproval:
		s.refusedSelf.Add(1)
		s.logf("approval refused: %s tried to approve their own request %s", approver, id)
		return Accepted{}, ErrSelfApproval
	case status.NotPending:
		return s.retryApproval(ctx, id, approver)
	}
	if err := s.publish(ctx, c); err != nil {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = s.status.Unapprove(fctx, id, approver)
		return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.approved.Add(1)
	s.trail(ctx, events.Event{Type: events.EventApprovalApproved, CommandID: c.ID, Key: c.Key, At: c.AcceptedAt,
		By: c.Principal, ApprovedBy: approver, Operation: string(c.Type)})
	s.logf("approval granted: %s %s requested by %s approved by %s", c.Type, c.ID, c.Principal, approver)
	return Accepted{CommandID: id, State: status.Accepted}, nil
}

// retryApproval handles an approval that finds the request already approved. If
// the same approver is repeating it long enough after the original, the
// message may never have been queued, so queue it again (applying is
// idempotent); anything else is "not pending".
func (s *Service) retryApproval(ctx context.Context, id, approver string) (Accepted, error) {
	rec, err := s.status.Get(ctx, id)
	if err != nil {
		return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if rec.State == status.Accepted && rec.ApprovedBy == approver && s.cfg.Now().Sub(rec.ApprovedAt) >= s.cfg.RepublishAfter {
		c, err := s.status.StoredCommand(ctx, id)
		if err != nil {
			return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		c.ApprovedBy, c.ApprovedAt = rec.ApprovedBy, rec.ApprovedAt.UnixMilli()
		if err := s.publish(ctx, c); err != nil {
			return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		return Accepted{CommandID: id, State: status.Accepted, Duplicate: true}, nil
	}
	s.refusedState.Add(1)
	return Accepted{}, ErrNotPending
}

// Reject declines a waiting request. Anyone who may approve may reject, the
// requester included (withdrawing their own request).
func (s *Service) Reject(ctx context.Context, id, by, reason string) error {
	if !command.ValidID(id) {
		return ErrNotFound
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > 256 {
		return &command.ValidationError{Msg: "the reason is at most 256 characters"}
	}
	out, err := s.status.Reject(ctx, id, by, reason)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	switch out {
	case status.NotFound:
		return ErrNotFound
	case status.TimedOut:
		s.refusedLate.Add(1)
		return ErrApprovalExpired
	case status.NotPending:
		s.refusedState.Add(1)
		return ErrNotPending
	}
	s.declined.Add(1)
	rec, _ := s.status.Get(ctx, id)
	s.trail(ctx, events.Event{Type: events.EventApprovalRejected, CommandID: id, Key: rec.Key, At: rec.AcceptedAt.UnixMilli(),
		By: rec.RequestedBy, RejectedBy: by, Operation: string(rec.Type), Reason: reason})
	s.logf("approval declined: %s %s requested by %s declined by %s", rec.Type, id, rec.RequestedBy, by)
	return nil
}

// PendingApproval is a request waiting for a decision, with what an approver needs to decide.
type PendingApproval struct {
	CommandID   string
	Type        command.Type
	Key         string
	RequestedBy string
	RequestedAt time.Time
	ExpiresAt   time.Time
	Rule        string
	Summary     string
	// What would happen (only the fields of the command's type are set).
	BucketID string
	Amount   int64
	Currency string
	Reason   string
	Target   string
	Force    bool
}

// PendingApprovals lists the requests waiting for approval, oldest first.
func (s *Service) PendingApprovals(ctx context.Context, limit int) ([]PendingApproval, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	l, err := s.status.ListPending(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	out := make([]PendingApproval, 0, len(l))
	for _, p := range l {
		c := p.Command
		out = append(out, PendingApproval{CommandID: p.ID, Type: p.Type, Key: p.Key, RequestedBy: p.RequestedBy,
			RequestedAt: p.AcceptedAt, ExpiresAt: p.ExpiresAt, Rule: p.Rule, Summary: approval.Summary(c),
			BucketID: c.BucketID, Amount: c.Amount, Currency: c.Currency, Reason: c.Reason, Target: c.Target, Force: c.Force})
	}
	return out, nil
}

// ApprovalStats are the approval counters and the state of the queue.
type ApprovalStats struct {
	Requested, Approved, Declined                  uint64
	RefusedSelf, RefusedExpired, RefusedNotPending uint64
	EventErrors                                    uint64
}

// ApprovalStats returns the approval counters.
func (s *Service) ApprovalStats() ApprovalStats {
	return ApprovalStats{s.requested.Load(), s.approved.Load(), s.declined.Load(), s.refusedSelf.Load(), s.refusedLate.Load(),
		s.refusedState.Load(), s.eventErrors.Load()}
}

// PendingStats reports how many requests wait and how long the oldest has waited.
func (s *Service) PendingStats(ctx context.Context) (int64, time.Duration, error) {
	n, age, err := s.status.PendingStats(ctx)
	if err != nil {
		return 0, 0, errors.Join(ErrUnavailable, err)
	}
	return n, age, nil
}
