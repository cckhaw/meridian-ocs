package worker

import (
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/events"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// The event types and the Event itself live in package events, so that the API service can publish
// the approval trail without depending on the worker.
type Event = events.Event

const (
	EventProvisioned       = events.EventProvisioned
	EventBalanceModified   = events.EventBalanceModified
	EventLifecycle         = events.EventLifecycle
	EventOffering          = events.EventOffering
	EventCommandFailed     = events.EventCommandFailed
	EventApprovalRequested = events.EventApprovalRequested
	EventApprovalApproved  = events.EventApprovalApproved
	EventApprovalRejected  = events.EventApprovalRejected
	EventVersion           = events.EventVersion
)

// eventFor builds the event for a finished command.
func eventFor(c *command.Command, o Outcome) Event {
	e := Event{V: EventVersion, CommandID: c.ID, Key: c.Key, At: c.AcceptedAt, By: c.Principal, ApprovedBy: c.ApprovedBy, Operation: string(c.Type)}
	if o.State == status.Failed {
		e.Type, e.Outcome, e.Message = EventCommandFailed, o.Code, o.Message
		return e
	}
	e.Version = o.Res.Version
	switch c.Type {
	case command.CreateSubscriber:
		e.Type, e.Payment = EventProvisioned, c.Payment
	case command.ChangeLifecycle:
		e.Type, e.From, e.To, e.Reason = EventLifecycle, o.Res.From, o.Res.To, c.Reason
	case command.AttachOffering:
		e.Type, e.From, e.To, e.Buckets = EventOffering, o.Res.PrevOffering, o.Res.Offering, o.Res.Buckets
	default:
		bal := o.Res.Balance
		e.Type, e.Bucket, e.Balance, e.Reason = EventBalanceModified, c.BucketID, &bal, c.Reason
		switch c.Type {
		case command.ReloadBucket:
			e.Delta = o.Res.Balance - o.Res.OldBalance
		case command.RemoveBucket:
			e.Delta = -o.Res.Balance
			zero := int64(0)
			e.Balance = &zero
		default:
			e.Delta = c.Amount
		}
	}
	return e
}
