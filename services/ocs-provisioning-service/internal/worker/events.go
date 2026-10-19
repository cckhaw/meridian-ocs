package worker

import (
	"encoding/json"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// Event types published on the events topic, keyed by subscriber.
const (
	EventProvisioned     = "subscriber.provisioned"
	EventBalanceModified = "balance.modified"
	EventLifecycle       = "lifecycle.changed"
	EventOffering        = "offering.changed"
	EventCommandFailed   = "command.failed"
)

// EventVersion is the schema version of Event.
const EventVersion = 1

// Event is a domain event for downstream systems (notifications, billing,
// analytics). Delivery is at least once: after a crash a command's event can
// be published again, so consumers de-duplicate on (CommandID, Type).
type Event struct {
	V         int    `json:"v"`
	Type      string `json:"type"`
	CommandID string `json:"commandId"`
	Key       string `json:"key"`
	At        int64  `json:"at"` // unix ms, when the command was accepted
	By        string `json:"by,omitempty"`

	Operation string `json:"operation,omitempty"` // command type
	Version   int64  `json:"version,omitempty"`
	Payment   string `json:"payment,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Delta     int64  `json:"delta,omitempty"`
	Balance   *int64 `json:"balance,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	Buckets   int    `json:"buckets,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Outcome   string `json:"outcome,omitempty"`
	Message   string `json:"message,omitempty"`
}

// eventFor builds the event for a finished command.
func eventFor(c *command.Command, o Outcome) Event {
	e := Event{V: EventVersion, CommandID: c.ID, Key: c.Key, At: c.AcceptedAt, By: c.Principal, Operation: string(c.Type)}
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

func (e Event) encode() ([]byte, error) { return json.Marshal(e) }
