// Package events defines the domain events provisioning publishes on the events topic.
package events

import "encoding/json"

// Event types published on the events topic, keyed by subscriber.
const (
	EventProvisioned     = "subscriber.provisioned"
	EventBalanceModified = "balance.modified"
	EventLifecycle       = "lifecycle.changed"
	EventOffering        = "offering.changed"
	EventCommandFailed   = "command.failed"

	// The approval trail of a command that needed a second person.
	EventApprovalRequested = "approval.requested"
	EventApprovalApproved  = "approval.approved"
	EventApprovalRejected  = "approval.rejected"
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
	At        int64  `json:"at"`           // unix ms, when the command was accepted
	By        string `json:"by,omitempty"` // who requested the command
	// ApprovedBy is the second person on a command that needed approval; on approval.rejected
	// it is empty and RejectedBy says who declined.
	ApprovedBy string `json:"approvedBy,omitempty"`
	RejectedBy string `json:"rejectedBy,omitempty"`
	Rule       string `json:"rule,omitempty"` // the policy rule that required approval

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

// Encode serialises the event for Kafka.
func (e Event) Encode() ([]byte, error) { return json.Marshal(e) }
