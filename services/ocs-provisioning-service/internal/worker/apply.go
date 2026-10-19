package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// Outcome is the result of applying a command.
type Outcome struct {
	State   status.State
	Code    string // "applied" or the reason it failed
	Message string
	Res     datalayer.ProvisionResult
}

// Applier applies one command. A non-nil error means the infrastructure could
// not be reached (the worker retries); a business failure is an Outcome with
// State == status.Failed.
type Applier interface {
	Apply(ctx context.Context, c *command.Command) (Outcome, error)
}

// Directory is the identity directory as the applier uses it
// (*identity.RedisDirectory).
type Directory interface {
	Lookup(ctx context.Context, id identity.Identity) (string, error)
	BindNew(ctx context.Context, id identity.Identity, key string) (created bool, err error)
	UnbindIf(ctx context.Context, id identity.Identity, key string) (bool, error)
}

// DataApplier applies commands to the hot store and the identity directory.
type DataApplier struct {
	Data *datalayer.Client
	Dir  Directory
	Now  func() time.Time
}

func failed(code, msg string) Outcome { return Outcome{State: status.Failed, Code: code, Message: msg} }

// Apply implements Applier.
func (a *DataApplier) Apply(ctx context.Context, c *command.Command) (Outcome, error) {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	op := datalayer.ProvisionOp{
		Key: c.Key, Token: c.ID, Fingerprint: c.Fingerprint(), Now: now(),
		BucketID: c.BucketID, Amount: c.Amount,
	}
	switch c.Type {
	case command.CreateSubscriber:
		return a.create(ctx, c, op)
	case command.TopUp:
		op.Op, op.Order = datalayer.ProvTopUp, command.CashPriority
	case command.AdjustBalance:
		op.Op, op.Order, op.AllowNegative = datalayer.ProvAdjust, command.CashPriority, c.AllowNegative
	case command.AddBundle:
		op.Op, op.Class, op.Order, op.ExpiresAt = datalayer.ProvAddBundle, datalayer.Class(c.Class), c.Order(), c.ExpiresTime()
		op.Unit = unitKind(c.Unit)
	case command.ReloadBucket:
		op.Op, op.ExpiresAt, op.KeepExpiry = datalayer.ProvReload, c.ExpiresTime(), c.KeepExpiry
		op.KeepOrder, op.Order = c.Priority == nil, c.Order()
	case command.RemoveBucket:
		op.Op, op.Force = datalayer.ProvRemove, c.Force
	case command.ChangeLifecycle:
		op.Op, op.Target = datalayer.ProvLifecycle, c.Target
		if c.GraceUntil > 0 {
			op.Until = time.UnixMilli(c.GraceUntil)
		}
	default:
		return failed("invalid", "unknown command type"), nil
	}
	return a.provision(ctx, op)
}

func unitKind(s string) canonical.UnitKind {
	switch s {
	case "seconds":
		return canonical.UnitSeconds
	case "events":
		return canonical.UnitEvents
	}
	return canonical.UnitBytes
}

func (a *DataApplier) provision(ctx context.Context, op datalayer.ProvisionOp) (Outcome, error) {
	res, err := a.Data.Provision(ctx, op)
	switch {
	case errors.Is(err, datalayer.ErrInvalid):
		// Our own validation disagreed with the data layer's: retrying cannot help.
		return failed("invalid", err.Error()), nil
	case err != nil:
		return Outcome{}, err
	}
	if eff := res.Effective(); eff != datalayer.ProvApplied {
		return Outcome{State: status.Failed, Code: eff.String(), Message: failureMessage(eff, res), Res: res}, nil
	}
	return Outcome{State: status.Applied, Code: "applied", Res: res}, nil
}

func failureMessage(c datalayer.ProvisionCode, r datalayer.ProvisionResult) string {
	switch c {
	case datalayer.ProvNoSubscriber:
		return "the subscriber does not exist"
	case datalayer.ProvExists:
		return "already exists"
	case datalayer.ProvInvalidTransition:
		return fmt.Sprintf("cannot move from %s to %s", r.From, r.To)
	case datalayer.ProvInsufficient:
		return fmt.Sprintf("balance %d would go below zero", r.Balance)
	case datalayer.ProvTokenMismatch:
		return "the idempotency key was already used for a different request"
	case datalayer.ProvNoBucket:
		return "no such bucket"
	case datalayer.ProvTerminated:
		return "the subscriber is terminated"
	case datalayer.ProvOverflow:
		return "the balance would exceed the maximum"
	case datalayer.ProvBucketNotEmpty:
		return fmt.Sprintf("the bucket still holds %d; use force to remove it", r.Balance)
	case datalayer.ProvNotCashBucket:
		return "a top-up needs a cash bucket"
	}
	return c.String()
}

// create creates the subscriber and binds its identities.
//
// Order matters. Identities are first checked read-only, so the common
// conflict is refused before anything is written. The profile is created next
// (idempotent under the command id), then the identities are bound. If a bind
// still conflicts because of a race, the bindings made by this attempt are
// undone and the command fails; the profile then exists without identities,
// which is harmless (no traffic maps to it) and visible to an operator.
func (a *DataApplier) create(ctx context.Context, c *command.Command, op datalayer.ProvisionOp) (Outcome, error) {
	ids := c.Identities()
	for _, id := range ids {
		k, err := a.Dir.Lookup(ctx, id)
		switch {
		case errors.Is(err, identity.ErrUnknown):
		case err != nil:
			return Outcome{}, err
		case k != c.Key:
			return failed("identity_conflict", fmt.Sprintf("%s %s already belongs to another subscriber", id.Type, id.Value)), nil
		}
	}

	op.Op, op.Payment = datalayer.ProvCreate, c.Payment
	out, err := a.provision(ctx, op)
	if err != nil || out.State != status.Applied {
		return out, err
	}

	var made []identity.Identity
	for _, id := range ids {
		created, err := a.Dir.BindNew(ctx, id, c.Key)
		switch {
		case errors.Is(err, identity.ErrConflict):
			for _, m := range made {
				if _, uerr := a.Dir.UnbindIf(ctx, m, c.Key); uerr != nil {
					return Outcome{}, uerr
				}
			}
			f := failed("identity_conflict", fmt.Sprintf("%s %s was taken by another subscriber while creating; the subscriber exists without identities", id.Type, id.Value))
			f.Res = out.Res
			return f, nil
		case err != nil:
			return Outcome{}, err
		}
		if created {
			made = append(made, id)
		}
	}
	return out, nil
}
