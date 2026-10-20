// Package service is the logic behind the REST and gRPC APIs: validate a
// command, make its acceptance idempotent, queue it on Kafka, and answer
// status and read requests.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

var (
	// ErrIdempotencyConflict: the id was used before for a different request.
	ErrIdempotencyConflict = errors.New("service: idempotency key reused with different content")
	// ErrUnavailable: the status store or Kafka could not be reached; nothing
	// was accepted and the caller may retry with the same id.
	ErrUnavailable = errors.New("service: temporarily unavailable")
	// ErrNotFound: unknown command or subscriber.
	ErrNotFound = errors.New("service: not found")
	// ErrNoCatalog: an offering was requested but no catalog version is in
	// effect (or the service has no catalog); nothing was accepted.
	ErrNoCatalog = errors.New("service: no catalog available")
)

// Config configures the Service.
type Config struct {
	Limits        command.Limits
	CommandsTopic string
	// RepublishAfter: a repeat of a command that is still "accepted" this long
	// after acceptance is queued again, in case the first message never made
	// it to Kafka (the process died between recording and producing). Applying
	// is idempotent, so a duplicate message is harmless. Default 30s.
	RepublishAfter time.Duration
	Now            func() time.Time
	// Catalog supplies the offerings for attach_offering (optional: without it
	// that command is refused with ErrNoCatalog).
	Catalog *catalog.Store
	// Directory resolves an MSISDN, IMSI or NAI to a subscriber key for
	// FindSubscriber (optional: without it lookups answer ErrUnavailable).
	Directory identity.Directory
}

// Service accepts commands and answers reads.
type Service struct {
	cfg    Config
	status *status.Store
	prod   bus.Producer
	data   *datalayer.Client
}

// New builds a Service.
func New(cfg Config, st *status.Store, prod bus.Producer, data *datalayer.Client) *Service {
	if cfg.CommandsTopic == "" {
		cfg.CommandsTopic = "ocs.provisioning.commands"
	}
	if cfg.RepublishAfter <= 0 {
		cfg.RepublishAfter = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{cfg: cfg, status: st, prod: prod, data: data}
}

// Limits returns the configured command limits.
func (s *Service) Limits() command.Limits { return s.cfg.Limits }

// Accepted is the answer to a submitted command.
type Accepted struct {
	CommandID string
	State     status.State
	Duplicate bool
}

// Submit validates and queues a command.
//
// The same id with the same content returns the original command (and, if it
// is still waiting, queues it again after RepublishAfter). The same id with
// different content is ErrIdempotencyConflict. If the command cannot be queued
// it is forgotten, so the caller can retry with the same id.
func (s *Service) Submit(ctx context.Context, c *command.Command) (Accepted, error) {
	c.AcceptedAt = s.cfg.Now().UnixMilli()
	if err := c.Validate(s.cfg.Limits); err != nil {
		return Accepted{}, err
	}
	if c.Type == command.AttachOffering {
		if err := s.expandOffering(c); err != nil {
			return Accepted{}, err
		}
	}
	rec, created, err := s.status.Begin(ctx, c)
	if err != nil {
		return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !created {
		if rec.Fingerprint != c.Fingerprint() {
			return Accepted{}, ErrIdempotencyConflict
		}
		if rec.State == status.Accepted && s.cfg.Now().Sub(rec.AcceptedAt) >= s.cfg.RepublishAfter {
			if err := s.publish(ctx, c); err != nil {
				return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
			}
		}
		return Accepted{CommandID: c.ID, State: rec.State, Duplicate: true}, nil
	}
	if err := s.publish(ctx, c); err != nil {
		// The context may be the thing that failed; forgetting must not depend on it.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = s.status.Abort(fctx, c.ID)
		return Accepted{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return Accepted{CommandID: c.ID, State: status.Accepted}, nil
}

// expandOffering copies the offering's allowances from the catalog version in
// effect into the command, so what is queued is exactly what will be applied.
func (s *Service) expandOffering(c *command.Command) error {
	if s.cfg.Catalog == nil {
		return ErrNoCatalog
	}
	sn := s.cfg.Catalog.At(time.UnixMilli(c.AcceptedAt))
	if sn == nil {
		return ErrNoCatalog
	}
	o, ok := sn.Offering(c.Offering)
	if !ok {
		return &command.ValidationError{Msg: fmt.Sprintf("unknown offering %q in catalog version %d", c.Offering, sn.Version)}
	}
	c.CatalogVersion, c.Allowances = sn.Version, nil
	for _, a := range o.Allowances {
		al := command.Allowance{ID: a.ID, Class: a.Class, Unit: a.Unit, Amount: a.Amount, Order: a.Order}
		if al.Order == 0 {
			al.Order = command.DefaultBundlePriority
			if a.Class == "promo" {
				al.Order = command.DefaultPromoPriority
			}
		}
		if a.ValidityDays > 0 {
			al.ExpiresAt = c.AcceptedAt + int64(a.ValidityDays)*24*60*60*1000
		}
		c.Allowances = append(c.Allowances, al)
	}
	return c.Validate(s.cfg.Limits)
}

func (s *Service) publish(ctx context.Context, c *command.Command) error {
	raw, err := c.Encode()
	if err != nil {
		return err
	}
	return s.prod.Produce(ctx, s.cfg.CommandsTopic, c.Key, raw, nil)
}

// GetCommand returns the status of a command.
func (s *Service) GetCommand(ctx context.Context, id string) (status.Record, error) {
	if !command.ValidID(id) {
		return status.Record{}, ErrNotFound
	}
	rec, err := s.status.Get(ctx, id)
	switch {
	case errors.Is(err, status.ErrNotFound):
		return status.Record{}, ErrNotFound
	case err != nil:
		return status.Record{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return rec, nil
}

// FindSubscriber looks a subscriber up by one of its identifiers. kind is
// "msisdn", "imsi" or "nai". A malformed identifier is a validation error; an
// identifier that is not bound, or bound to a subscriber with no profile, is
// ErrNotFound.
func (s *Service) FindSubscriber(ctx context.Context, kind, value string) (SubscriberView, error) {
	if s.cfg.Directory == nil {
		return SubscriberView{}, fmt.Errorf("%w: subscriber lookup by identifier is not configured", ErrUnavailable)
	}
	t, ok := command.IdentityType(kind)
	if !ok {
		return SubscriberView{}, &command.ValidationError{Msg: "identifier type must be msisdn, imsi or nai"}
	}
	v, ok := identity.Normalize(t, value)
	if !ok {
		return SubscriberView{}, &command.ValidationError{Msg: "malformed " + kind}
	}
	key, err := s.cfg.Directory.Lookup(ctx, identity.Identity{Type: t, Value: v})
	switch {
	case errors.Is(err, identity.ErrUnknown):
		return SubscriberView{}, ErrNotFound
	case err != nil:
		return SubscriberView{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return s.GetSubscriber(ctx, key)
}

// OfferingInfo is an offering of the catalog version in effect, for pickers.
type OfferingInfo struct {
	ID, Name, PlanID string
	Allowances       []catalog.AllowanceBucket
}

// Meta is what a client needs to build a form: the deployment currency and
// limits, and the offerings that can be attached right now.
type Meta struct {
	Currency       string
	MaxTopUp       int64
	CatalogVersion int64 // 0 = no catalog version in effect
	Offerings      []OfferingInfo
}

// Meta reports the deployment's settings and the current offerings.
func (s *Service) Meta() Meta {
	m := Meta{Currency: s.cfg.Limits.Currency, MaxTopUp: s.cfg.Limits.MaxTopUp}
	if s.cfg.Catalog == nil {
		return m
	}
	if sn := s.cfg.Catalog.At(s.cfg.Now()); sn != nil {
		m.CatalogVersion = sn.Version
		for _, o := range sn.Document().Offerings {
			m.Offerings = append(m.Offerings, OfferingInfo{ID: o.ID, Name: o.Name, PlanID: o.PlanID, Allowances: o.Allowances})
		}
	}
	return m
}

// SubscriberView is a subscriber's profile and buckets.
type SubscriberView struct {
	Key     string
	Profile datalayer.Profile
	Buckets []datalayer.BucketInfo
}

// GetSubscriber reads a subscriber directly from the hot store.
func (s *Service) GetSubscriber(ctx context.Context, key string) (SubscriberView, error) {
	p, err := s.data.GetProfile(ctx, key)
	switch {
	case errors.Is(err, datalayer.ErrNoSubscriber), errors.Is(err, datalayer.ErrInvalid):
		return SubscriberView{}, ErrNotFound
	case err != nil:
		return SubscriberView{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	bs, err := s.data.ListBuckets(ctx, key)
	if err != nil {
		return SubscriberView{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return SubscriberView{Key: key, Profile: p, Buckets: bs}, nil
}
