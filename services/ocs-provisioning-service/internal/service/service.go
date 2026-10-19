// Package service is the logic behind the REST and gRPC APIs: validate a
// command, make its acceptance idempotent, queue it on Kafka, and answer
// status and read requests.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
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
