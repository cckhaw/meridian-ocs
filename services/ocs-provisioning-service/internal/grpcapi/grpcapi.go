// Package grpcapi serves ocs.v1.ProvisioningService. It is a thin mapping from
// the protobuf messages onto the same commands, authorisation and service
// logic as the REST API; see the comments in provisioning.proto for the
// semantics (asynchronous, idempotent, ordered per subscriber).
package grpcapi

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	cmdstatus "github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// Server implements ocsv1.ProvisioningServiceServer.
type Server struct {
	ocsv1.UnimplementedProvisioningServiceServer
	svc *service.Service
	az  *auth.Authorizer
}

// New builds the server.
func New(svc *service.Service, az *auth.Authorizer) *Server { return &Server{svc: svc, az: az} }

// Register adds the service to a grpc.Server.
func (s *Server) Register(g *grpc.Server) { ocsv1.RegisterProvisioningServiceServer(g, s) }

func (s *Server) principal(ctx context.Context) string {
	cn := ""
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			cn = tlsconf.PeerCommonName(&ti.State)
		}
	}
	return s.az.Principal(cn)
}

// submit authorises, validates and queues a command.
func (s *Server) submit(ctx context.Context, c *command.Command, err error) (*ocsv1.CommandAccepted, error) {
	if err != nil {
		return nil, toStatus(err)
	}
	principal := s.principal(ctx)
	if err := s.az.Allow(principal, c.Type); err != nil {
		return nil, toStatus(err)
	}
	c.Principal = principal
	acc, err := s.svc.Submit(ctx, c)
	if err != nil {
		return nil, toStatus(err)
	}
	return &ocsv1.CommandAccepted{CommandId: acc.CommandID, State: commandState(acc.State), Duplicate: acc.Duplicate}, nil
}

func invalid(msg string) error { return &command.ValidationError{Msg: msg} }

func ms(t *timestamppb.Timestamp) (int64, error) {
	if t == nil {
		return 0, nil
	}
	if err := t.CheckValid(); err != nil {
		return 0, invalid("invalid timestamp")
	}
	return t.AsTime().UnixMilli(), nil
}

func bucketOrCash(id string) string {
	if id == "" {
		return command.DefaultCashBucket
	}
	return id
}

func (s *Server) CreateSubscriber(ctx context.Context, r *ocsv1.CreateSubscriberRequest) (*ocsv1.CommandAccepted, error) {
	c := &command.Command{ID: r.IdempotencyKey, Type: command.CreateSubscriber, Key: r.SubscriberKey}
	switch r.PaymentType {
	case ocsv1.PaymentType_PAYMENT_TYPE_PREPAID:
		c.Payment = "prepaid"
	case ocsv1.PaymentType_PAYMENT_TYPE_POSTPAID:
		c.Payment = "postpaid"
	}
	for _, id := range r.Identifiers {
		t := ""
		switch id.Type {
		case ocsv1.IdentifierType_IDENTIFIER_TYPE_MSISDN:
			t = "msisdn"
		case ocsv1.IdentifierType_IDENTIFIER_TYPE_IMSI:
			t = "imsi"
		case ocsv1.IdentifierType_IDENTIFIER_TYPE_NAI:
			t = "nai"
		}
		c.Identifiers = append(c.Identifiers, command.Identifier{Type: t, Value: id.Value})
	}
	return s.submit(ctx, c, nil)
}

func (s *Server) TopUp(ctx context.Context, r *ocsv1.TopUpRequest) (*ocsv1.CommandAccepted, error) {
	return s.submit(ctx, &command.Command{ID: r.IdempotencyKey, Type: command.TopUp, Key: r.SubscriberKey,
		Amount: r.AmountMinorUnits, Currency: r.Currency, BucketID: bucketOrCash(r.BucketId)}, nil)
}

func (s *Server) AdjustBalance(ctx context.Context, r *ocsv1.AdjustBalanceRequest) (*ocsv1.CommandAccepted, error) {
	return s.submit(ctx, &command.Command{ID: r.IdempotencyKey, Type: command.AdjustBalance, Key: r.SubscriberKey,
		BucketID: bucketOrCash(r.BucketId), Amount: r.Delta, Currency: r.Currency, Reason: r.Reason, AllowNegative: r.AllowNegative}, nil)
}

func (s *Server) AddBundle(ctx context.Context, r *ocsv1.AddBundleRequest) (*ocsv1.CommandAccepted, error) {
	c := &command.Command{ID: r.IdempotencyKey, Type: command.AddBundle, Key: r.SubscriberKey,
		BucketID: r.BucketId, Amount: r.Amount, Priority: r.Priority}
	switch r.BucketClass {
	case ocsv1.BucketClass_BUCKET_CLASS_PROMO:
		c.Class = "promo"
	case ocsv1.BucketClass_BUCKET_CLASS_BUNDLE:
		c.Class = "bundle"
	}
	switch r.UnitKind {
	case ocsv1.UnitKind_UNIT_KIND_BYTES:
		c.Unit = "bytes"
	case ocsv1.UnitKind_UNIT_KIND_SECONDS:
		c.Unit = "seconds"
	case ocsv1.UnitKind_UNIT_KIND_EVENTS:
		c.Unit = "events"
	}
	var err error
	c.ExpiresAt, err = ms(r.ExpiresAt)
	return s.submit(ctx, c, err)
}

func (s *Server) AttachOffering(ctx context.Context, r *ocsv1.AttachOfferingRequest) (*ocsv1.CommandAccepted, error) {
	return s.submit(ctx, &command.Command{ID: r.IdempotencyKey, Type: command.AttachOffering, Key: r.SubscriberKey,
		Offering: r.OfferingId, Renew: r.Renew}, nil)
}

func (s *Server) ReloadBucket(ctx context.Context, r *ocsv1.ReloadBucketRequest) (*ocsv1.CommandAccepted, error) {
	c := &command.Command{ID: r.IdempotencyKey, Type: command.ReloadBucket, Key: r.SubscriberKey,
		BucketID: r.BucketId, Amount: r.Amount, KeepExpiry: r.KeepExpiry, Priority: r.Priority}
	var err error
	c.ExpiresAt, err = ms(r.ExpiresAt)
	return s.submit(ctx, c, err)
}

func (s *Server) RemoveBucket(ctx context.Context, r *ocsv1.RemoveBucketRequest) (*ocsv1.CommandAccepted, error) {
	return s.submit(ctx, &command.Command{ID: r.IdempotencyKey, Type: command.RemoveBucket, Key: r.SubscriberKey,
		BucketID: r.BucketId, Force: r.Force}, nil)
}

func (s *Server) ChangeLifecycle(ctx context.Context, r *ocsv1.ChangeLifecycleRequest) (*ocsv1.CommandAccepted, error) {
	c := &command.Command{ID: r.IdempotencyKey, Type: command.ChangeLifecycle, Key: r.SubscriberKey, Reason: r.Reason}
	switch r.Target {
	case ocsv1.LifecycleState_LIFECYCLE_STATE_ACTIVE:
		c.Target = "active"
	case ocsv1.LifecycleState_LIFECYCLE_STATE_GRACE_PERIOD:
		c.Target = "grace"
	case ocsv1.LifecycleState_LIFECYCLE_STATE_SUSPENDED:
		c.Target = "suspended"
	case ocsv1.LifecycleState_LIFECYCLE_STATE_TERMINATED:
		c.Target = "terminated"
	}
	var err error
	c.GraceUntil, err = ms(r.GraceUntil)
	return s.submit(ctx, c, err)
}

func (s *Server) GetCommand(ctx context.Context, r *ocsv1.GetCommandRequest) (*ocsv1.CommandStatus, error) {
	if err := s.az.Read(s.principal(ctx)); err != nil {
		return nil, toStatus(err)
	}
	rec, err := s.svc.GetCommand(ctx, r.CommandId)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &ocsv1.CommandStatus{CommandId: rec.ID, Type: string(rec.Type), SubscriberKey: rec.Key, State: commandState(rec.State),
		Outcome: rec.Outcome, Message: rec.Message, Balance: rec.Balance, OldBalance: rec.OldBalance,
		FromState: rec.From, ToState: rec.To, Version: rec.Version, AcceptedAt: timestamppb.New(rec.AcceptedAt)}
	if rec.Type == command.AttachOffering {
		out.FromState, out.ToState, out.FromOffering, out.ToOffering = "", "", rec.From, rec.To
	}
	if !rec.CompletedAt.IsZero() {
		out.CompletedAt = timestamppb.New(rec.CompletedAt)
	}
	return out, nil
}

func (s *Server) GetSubscriber(ctx context.Context, r *ocsv1.GetSubscriberRequest) (*ocsv1.Subscriber, error) {
	if err := s.az.Read(s.principal(ctx)); err != nil {
		return nil, toStatus(err)
	}
	v, err := s.svc.GetSubscriber(ctx, r.SubscriberKey)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &ocsv1.Subscriber{SubscriberKey: v.Key, Version: v.Profile.Version, OfferingId: v.Profile.Offering,
		CreatedAt: tsOrNil(v.Profile.CreatedAt), StatusAt: tsOrNil(v.Profile.StatusAt), GraceUntil: tsOrNil(v.Profile.GraceUntil)}
	switch v.Profile.Payment {
	case "prepaid":
		out.PaymentType = ocsv1.PaymentType_PAYMENT_TYPE_PREPAID
	case "postpaid":
		out.PaymentType = ocsv1.PaymentType_PAYMENT_TYPE_POSTPAID
	}
	switch v.Profile.Status {
	case "active":
		out.Status = ocsv1.LifecycleState_LIFECYCLE_STATE_ACTIVE
	case "grace":
		out.Status = ocsv1.LifecycleState_LIFECYCLE_STATE_GRACE_PERIOD
	case "suspended":
		out.Status = ocsv1.LifecycleState_LIFECYCLE_STATE_SUSPENDED
	case "terminated":
		out.Status = ocsv1.LifecycleState_LIFECYCLE_STATE_TERMINATED
	}
	for _, b := range v.Buckets {
		pb := &ocsv1.Bucket{Id: b.ID, Kind: b.Kind, Balance: b.Balance, Priority: b.Order}
		switch string(b.Class) {
		case "promo":
			pb.BucketClass = ocsv1.BucketClass_BUCKET_CLASS_PROMO
		case "bundle":
			pb.BucketClass = ocsv1.BucketClass_BUCKET_CLASS_BUNDLE
		case "cash":
			pb.BucketClass = ocsv1.BucketClass_BUCKET_CLASS_CASH
		}
		if !b.ExpiresAt.IsZero() {
			pb.ExpiresAt = timestamppb.New(b.ExpiresAt)
		}
		out.Buckets = append(out.Buckets, pb)
	}
	return out, nil
}

func tsOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func commandState(s cmdstatus.State) ocsv1.CommandState {
	switch s {
	case cmdstatus.Accepted:
		return ocsv1.CommandState_COMMAND_STATE_ACCEPTED
	case cmdstatus.Applied:
		return ocsv1.CommandState_COMMAND_STATE_APPLIED
	case cmdstatus.Failed:
		return ocsv1.CommandState_COMMAND_STATE_FAILED
	}
	return ocsv1.CommandState_COMMAND_STATE_UNSPECIFIED
}

// toStatus maps an error onto a gRPC status without leaking internals.
func toStatus(err error) error {
	switch {
	case command.IsValidation(err):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, auth.ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, "a client certificate is required")
	case errors.Is(err, auth.ErrForbidden):
		return status.Error(codes.PermissionDenied, "this client may not perform this operation")
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, service.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "this idempotency key was already used for a different request")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, service.ErrNoCatalog):
		return status.Error(codes.Unavailable, "no tariff catalog is available to resolve the offering; retry later")
	case errors.Is(err, service.ErrUnavailable):
		return status.Error(codes.Unavailable, "temporarily unavailable, retry with the same idempotency key")
	}
	return status.Error(codes.Internal, "internal error")
}
