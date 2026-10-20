// Package controlrpc serves a control.Controller over gRPC.
//
// Identity is the common name of the verified mTLS client certificate; there is
// no other way in. Two allow-lists decide what a caller may do:
//
//	agents     may propose, confirm, revert, and list
//	approvers  may do all of that, and also approve risky changes and freeze
//
// With authentication switched off (development only) every caller is the
// single principal "anonymous", which makes the two-person rule impossible to
// satisfy: risky parameters simply cannot be changed. That is deliberate.
package controlrpc

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

	"github.com/cckhaw/meridian-ocs/pkg/control"
	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
)

// Anonymous is the principal when authentication is off.
const Anonymous = "anonymous"

// Server implements ocsv1.ControlServiceServer.
type Server struct {
	ocsv1.UnimplementedControlServiceServer
	c         *control.Controller
	open      bool
	agents    map[string]bool
	approvers map[string]bool
}

// New builds the server. With open (auth off) every caller is Anonymous and may
// use every call except those that need a second person.
func New(c *control.Controller, open bool, agents, approvers []string) *Server {
	s := &Server{c: c, open: open, agents: map[string]bool{}, approvers: map[string]bool{}}
	for _, a := range agents {
		s.agents[a] = true
	}
	for _, a := range approvers {
		s.approvers[a] = true
	}
	return s
}

// Register adds the service to a gRPC server.
func (s *Server) Register(g *grpc.Server) { ocsv1.RegisterControlServiceServer(g, s) }

type role uint8

const (
	roleNone role = iota
	roleAgent
	roleApprover
)

func (s *Server) who(ctx context.Context) (string, role) {
	if s.open {
		return Anonymous, roleApprover
	}
	cn := ""
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			cn = tlsconf.PeerCommonName(&ti.State)
		}
	}
	switch {
	case cn == "":
		return "", roleNone
	case s.approvers[cn]:
		return cn, roleApprover
	case s.agents[cn]:
		return cn, roleAgent
	}
	return cn, roleNone
}

// authorize checks the caller has at least need, and that the request names
// this service.
func (s *Server) authorize(ctx context.Context, service string, need role) (string, error) {
	who, r := s.who(ctx)
	switch {
	case who == "":
		return "", status.Error(codes.Unauthenticated, "a client certificate is required")
	case r < need:
		return who, status.Error(codes.PermissionDenied, "this client may not perform this operation")
	case service != s.c.Service():
		return who, status.Errorf(codes.NotFound, "this process is %q, not %q", s.c.Service(), service)
	}
	return who, nil
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, control.ErrUnknownParam), errors.Is(err, control.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, control.ErrInvalidValue), errors.Is(err, control.ErrReason):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, control.ErrRateLimited):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, control.ErrBusy), errors.Is(err, control.ErrState), errors.Is(err, control.ErrFrozen):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, control.ErrSamePerson):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, control.ErrNoPrincipal):
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return status.Error(codes.Internal, "internal error")
}

func state(s control.State) ocsv1.ChangeState {
	switch s {
	case control.PendingApproval:
		return ocsv1.ChangeState_CHANGE_STATE_PENDING_APPROVAL
	case control.Applied:
		return ocsv1.ChangeState_CHANGE_STATE_APPLIED
	case control.Confirmed:
		return ocsv1.ChangeState_CHANGE_STATE_CONFIRMED
	case control.Reverted:
		return ocsv1.ChangeState_CHANGE_STATE_REVERTED
	case control.Expired:
		return ocsv1.ChangeState_CHANGE_STATE_EXPIRED
	case control.Cancelled:
		return ocsv1.ChangeState_CHANGE_STATE_CANCELLED
	case control.Failed:
		return ocsv1.ChangeState_CHANGE_STATE_FAILED
	}
	return ocsv1.ChangeState_CHANGE_STATE_UNSPECIFIED
}

func ts(t interface{ IsZero() bool }, v *timestamppb.Timestamp) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return v
}

func response(ch control.Change) *ocsv1.SetParameterResponse {
	r := &ocsv1.SetParameterResponse{ChangeId: ch.ID, State: state(ch.State), Clamped: ch.Clamped, PreviousValue: ch.Previous}
	if ch.State != control.PendingApproval {
		r.AppliedValue = ch.Applied
	}
	if !ch.RevertsAt.IsZero() {
		r.RevertsAt = timestamppb.New(ch.RevertsAt)
	}
	return r
}

// SetParameter implements ControlService.
func (s *Server) SetParameter(ctx context.Context, r *ocsv1.SetParameterRequest) (*ocsv1.SetParameterResponse, error) {
	who, err := s.authorize(ctx, r.Service, roleAgent)
	if err != nil {
		return nil, err
	}
	ch, err := s.c.Propose(who, r.Parameter, r.Value, time.Duration(r.TtlSeconds)*time.Second, r.Reason)
	if err != nil {
		return nil, toStatus(err)
	}
	return response(ch), nil
}

// ConfirmChange implements ControlService.
func (s *Server) ConfirmChange(ctx context.Context, r *ocsv1.ConfirmChangeRequest) (*ocsv1.ConfirmChangeResponse, error) {
	who, err := s.authorize(ctx, r.Service, roleAgent)
	if err != nil {
		return nil, err
	}
	if err := s.c.Confirm(r.ChangeId, who); err != nil {
		return nil, toStatus(err)
	}
	return &ocsv1.ConfirmChangeResponse{}, nil
}

// ApproveChange implements ControlService.
func (s *Server) ApproveChange(ctx context.Context, r *ocsv1.ApproveChangeRequest) (*ocsv1.SetParameterResponse, error) {
	who, err := s.authorize(ctx, r.Service, roleApprover)
	if err != nil {
		return nil, err
	}
	ch, err := s.c.Approve(r.ChangeId, who)
	if err != nil {
		return nil, toStatus(err)
	}
	return response(ch), nil
}

// RevertChange implements ControlService.
func (s *Server) RevertChange(ctx context.Context, r *ocsv1.RevertChangeRequest) (*ocsv1.RevertChangeResponse, error) {
	who, err := s.authorize(ctx, r.Service, roleAgent)
	if err != nil {
		return nil, err
	}
	if err := s.c.Revert(r.ChangeId, who); err != nil {
		return nil, toStatus(err)
	}
	return &ocsv1.RevertChangeResponse{}, nil
}

// ListParameters implements ControlService.
func (s *Server) ListParameters(ctx context.Context, r *ocsv1.ListParametersRequest) (*ocsv1.ListParametersResponse, error) {
	if _, err := s.authorize(ctx, r.Service, roleAgent); err != nil {
		return nil, err
	}
	out := &ocsv1.ListParametersResponse{Frozen: s.c.Frozen()}
	for _, p := range s.c.List() {
		out.Parameters = append(out.Parameters, &ocsv1.ParameterInfo{Name: p.Name, Description: p.Description, Kind: p.Kind.String(),
			CurrentValue: p.Current, Min: p.Min, Max: p.Max, EnumValues: p.Values, Risky: p.Risky, ActiveChangeId: p.ActiveChange})
	}
	return out, nil
}

// ListChanges implements ControlService.
func (s *Server) ListChanges(ctx context.Context, r *ocsv1.ListChangesRequest) (*ocsv1.ListChangesResponse, error) {
	if _, err := s.authorize(ctx, r.Service, roleAgent); err != nil {
		return nil, err
	}
	out := &ocsv1.ListChangesResponse{}
	for _, ch := range s.c.Changes() {
		ci := &ocsv1.ChangeInfo{ChangeId: ch.ID, Parameter: ch.Param, PreviousValue: ch.Previous, RequestedValue: ch.Requested,
			AppliedValue: ch.Applied, Clamped: ch.Clamped, ProposedBy: ch.Principal, ApprovedBy: ch.Approver, Reason: ch.Reason,
			State: state(ch.State), ProposedAt: timestamppb.New(ch.ProposedAt)}
		ci.AppliedAt = ts(ch.AppliedAt, timestamppb.New(ch.AppliedAt))
		ci.RevertsAt = ts(ch.RevertsAt, timestamppb.New(ch.RevertsAt))
		out.Changes = append(out.Changes, ci)
	}
	return out, nil
}

// Freeze implements ControlService.
func (s *Server) Freeze(ctx context.Context, r *ocsv1.FreezeRequest) (*ocsv1.FreezeResponse, error) {
	who, err := s.authorize(ctx, r.Service, roleApprover)
	if err != nil {
		return nil, err
	}
	s.c.Freeze(r.Frozen, who, r.Reason)
	return &ocsv1.FreezeResponse{}, nil
}
