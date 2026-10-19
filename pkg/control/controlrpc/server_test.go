package controlrpc

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/cckhaw/meridian-ocs/pkg/control"
	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

type rig struct {
	pki  *tlstest.PKI
	addr string
	ctl  *control.Controller

	mu    sync.Mutex
	rate  float64
	mode  string
	audit []control.Event
}

func newRig(t *testing.T, open bool) *rig {
	t.Helper()
	r := &rig{pki: tlstest.New(t), rate: 1000, mode: "auto"}
	r.ctl = control.New(control.Config{Service: "ocs-signaling-gateway", ChangesPerMinute: 4,
		Audit: func(e control.Event) { r.mu.Lock(); r.audit = append(r.audit, e); r.mu.Unlock() }})
	t.Cleanup(r.ctl.Close)
	if err := r.ctl.Register(control.Param{Name: "admission.rate", Kind: control.Float, Min: 100, Max: 100000,
		Description: "admitted requests per second",
		Get:         func() string { r.mu.Lock(); defer r.mu.Unlock(); return strconv.FormatFloat(r.rate, 'g', -1, 64) },
		Set: func(v string) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.rate, _ = strconv.ParseFloat(v, 64)
			return nil
		}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ctl.Register(control.Param{Name: "breaker.mode", Kind: control.Enum, Values: []string{"auto", "open"}, Risky: true,
		Get: func() string { r.mu.Lock(); defer r.mu.Unlock(); return r.mode },
		Set: func(v string) error { r.mu.Lock(); defer r.mu.Unlock(); r.mode = v; return nil }}); err != nil {
		t.Fatal(err)
	}

	srv := New(r.ctl, open, []string{"agent-a", "agent-b"}, []string{"oncall"})
	var opts []grpc.ServerOption
	if !open {
		tc, err := tlsconf.Server(r.pki.CertFile, r.pki.KeyFile, r.pki.CAFile, "h2")
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tc)))
	}
	g := grpc.NewServer(opts...)
	srv.Register(g)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(ln)
	t.Cleanup(g.Stop)
	r.addr = ln.Addr().String()
	return r
}

func (r *rig) client(t *testing.T, cn string) ocsv1.ControlServiceClient {
	t.Helper()
	var opt grpc.DialOption
	if cn == "" {
		opt = grpc.WithTransportCredentials(insecureCreds())
	} else {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(r.pki.ClientTLS(t, cn)))
	}
	conn, err := grpc.NewClient("passthrough:///"+r.addr, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return ocsv1.NewControlServiceClient(conn)
}

func ctx5() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func code(err error) codes.Code { return status.Code(err) }

const svc = "ocs-signaling-gateway"

func TestAgentFlowOverMutualTLS(t *testing.T) {
	r := newRig(t, false)
	agent := r.client(t, "agent-a")
	ctx, cancel := ctx5()
	defer cancel()

	resp, err := agent.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "5000000", Reason: "storm"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.State != ocsv1.ChangeState_CHANGE_STATE_APPLIED || resp.AppliedValue != "100000" || !resp.Clamped || resp.PreviousValue != "1000" || resp.RevertsAt == nil {
		t.Fatalf("%+v", resp)
	}
	if r.rate != 100000 {
		t.Fatalf("not applied: %v", r.rate)
	}

	lp, err := agent.ListParameters(ctx, &ocsv1.ListParametersRequest{Service: svc})
	if err != nil || len(lp.Parameters) != 2 || lp.Parameters[0].Name != "admission.rate" || lp.Parameters[0].CurrentValue != "100000" ||
		lp.Parameters[0].ActiveChangeId != resp.ChangeId || lp.Parameters[0].Max != 100000 || !lp.Parameters[1].Risky || lp.Frozen {
		t.Fatalf("%+v %v", lp, err)
	}
	if _, err := agent.ConfirmChange(ctx, &ocsv1.ConfirmChangeRequest{Service: svc, ChangeId: resp.ChangeId}); err != nil {
		t.Fatal(err)
	}
	lc, err := agent.ListChanges(ctx, &ocsv1.ListChangesRequest{Service: svc})
	if err != nil || len(lc.Changes) != 1 || lc.Changes[0].State != ocsv1.ChangeState_CHANGE_STATE_CONFIRMED || lc.Changes[0].ProposedBy != "agent-a" || lc.Changes[0].Reason != "storm" {
		t.Fatalf("%+v %v", lc, err)
	}
	// Revert of a confirmed change is refused; a fresh one can be reverted.
	if _, err := agent.RevertChange(ctx, &ocsv1.RevertChangeRequest{Service: svc, ChangeId: resp.ChangeId}); code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
	next, _ := agent.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "trial"})
	if _, err := agent.RevertChange(ctx, &ocsv1.RevertChangeRequest{Service: svc, ChangeId: next.ChangeId}); err != nil || r.rate != 100000 {
		t.Fatalf("%v rate=%v", err, r.rate)
	}
}

func TestTwoPersonRuleOverTheWire(t *testing.T) {
	r := newRig(t, false)
	a, b, oncall := r.client(t, "agent-a"), r.client(t, "agent-b"), r.client(t, "oncall")
	ctx, cancel := ctx5()
	defer cancel()

	p, err := a.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "breaker.mode", Value: "open", Reason: "redis flapping"})
	if err != nil || p.State != ocsv1.ChangeState_CHANGE_STATE_PENDING_APPROVAL || p.AppliedValue != "" || r.mode != "auto" {
		t.Fatalf("%+v %v mode=%s", p, err, r.mode)
	}
	// Another agent is not an approver; the proposer cannot approve itself even as an approver.
	if _, err := b.ApproveChange(ctx, &ocsv1.ApproveChangeRequest{Service: svc, ChangeId: p.ChangeId}); code(err) != codes.PermissionDenied {
		t.Fatalf("an agent approved: %v", err)
	}
	if r.mode != "auto" {
		t.Fatal("applied without approval")
	}
	op, err := oncall.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "x"})
	if err != nil || op.State != ocsv1.ChangeState_CHANGE_STATE_APPLIED {
		t.Fatalf("%+v %v", op, err)
	}
	// oncall proposes a risky change and tries to approve it: refused.
	if _, err := a.RevertChange(ctx, &ocsv1.RevertChangeRequest{Service: svc, ChangeId: p.ChangeId}); err != nil {
		t.Fatal(err)
	}
	self, _ := oncall.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "breaker.mode", Value: "open", Reason: "x"})
	if _, err := oncall.ApproveChange(ctx, &ocsv1.ApproveChangeRequest{Service: svc, ChangeId: self.ChangeId}); code(err) != codes.PermissionDenied {
		t.Fatalf("self-approval: %v", err)
	}
	// agent-a proposes, oncall approves.
	oncall.RevertChange(ctx, &ocsv1.RevertChangeRequest{Service: svc, ChangeId: self.ChangeId})
	q, _ := a.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "breaker.mode", Value: "open", Reason: "again"})
	ap, err := oncall.ApproveChange(ctx, &ocsv1.ApproveChangeRequest{Service: svc, ChangeId: q.ChangeId})
	if err != nil || ap.State != ocsv1.ChangeState_CHANGE_STATE_APPLIED || ap.AppliedValue != "open" || r.mode != "open" {
		t.Fatalf("%+v %v", ap, err)
	}
}

func TestAuthorizationMatrix(t *testing.T) {
	r := newRig(t, false)
	ctx, cancel := ctx5()
	defer cancel()
	stranger := r.client(t, "stranger")
	agent := r.client(t, "agent-a")
	oncall := r.client(t, "oncall")

	for name, call := range map[string]func(ocsv1.ControlServiceClient) error{
		"set": func(c ocsv1.ControlServiceClient) error {
			_, e := c.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "x"})
			return e
		},
		"list": func(c ocsv1.ControlServiceClient) error {
			_, e := c.ListParameters(ctx, &ocsv1.ListParametersRequest{Service: svc})
			return e
		},
		"changes": func(c ocsv1.ControlServiceClient) error {
			_, e := c.ListChanges(ctx, &ocsv1.ListChangesRequest{Service: svc})
			return e
		},
		"freeze": func(c ocsv1.ControlServiceClient) error {
			_, e := c.Freeze(ctx, &ocsv1.FreezeRequest{Service: svc, Frozen: true, Reason: "x"})
			return e
		},
	} {
		if code(call(stranger)) != codes.PermissionDenied {
			t.Errorf("%s: a client with a valid certificate that is on no list was served", name)
		}
	}
	if code(func() error { _, e := agent.Freeze(ctx, &ocsv1.FreezeRequest{Service: svc, Frozen: true}); return e }()) != codes.PermissionDenied {
		t.Error("an agent froze the control plane")
	}
	if r.ctl.Frozen() || r.rate != 1000 {
		t.Fatal("a refused call had an effect")
	}
	// An approver may freeze, which stops agents; unfreezing resumes.
	if _, err := oncall.Freeze(ctx, &ocsv1.FreezeRequest{Service: svc, Frozen: true, Reason: "incident"}); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "x"}); code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
	if lp, _ := agent.ListParameters(ctx, &ocsv1.ListParametersRequest{Service: svc}); !lp.Frozen {
		t.Fatal("frozen not reported")
	}
	oncall.Freeze(ctx, &ocsv1.FreezeRequest{Service: svc, Frozen: false, Reason: "over"})

	// A client with no certificate cannot connect at all.
	anon := r.client(t, "")
	if _, err := anon.ListParameters(ctx, &ocsv1.ListParametersRequest{Service: svc}); err == nil {
		t.Fatal("a client without a certificate was served")
	}
}

func TestRequestErrorsMapToStatusCodes(t *testing.T) {
	r := newRig(t, false)
	agent := r.client(t, "agent-a")
	ctx, cancel := ctx5()
	defer cancel()
	set := func(service, param, val, reason string) error {
		_, err := agent.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: service, Parameter: param, Value: val, Reason: reason})
		return err
	}
	for name, c := range map[string]struct {
		err  error
		want codes.Code
	}{
		"other service": {set("ocs-rating-engine", "admission.rate", "2000", "x"), codes.NotFound},
		"unknown param": {set(svc, "engine.workers", "64", "x"), codes.NotFound},
		"not a number":  {set(svc, "admission.rate", "fast", "x"), codes.InvalidArgument},
		"no reason":     {set(svc, "admission.rate", "2000", ""), codes.InvalidArgument},
		"enum value":    {set(svc, "breaker.mode", "half", "x"), codes.InvalidArgument},
	} {
		if code(c.err) != c.want {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
	if _, err := agent.ConfirmChange(ctx, &ocsv1.ConfirmChangeRequest{Service: svc, ChangeId: "chg-none"}); code(err) != codes.NotFound {
		t.Errorf("%v", err)
	}
	// Rate limit: 4 per minute, and the invalid ones above already used some.
	var limited bool
	for i := 0; i < 8 && !limited; i++ {
		_, err := agent.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "x"})
		if code(err) == codes.ResourceExhausted {
			limited = true
		}
	}
	if !limited {
		t.Error("the per-principal rate limit never applied")
	}
}

func TestOpenModeCannotSatisfyTheTwoPersonRule(t *testing.T) {
	r := newRig(t, true)
	c := r.client(t, "") // no TLS in open mode
	ctx, cancel := ctx5()
	defer cancel()
	if ok, err := c.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "admission.rate", Value: "2000", Reason: "dev"}); err != nil || ok.State != ocsv1.ChangeState_CHANGE_STATE_APPLIED {
		t.Fatalf("%+v %v", ok, err)
	}
	p, err := c.SetParameter(ctx, &ocsv1.SetParameterRequest{Service: svc, Parameter: "breaker.mode", Value: "open", Reason: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApproveChange(ctx, &ocsv1.ApproveChangeRequest{Service: svc, ChangeId: p.ChangeId}); code(err) != codes.PermissionDenied {
		t.Fatalf("with one anonymous principal a risky change must stay unapprovable: %v", err)
	}
	if r.mode != "auto" {
		t.Fatal("applied")
	}
}
