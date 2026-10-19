package grpcapi

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	cmdstatus "github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

type capturingProducer struct {
	mu   sync.Mutex
	cmds []*command.Command
}

func (p *capturingProducer) Produce(_ context.Context, _, _ string, value []byte, _ map[string]string) error {
	c, err := command.Decode(value)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.cmds = append(p.cmds, c)
	p.mu.Unlock()
	return nil
}

func (p *capturingProducer) last() *command.Command {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.cmds) == 0 {
		return nil
	}
	return p.cmds[len(p.cmds)-1]
}

type rig struct {
	prod *capturingProducer
	st   *cmdstatus.Store
	data *datalayer.Client
	addr string
	pki  *tlstest.PKI
}

var lim = command.Limits{Currency: "EUR", MaxTopUp: 100_000, MaxAdjust: 5_000}

// newRig serves over mutual TLS when pki is true, plaintext otherwise.
func newRig(t *testing.T, az *auth.Authorizer, mtls bool) *rig {
	t.Helper()
	rdb := redistest.Start(t)
	g := &rig{prod: &capturingProducer{}, st: cmdstatus.New(rdb, "", 0), data: datalayer.New(rdb)}
	svc := service.New(service.Config{Limits: lim}, g.st, g.prod, g.data)
	var opts []grpc.ServerOption
	if mtls {
		g.pki = tlstest.New(t)
		cfg, err := tlsconf.Server(g.pki.CertFile, g.pki.KeyFile, g.pki.CAFile, "h2")
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	srv := grpc.NewServer(opts...)
	New(svc, az).Register(srv)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	g.addr = ln.Addr().String()
	return g
}

func (g *rig) client(t *testing.T, cn string) ocsv1.ProvisioningServiceClient {
	t.Helper()
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if g.pki != nil {
		creds = credentials.NewTLS(g.pki.ClientTLS(t, cn))
	}
	conn, err := grpc.NewClient("passthrough:///"+g.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return ocsv1.NewProvisioningServiceClient(conn)
}

func ctx5() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func code(err error) codes.Code { return status.Code(err) }

func TestEveryRPCQueuesTheRightCommand(t *testing.T) {
	g := newRig(t, auth.Off(), false)
	c := g.client(t, "")
	ctx, cancel := ctx5()
	defer cancel()
	exp := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	prio := int32(7)

	type call func() (*ocsv1.CommandAccepted, error)
	tests := []struct {
		name  string
		call  call
		check func(*command.Command) bool
	}{
		{"create", func() (*ocsv1.CommandAccepted, error) {
			return c.CreateSubscriber(ctx, &ocsv1.CreateSubscriberRequest{IdempotencyKey: "r-1", SubscriberKey: "acct-1",
				PaymentType: ocsv1.PaymentType_PAYMENT_TYPE_POSTPAID, Identifiers: []*ocsv1.Identifier{
					{Type: ocsv1.IdentifierType_IDENTIFIER_TYPE_MSISDN, Value: "491701234567"},
					{Type: ocsv1.IdentifierType_IDENTIFIER_TYPE_IMSI, Value: "262011234567890"},
					{Type: ocsv1.IdentifierType_IDENTIFIER_TYPE_NAI, Value: "user@example.com"}}})
		}, func(c *command.Command) bool {
			return c.Type == command.CreateSubscriber && c.Payment == "postpaid" && len(c.Identifiers) == 3 &&
				c.Identifiers[0].Type == "msisdn" && c.Identifiers[1].Type == "imsi" && c.Identifiers[2].Type == "nai"
		}},
		{"top-up defaults to cash", func() (*ocsv1.CommandAccepted, error) {
			return c.TopUp(ctx, &ocsv1.TopUpRequest{IdempotencyKey: "r-2", SubscriberKey: "acct-1", AmountMinorUnits: 500, Currency: "EUR"})
		}, func(c *command.Command) bool {
			return c.Type == command.TopUp && c.Amount == 500 && c.BucketID == "cash"
		}},
		{"adjust", func() (*ocsv1.CommandAccepted, error) {
			return c.AdjustBalance(ctx, &ocsv1.AdjustBalanceRequest{IdempotencyKey: "r-3", SubscriberKey: "acct-1", Delta: -50, Currency: "EUR", Reason: "goodwill", AllowNegative: true})
		}, func(c *command.Command) bool {
			return c.Type == command.AdjustBalance && c.Amount == -50 && c.AllowNegative && c.Reason == "goodwill"
		}},
		{"bundle", func() (*ocsv1.CommandAccepted, error) {
			return c.AddBundle(ctx, &ocsv1.AddBundleRequest{IdempotencyKey: "r-4", SubscriberKey: "acct-1", BucketId: "data",
				BucketClass: ocsv1.BucketClass_BUCKET_CLASS_PROMO, UnitKind: ocsv1.UnitKind_UNIT_KIND_SECONDS, Amount: 60,
				ExpiresAt: timestamppb.New(exp), Priority: &prio})
		}, func(c *command.Command) bool {
			return c.Type == command.AddBundle && c.Class == "promo" && c.Unit == "seconds" && c.ExpiresAt == exp.UnixMilli() && *c.Priority == 7
		}},
		{"reload", func() (*ocsv1.CommandAccepted, error) {
			return c.ReloadBucket(ctx, &ocsv1.ReloadBucketRequest{IdempotencyKey: "r-5", SubscriberKey: "acct-1", BucketId: "data", Amount: 9, KeepExpiry: true})
		}, func(c *command.Command) bool {
			return c.Type == command.ReloadBucket && c.Amount == 9 && c.KeepExpiry && c.Priority == nil
		}},
		{"remove", func() (*ocsv1.CommandAccepted, error) {
			return c.RemoveBucket(ctx, &ocsv1.RemoveBucketRequest{IdempotencyKey: "r-6", SubscriberKey: "acct-1", BucketId: "data", Force: true})
		}, func(c *command.Command) bool { return c.Type == command.RemoveBucket && c.Force }},
		{"lifecycle", func() (*ocsv1.CommandAccepted, error) {
			return c.ChangeLifecycle(ctx, &ocsv1.ChangeLifecycleRequest{IdempotencyKey: "r-7", SubscriberKey: "acct-1",
				Target: ocsv1.LifecycleState_LIFECYCLE_STATE_GRACE_PERIOD, GraceUntil: timestamppb.New(exp), Reason: "low"})
		}, func(c *command.Command) bool {
			return c.Type == command.ChangeLifecycle && c.Target == "grace" && c.GraceUntil == exp.UnixMilli() && c.Reason == "low"
		}},
	}
	for _, tc := range tests {
		acc, err := tc.call()
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		last := g.prod.last()
		if acc.State != ocsv1.CommandState_COMMAND_STATE_ACCEPTED || acc.Duplicate || last == nil || acc.CommandId != last.ID || !tc.check(last) {
			t.Errorf("%s: %+v queued %+v", tc.name, acc, last)
		}
	}
}

func TestIdempotencyOverGRPC(t *testing.T) {
	g := newRig(t, auth.Off(), false)
	c := g.client(t, "")
	ctx, cancel := ctx5()
	defer cancel()
	req := &ocsv1.TopUpRequest{IdempotencyKey: "pay-1", SubscriberKey: "acct-1", AmountMinorUnits: 500, Currency: "EUR"}
	if _, err := c.TopUp(ctx, req); err != nil {
		t.Fatal(err)
	}
	again, err := c.TopUp(ctx, req)
	if err != nil || !again.Duplicate {
		t.Fatalf("%+v %v", again, err)
	}
	req.AmountMinorUnits = 900
	if _, err := c.TopUp(ctx, req); code(err) != codes.AlreadyExists {
		t.Fatalf("%v", err)
	}
}

func TestValidationErrorsAreInvalidArgument(t *testing.T) {
	g := newRig(t, auth.Off(), false)
	c := g.client(t, "")
	ctx, cancel := ctx5()
	defer cancel()
	bad := []error{
		call(c.TopUp(ctx, &ocsv1.TopUpRequest{SubscriberKey: "a", AmountMinorUnits: 5, Currency: "EUR"})), // no key
		call(c.TopUp(ctx, &ocsv1.TopUpRequest{IdempotencyKey: "k", SubscriberKey: "a", AmountMinorUnits: -5, Currency: "EUR"})),
		call(c.TopUp(ctx, &ocsv1.TopUpRequest{IdempotencyKey: "k", SubscriberKey: "a", AmountMinorUnits: 5, Currency: "USD"})),
		call(c.CreateSubscriber(ctx, &ocsv1.CreateSubscriberRequest{IdempotencyKey: "k", SubscriberKey: "a"})),             // unspecified payment type
		call(c.AddBundle(ctx, &ocsv1.AddBundleRequest{IdempotencyKey: "k", SubscriberKey: "a", BucketId: "d", Amount: 1})), // unspecified class and unit
		call(c.ChangeLifecycle(ctx, &ocsv1.ChangeLifecycleRequest{IdempotencyKey: "k", SubscriberKey: "a"})),               // unspecified target
		call(c.AddBundle(ctx, &ocsv1.AddBundleRequest{IdempotencyKey: "k", SubscriberKey: "a", BucketId: "d", Amount: 1,
			BucketClass: ocsv1.BucketClass_BUCKET_CLASS_BUNDLE, UnitKind: ocsv1.UnitKind_UNIT_KIND_BYTES,
			ExpiresAt: &timestamppb.Timestamp{Seconds: 1 << 60}})), // not a valid timestamp
	}
	for i, err := range bad {
		if code(err) != codes.InvalidArgument {
			t.Errorf("case %d: %v", i, err)
		}
	}
	if g.prod.last() != nil {
		t.Fatal("invalid requests were queued")
	}
}

func call(_ *ocsv1.CommandAccepted, err error) error { return err }

func TestAuthorisationOverMutualTLS(t *testing.T) {
	g := newRig(t, auth.New([]string{"crm"}, []string{"care"}), true)
	ctx, cancel := ctx5()
	defer cancel()
	top := &ocsv1.TopUpRequest{IdempotencyKey: "k1", SubscriberKey: "a", AmountMinorUnits: 5, Currency: "EUR"}
	adj := &ocsv1.AdjustBalanceRequest{IdempotencyKey: "k2", SubscriberKey: "a", Delta: -1, Currency: "EUR", Reason: "x"}
	life := &ocsv1.ChangeLifecycleRequest{IdempotencyKey: "k3", SubscriberKey: "a", Target: ocsv1.LifecycleState_LIFECYCLE_STATE_TERMINATED}

	// A client with no certificate cannot even connect.
	if _, err := g.client(t, "").TopUp(ctx, top); code(err) != codes.Unavailable {
		t.Fatalf("no certificate: %v", err)
	}
	// An unlisted client is authenticated but forbidden.
	if _, err := g.client(t, "stranger").TopUp(ctx, top); code(err) != codes.PermissionDenied {
		t.Fatalf("stranger: %v", err)
	}
	crm := g.client(t, "crm")
	if _, err := crm.TopUp(ctx, top); err != nil {
		t.Fatalf("crm top-up: %v", err)
	}
	if _, err := crm.AdjustBalance(ctx, adj); code(err) != codes.PermissionDenied {
		t.Fatalf("a standard caller must not adjust balances: %v", err)
	}
	if _, err := crm.ChangeLifecycle(ctx, life); code(err) != codes.PermissionDenied {
		t.Fatalf("a standard caller must not terminate: %v", err)
	}
	care := g.client(t, "care")
	if _, err := care.AdjustBalance(ctx, adj); err != nil {
		t.Fatalf("care adjust: %v", err)
	}
	if _, err := care.ChangeLifecycle(ctx, life); err != nil {
		t.Fatalf("care lifecycle: %v", err)
	}
	if last := g.prod.last(); last.Principal != "care" || last.Type != command.ChangeLifecycle {
		t.Fatalf("the caller is recorded on the command: %+v", last)
	}
	// Reads need only an authenticated caller.
	if _, err := crm.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "k1"}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := crm.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "missing"}); code(err) != codes.NotFound {
		t.Fatalf("%v", err)
	}
}

func TestReads(t *testing.T) {
	g := newRig(t, auth.Off(), false)
	c := g.client(t, "")
	ctx, cancel := ctx5()
	defer cancel()
	prov := func(op datalayer.ProvisionOp) {
		t.Helper()
		op.Now, op.Fingerprint = time.Unix(1_800_000_000, 0), op.Token
		if r, err := g.data.Provision(ctx, op); err != nil || r.Code != datalayer.ProvApplied {
			t.Fatalf("%+v %v", r, err)
		}
	}
	prov(datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: "acct-9", Token: "c", Payment: "prepaid"})
	prov(datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "acct-9", Token: "t", BucketID: "cash", Amount: 750, Order: 1000})
	prov(datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "acct-9", Token: "l", Target: "suspended"})

	s, err := c.GetSubscriber(ctx, &ocsv1.GetSubscriberRequest{SubscriberKey: "acct-9"})
	if err != nil || s.Status != ocsv1.LifecycleState_LIFECYCLE_STATE_SUSPENDED || s.PaymentType != ocsv1.PaymentType_PAYMENT_TYPE_PREPAID ||
		s.Version != 3 || len(s.Buckets) != 1 || s.Buckets[0].Balance != 750 || s.Buckets[0].BucketClass != ocsv1.BucketClass_BUCKET_CLASS_CASH ||
		s.GraceUntil != nil || s.CreatedAt == nil {
		t.Fatalf("%+v %v", s, err)
	}
	if _, err := c.GetSubscriber(ctx, &ocsv1.GetSubscriberRequest{SubscriberKey: "nobody"}); code(err) != codes.NotFound {
		t.Fatalf("%v", err)
	}

	if _, err := c.TopUp(ctx, &ocsv1.TopUpRequest{IdempotencyKey: "pay-9", SubscriberKey: "acct-9", AmountMinorUnits: 5, Currency: "EUR"}); err != nil {
		t.Fatal(err)
	}
	st, err := c.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "pay-9"})
	if err != nil || st.State != ocsv1.CommandState_COMMAND_STATE_ACCEPTED || st.Type != "top_up" || st.CompletedAt != nil {
		t.Fatalf("%+v %v", st, err)
	}
	g.st.Complete(ctx, "pay-9", cmdstatus.Result{State: cmdstatus.Applied, Outcome: "applied", Balance: 755, Version: 4})
	st, _ = c.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "pay-9"})
	if st.State != ocsv1.CommandState_COMMAND_STATE_APPLIED || st.Balance != 755 || st.Version != 4 || st.CompletedAt == nil {
		t.Fatalf("%+v", st)
	}
}
