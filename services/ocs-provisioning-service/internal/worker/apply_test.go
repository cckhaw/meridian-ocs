package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

var lim = command.Limits{Currency: "EUR", MaxTopUp: 1_000_000, MaxAdjust: 100_000}

func newApplier(t testing.TB) (*DataApplier, *datalayer.Client, *identity.RedisDirectory) {
	t.Helper()
	rdb := redistest.Start(t)
	data := datalayer.New(rdb)
	dir := identity.NewRedisDirectory(rdb, "")
	return &DataApplier{Data: data, Dir: dir, Now: func() time.Time { return time.Unix(1_800_000_000, 0) }}, data, dir
}

func apply(t testing.TB, a Applier, c command.Command) Outcome {
	t.Helper()
	if c.Key == "" {
		c.Key = "acct-1"
	}
	if err := c.Validate(lim); err != nil {
		t.Fatalf("test command is invalid: %v", err)
	}
	out, err := a.Apply(context.Background(), &c)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func create(id, key string, ids ...command.Identifier) command.Command {
	return command.Command{ID: id, Type: command.CreateSubscriber, Key: key, Payment: "prepaid", Identifiers: ids}
}

var (
	msisdn = command.Identifier{Type: "msisdn", Value: "491701234567"}
	imsi   = command.Identifier{Type: "imsi", Value: "262011234567890"}
)

func TestCreateBindsIdentitiesAndProfile(t *testing.T) {
	a, data, dir := newApplier(t)
	out := apply(t, a, create("c1", "acct-1", msisdn, imsi))
	if out.State != status.Applied || out.Code != "applied" || out.Res.Version != 1 {
		t.Fatalf("%+v", out)
	}
	for _, id := range []identity.Identity{{Type: identity.MSISDN, Value: "491701234567"}, {Type: identity.IMSI, Value: "262011234567890"}} {
		if k, err := dir.Lookup(context.Background(), id); err != nil || k != "acct-1" {
			t.Fatalf("%v: %q %v", id, k, err)
		}
	}
	if p, err := data.GetProfile(context.Background(), "acct-1"); err != nil || p.Payment != "prepaid" || p.Status != "active" {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestRedeliveredCreateIsHarmless(t *testing.T) {
	a, _, _ := newApplier(t)
	first := apply(t, a, create("c1", "acct-1", msisdn, imsi))
	again := apply(t, a, create("c1", "acct-1", msisdn, imsi))
	if again.State != status.Applied || again.Res.Version != first.Res.Version || again.Res.Code != datalayer.ProvDuplicate {
		t.Fatalf("%+v", again)
	}
}

func TestCreateRefusesIdentitiesOwnedByAnotherSubscriberBeforeWritingAnything(t *testing.T) {
	a, data, dir := newApplier(t)
	apply(t, a, create("c1", "acct-1", msisdn))
	out := apply(t, a, create("c2", "acct-2", imsi, msisdn)) // msisdn belongs to acct-1
	if out.State != status.Failed || out.Code != "identity_conflict" || !strings.Contains(out.Message, "491701234567") {
		t.Fatalf("%+v", out)
	}
	if _, err := data.GetProfile(context.Background(), "acct-2"); !errors.Is(err, datalayer.ErrNoSubscriber) {
		t.Fatal("the subscriber must not have been created")
	}
	if _, err := dir.Lookup(context.Background(), identity.Identity{Type: identity.IMSI, Value: "262011234567890"}); !errors.Is(err, identity.ErrUnknown) {
		t.Fatal("the free identifier must not have been bound")
	}
}

func TestCreateOfAnExistingSubscriberFailsAndBindsNothing(t *testing.T) {
	a, _, dir := newApplier(t)
	apply(t, a, create("c1", "acct-1", msisdn))
	out := apply(t, a, create("c2", "acct-1", imsi)) // same key, new command
	if out.State != status.Failed || out.Code != "already_exists" {
		t.Fatalf("%+v", out)
	}
	if _, err := dir.Lookup(context.Background(), identity.Identity{Type: identity.IMSI, Value: "262011234567890"}); !errors.Is(err, identity.ErrUnknown) {
		t.Fatal("a failed create must not add identities to the existing subscriber")
	}
}

// A directory that reports free identities and then conflicts on the second
// bind, as when another command takes it in between.
type racyDir struct {
	mu       sync.Mutex
	bound    map[identity.Identity]string
	conflict identity.Identity
	unbound  []identity.Identity
}

func (d *racyDir) Lookup(context.Context, identity.Identity) (string, error) {
	return "", identity.ErrUnknown
}
func (d *racyDir) BindNew(_ context.Context, id identity.Identity, key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == d.conflict {
		return false, identity.ErrConflict
	}
	d.bound[id] = key
	return true, nil
}
func (d *racyDir) UnbindIf(_ context.Context, id identity.Identity, key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.bound[id] == key {
		delete(d.bound, id)
		d.unbound = append(d.unbound, id)
		return true, nil
	}
	return false, nil
}

func TestCreateUndoesItsOwnBindingsWhenARaceLosesAnIdentifier(t *testing.T) {
	a, _, _ := newApplier(t)
	d := &racyDir{bound: map[identity.Identity]string{}, conflict: identity.Identity{Type: identity.IMSI, Value: "262011234567890"}}
	a.Dir = d
	out := apply(t, a, create("c1", "acct-1", msisdn, imsi))
	if out.State != status.Failed || out.Code != "identity_conflict" || !strings.Contains(out.Message, "without identities") {
		t.Fatalf("%+v", out)
	}
	if len(d.bound) != 0 || len(d.unbound) != 1 {
		t.Fatalf("the identifiers bound by this attempt must be undone: bound=%v undone=%v", d.bound, d.unbound)
	}
}

func TestMoneyAndBucketCommands(t *testing.T) {
	a, data, _ := newApplier(t)
	ctx := context.Background()
	apply(t, a, create("c", "acct-1", msisdn))

	out := apply(t, a, command.Command{ID: "t1", Type: command.TopUp, BucketID: "cash", Amount: 5000, Currency: "EUR"})
	if out.State != status.Applied || out.Res.Balance != 5000 || out.Res.Version != 2 {
		t.Fatalf("%+v", out)
	}
	out = apply(t, a, command.Command{ID: "a1", Type: command.AdjustBalance, BucketID: "cash", Amount: -1200, Currency: "EUR", Reason: "refund"})
	if out.State != status.Applied || out.Res.Balance != 3800 {
		t.Fatalf("%+v", out)
	}
	out = apply(t, a, command.Command{ID: "a2", Type: command.AdjustBalance, BucketID: "cash", Amount: -9000, Currency: "EUR", Reason: "chargeback"})
	if out.State != status.Failed || out.Code != "insufficient_balance" || out.Res.Balance != 3800 || !strings.Contains(out.Message, "3800") {
		t.Fatalf("%+v", out)
	}
	out = apply(t, a, command.Command{ID: "a3", Type: command.AdjustBalance, BucketID: "cash", Amount: -9000, Currency: "EUR", Reason: "chargeback", AllowNegative: true})
	if out.State != status.Applied || out.Res.Balance != -5200 {
		t.Fatalf("%+v", out)
	}

	exp := time.Unix(1_900_000_000, 0)
	out = apply(t, a, command.Command{ID: "b1", Type: command.AddBundle, BucketID: "data", Class: "bundle", Unit: "bytes", Amount: 1_000_000, ExpiresAt: exp.UnixMilli()})
	if out.State != status.Applied || out.Res.Balance != 1_000_000 {
		t.Fatalf("%+v", out)
	}
	p := int32(5)
	apply(t, a, command.Command{ID: "b2", Type: command.AddBundle, BucketID: "promo", Class: "promo", Unit: "bytes", Amount: 10, Priority: &p})
	bs, _ := data.ListBuckets(ctx, "acct-1")
	if len(bs) != 3 || bs[0].ID != "promo" || bs[0].Order != 5 || bs[1].ID != "data" || bs[1].Order != command.DefaultBundlePriority || bs[2].ID != "cash" || bs[2].Order != command.CashPriority {
		t.Fatalf("default consumption order is promo, bundle, cash: %+v", bs)
	}
	if bs[1].Kind != "bytes" || !bs[1].ExpiresAt.Equal(exp) {
		t.Fatalf("%+v", bs[1])
	}

	out = apply(t, a, command.Command{ID: "r1", Type: command.ReloadBucket, BucketID: "data", Amount: 2_000_000, KeepExpiry: true})
	if out.State != status.Applied || out.Res.Balance != 2_000_000 || out.Res.OldBalance != 1_000_000 {
		t.Fatalf("%+v", out)
	}
	bs, _ = data.ListBuckets(ctx, "acct-1")
	if !bs[1].ExpiresAt.Equal(exp) || bs[1].Order != command.DefaultBundlePriority {
		t.Fatalf("a reload without priority or expiry keeps both: %+v", bs[1])
	}

	if out = apply(t, a, command.Command{ID: "x1", Type: command.RemoveBucket, BucketID: "data"}); out.Code != "bucket_not_empty" {
		t.Fatalf("%+v", out)
	}
	if out = apply(t, a, command.Command{ID: "x2", Type: command.RemoveBucket, BucketID: "data", Force: true}); out.State != status.Applied || out.Res.Balance != 2_000_000 {
		t.Fatalf("%+v", out)
	}
	if out = apply(t, a, command.Command{ID: "x3", Type: command.RemoveBucket, BucketID: "data"}); out.Code != "no_such_bucket" {
		t.Fatalf("%+v", out)
	}
}

func TestLifecycleCommands(t *testing.T) {
	a, data, _ := newApplier(t)
	apply(t, a, create("c", "acct-1", msisdn))
	until := time.Unix(1_800_100_000, 0)
	out := apply(t, a, command.Command{ID: "l1", Type: command.ChangeLifecycle, Target: "grace", GraceUntil: until.UnixMilli(), Reason: "balance depleted"})
	if out.State != status.Applied || out.Res.From != "active" || out.Res.To != "grace" {
		t.Fatalf("%+v", out)
	}
	if p, _ := data.GetProfile(context.Background(), "acct-1"); !p.GraceUntil.Equal(until) {
		t.Fatalf("%+v", p)
	}
	out = apply(t, a, command.Command{ID: "l2", Type: command.ChangeLifecycle, Target: "terminated"})
	if out.State != status.Applied {
		t.Fatal(out)
	}
	out = apply(t, a, command.Command{ID: "l3", Type: command.ChangeLifecycle, Target: "active"})
	if out.State != status.Failed || out.Code != "invalid_transition" || out.Message != "cannot move from terminated to active" {
		t.Fatalf("%+v", out)
	}
}

func TestFailuresOnMissingSubscribers(t *testing.T) {
	a, _, _ := newApplier(t)
	out := apply(t, a, command.Command{ID: "t1", Key: "ghost", Type: command.TopUp, BucketID: "cash", Amount: 5, Currency: "EUR"})
	if out.State != status.Failed || out.Code != "no_such_subscriber" {
		t.Fatalf("%+v", out)
	}
	apply(t, a, create("c", "acct-1", msisdn))
	apply(t, a, command.Command{ID: "t2", Type: command.AddBundle, BucketID: "data", Class: "bundle", Unit: "bytes", Amount: 1})
	out = apply(t, a, command.Command{ID: "t3", Type: command.TopUp, BucketID: "data", Amount: 5, Currency: "EUR"})
	if out.Code != "not_a_cash_bucket" {
		t.Fatalf("%+v", out)
	}
}

func TestReusedIDForDifferentContentIsAFailureNotASecondEffect(t *testing.T) {
	a, data, _ := newApplier(t)
	apply(t, a, create("c", "acct-1", msisdn))
	apply(t, a, command.Command{ID: "pay", Type: command.TopUp, BucketID: "cash", Amount: 100, Currency: "EUR"})
	out := apply(t, a, command.Command{ID: "pay", Type: command.TopUp, BucketID: "cash", Amount: 7777, Currency: "EUR"})
	if out.State != status.Failed || out.Code != "idempotency_token_reused" {
		t.Fatalf("%+v", out)
	}
	if bs, _ := data.ListBuckets(context.Background(), "acct-1"); bs[0].Balance != 100 {
		t.Fatalf("%+v", bs)
	}
}

func TestInfrastructureErrorsAreReturnedForRetry(t *testing.T) {
	a, _, _ := newApplier(t)
	boom := errors.New("redis down")
	a.Dir = failingDir{boom}
	c := create("c", "acct-1", msisdn)
	_ = c.Validate(lim)
	if _, err := a.Apply(context.Background(), &c); !errors.Is(err, boom) {
		t.Fatalf("a directory outage must be a retryable error, not a business failure: %v", err)
	}
}

type failingDir struct{ err error }

func (f failingDir) Lookup(context.Context, identity.Identity) (string, error) { return "", f.err }
func (f failingDir) BindNew(context.Context, identity.Identity, string) (bool, error) {
	return false, f.err
}
func (f failingDir) UnbindIf(context.Context, identity.Identity, string) (bool, error) {
	return false, f.err
}

func TestChargingUsesWhatProvisioningCreated(t *testing.T) {
	a, data, _ := newApplier(t)
	ctx := context.Background()
	apply(t, a, create("c", "acct-1", msisdn))
	apply(t, a, command.Command{ID: "t", Type: command.TopUp, BucketID: "cash", Amount: 10_000, Currency: "EUR"})
	apply(t, a, command.Command{ID: "b", Type: command.AddBundle, BucketID: "data", Class: "bundle", Unit: "bytes", Amount: 400})

	rates := []datalayer.Rate{{Class: datalayer.ClassBundle, Num: 1, Den: 1}, {Class: datalayer.ClassCash, Num: 2, Den: 1}}
	charge := func(mode datalayer.Mode, sid string, n uint32, req, used int64) datalayer.ItemResult {
		var resp datalayer.Response
		err := data.Charge(ctx, &datalayer.Request{SubscriberID: "acct-1", SessionID: sid, Mode: mode, ReqNo: n, Now: time.Now(), Validity: 30 * time.Second,
			Items: []datalayer.Item{{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: req, Used: used, Rates: rates}}}, &resp)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Items[0]
	}
	if it := charge(datalayer.ModeInitial, "s1", 0, 1000, 0); it.Granted != 1000 {
		t.Fatalf("%+v", it)
	}
	// Suspend through the same applier: new quota is refused at once.
	apply(t, a, command.Command{ID: "s", Type: command.ChangeLifecycle, Target: "suspended"})
	if it := charge(datalayer.ModeInitial, "s2", 0, 100, 0); it.Code != datalayer.ItemBarred {
		t.Fatalf("%+v", it)
	}
	apply(t, a, command.Command{ID: "r", Type: command.ChangeLifecycle, Target: "active"})
	if it := charge(datalayer.ModeInitial, "s3", 0, 100, 0); it.Granted != 100 {
		t.Fatalf("%+v", it)
	}
}
