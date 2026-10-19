package identity_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
)

func TestRedisDirectoryBindLookupUnbind(t *testing.T) {
	rdb := redistest.Start(t)
	d := identity.NewRedisDirectory(rdb, "")
	ctx := context.Background()
	m := identity.Identity{Type: identity.MSISDN, Value: "491701234567"}
	i := identity.Identity{Type: identity.IMSI, Value: "262011234567890"}

	if _, err := d.Lookup(ctx, m); !errors.Is(err, identity.ErrUnknown) {
		t.Fatal(err)
	}
	if err := d.Bind(ctx, m, "acct-1"); err != nil {
		t.Fatal(err)
	}
	if err := d.Bind(ctx, i, "acct-1"); err != nil {
		t.Fatal(err)
	}
	if err := d.Bind(ctx, m, "acct-1"); err != nil {
		t.Fatalf("binding the same key again must be idempotent: %v", err)
	}
	if k, err := d.Lookup(ctx, identity.Identity{Type: identity.MSISDN, Value: "+491701234567"}); err != nil || k != "acct-1" {
		t.Fatalf("lookup must normalise: %q %v", k, err)
	}
	// The documented key layout, so operators can seed it by hand.
	if v, _ := rdb.Get(ctx, "id:msisdn:491701234567").Result(); v != "acct-1" {
		t.Fatalf("layout: %q", v)
	}

	if err := d.Bind(ctx, m, "acct-2"); !errors.Is(err, identity.ErrConflict) {
		t.Fatalf("an identity owned by another subscriber must not be taken silently: %v", err)
	}
	if k, _ := d.Lookup(ctx, m); k != "acct-1" {
		t.Fatalf("a refused bind must not change anything: %q", k)
	}
	if moved, err := d.Rebind(ctx, i, "acct-2"); err != nil || !moved {
		t.Fatalf("SIM swap: %v %v", moved, err)
	}
	if moved, err := d.Rebind(ctx, i, "acct-2"); err != nil || moved {
		t.Fatalf("rebinding to the same key is not a move: %v %v", moved, err)
	}
	if k, _ := d.Lookup(ctx, i); k != "acct-2" {
		t.Fatal(k)
	}

	if err := d.Unbind(ctx, m); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Lookup(ctx, m); !errors.Is(err, identity.ErrUnknown) {
		t.Fatal(err)
	}
}

func TestRedisDirectoryRejectsBadInput(t *testing.T) {
	d := identity.NewRedisDirectory(redistest.Start(t), "")
	ctx := context.Background()
	good := identity.Identity{Type: identity.MSISDN, Value: "491701234567"}
	if err := d.Bind(ctx, identity.Identity{Type: identity.MSISDN, Value: "12"}, "acct"); !errors.Is(err, identity.ErrInvalid) {
		t.Error(err)
	}
	for _, key := range []string{"", "a{b}", "has space"} {
		if err := d.Bind(ctx, good, key); !errors.Is(err, identity.ErrInvalid) {
			t.Errorf("key %q: %v", key, err)
		}
	}
	if _, err := d.Lookup(ctx, identity.Identity{Type: identity.IMSI, Value: "abc"}); !errors.Is(err, identity.ErrInvalid) {
		t.Error(err)
	}
}

func TestResolverOverCachedRedisDirectory(t *testing.T) {
	rdb := redistest.Start(t)
	rd := identity.NewRedisDirectory(rdb, "")
	ctx := context.Background()
	_ = rd.Bind(ctx, identity.Identity{Type: identity.MSISDN, Value: "491701234567"}, "acct-1")
	_ = rd.Bind(ctx, identity.Identity{Type: identity.IMSI, Value: "262011234567890"}, "acct-1")
	r := identity.NewResolver(identity.NewCachedDirectory(rd, identity.CacheConfig{}), identity.ConflictReject)

	k, err := r.Resolve(ctx, []identity.Identity{
		{Type: identity.MSISDN, Value: "491701234567"}, {Type: identity.IMSI, Value: "262011234567890"}})
	if err != nil || k != "acct-1" {
		t.Fatalf("%q %v", k, err)
	}
}

func TestBindNewAndUnbindIf(t *testing.T) {
	d := identity.NewRedisDirectory(redistest.Start(t), "")
	ctx := context.Background()
	id := identity.Identity{Type: identity.IMSI, Value: "262011234567890"}

	if created, err := d.BindNew(ctx, id, "acct-1"); err != nil || !created {
		t.Fatalf("%v %v", created, err)
	}
	if created, err := d.BindNew(ctx, id, "acct-1"); err != nil || created {
		t.Fatalf("binding the same key again must not claim to have created it: %v %v", created, err)
	}
	if _, err := d.BindNew(ctx, id, "acct-2"); !errors.Is(err, identity.ErrConflict) {
		t.Fatal(err)
	}
	// Undo is conditional: it never removes someone else's binding.
	if removed, err := d.UnbindIf(ctx, id, "acct-2"); err != nil || removed {
		t.Fatalf("%v %v", removed, err)
	}
	if k, _ := d.Lookup(ctx, id); k != "acct-1" {
		t.Fatalf("%q", k)
	}
	if removed, err := d.UnbindIf(ctx, id, "acct-1"); err != nil || !removed {
		t.Fatalf("%v %v", removed, err)
	}
	if _, err := d.Lookup(ctx, id); !errors.Is(err, identity.ErrUnknown) {
		t.Fatal(err)
	}
	if removed, _ := d.UnbindIf(ctx, id, "acct-1"); removed {
		t.Fatal("nothing left to remove")
	}
}
