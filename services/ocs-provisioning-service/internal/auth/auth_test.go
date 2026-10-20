package auth

import (
	"errors"
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

var all = []command.Type{command.CreateSubscriber, command.TopUp, command.AddBundle, command.ReloadBucket,
	command.AdjustBalance, command.RemoveBucket, command.ChangeLifecycle}

func TestOffAllowsEverythingAsAnonymous(t *testing.T) {
	a := Off()
	if a.Principal("whoever") != Anonymous || a.Read("") != nil {
		t.Fatal()
	}
	for _, ty := range all {
		if err := a.Allow(Anonymous, ty); err != nil {
			t.Errorf("%s: %v", ty, err)
		}
	}
}

func TestNoClientCertificateIsUnauthenticated(t *testing.T) {
	a := New(nil, []string{"care"})
	for _, ty := range all {
		if err := a.Allow("", ty); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: %v", ty, err)
		}
	}
	if !errors.Is(a.Read(""), ErrUnauthenticated) {
		t.Fatal()
	}
	if a.Read("anyone") != nil {
		t.Fatal("any authenticated caller may read")
	}
}

func TestStandardCallersCannotDoPrivilegedThings(t *testing.T) {
	a := New([]string{"crm"}, []string{"care"})
	for _, ty := range all {
		err := a.Allow("crm", ty)
		if ty.Privileged() && !errors.Is(err, ErrForbidden) {
			t.Errorf("%s: a standard caller got %v", ty, err)
		}
		if !ty.Privileged() && err != nil {
			t.Errorf("%s: %v", ty, err)
		}
		if err := a.Allow("care", ty); err != nil {
			t.Errorf("privileged caller, %s: %v", ty, err)
		}
	}
}

func TestUnlistedCallersAreForbiddenWhenAStandardListExists(t *testing.T) {
	a := New([]string{"crm"}, []string{"care"})
	if err := a.Allow("stranger", command.TopUp); !errors.Is(err, ErrForbidden) {
		t.Fatalf("%v", err)
	}
}

func TestAnyVerifiedClientIsStandardWhenNoListIsGivenButNeverPrivileged(t *testing.T) {
	a := New(nil, nil)
	if err := a.Allow("anyone", command.TopUp); err != nil {
		t.Fatal(err)
	}
	for _, ty := range []command.Type{command.AdjustBalance, command.RemoveBucket, command.ChangeLifecycle} {
		if err := a.Allow("anyone", ty); !errors.Is(err, ErrForbidden) {
			t.Errorf("%s must need a named privileged caller: %v", ty, err)
		}
	}
}

func TestDescribe(t *testing.T) {
	if !strings.Contains(Off().Describe(), "OFF") {
		t.Fatal()
	}
	d := New([]string{"b", "a"}, nil).Describe()
	if !strings.Contains(d, "a,b") || !strings.Contains(d, "privileged: none") {
		t.Fatal(d)
	}
}

func TestReadHonoursTheAllowList(t *testing.T) {
	open := New(nil, []string{"care"}) // no standard list: any verified client may read
	if err := open.Read("anyone"); err != nil {
		t.Fatalf("%v", err)
	}
	if err := open.Read(""); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("%v", err)
	}
	listed := New([]string{"crm"}, []string{"care"})
	for who, want := range map[string]error{"crm": nil, "care": nil, "stranger": ErrForbidden, "": ErrUnauthenticated} {
		if err := listed.Read(who); !errors.Is(err, want) && !(err == nil && want == nil) {
			t.Errorf("%q: %v, want %v", who, err, want)
		}
	}
	if err := Off().Read(""); err != nil {
		t.Fatal(err)
	}
}
