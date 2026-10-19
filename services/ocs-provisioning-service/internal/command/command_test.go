package command

import (
	"strings"
	"testing"
	"time"
)

var lim = Limits{Currency: "EUR", MaxTopUp: 100_000, MaxAdjust: 5_000}

func base(t Type) *Command {
	c := &Command{ID: "req-1", Type: t, Key: "acct-1"}
	switch t {
	case CreateSubscriber:
		c.Payment = "prepaid"
		c.Identifiers = []Identifier{{"msisdn", "491701234567"}, {"imsi", "262011234567890"}}
	case TopUp:
		c.BucketID, c.Amount, c.Currency = "cash", 500, "EUR"
	case AdjustBalance:
		c.BucketID, c.Amount, c.Currency, c.Reason = "cash", -50, "EUR", "goodwill"
	case AddBundle:
		c.BucketID, c.Class, c.Unit, c.Amount = "data", "bundle", "bytes", 1000
	case ReloadBucket:
		c.BucketID, c.Amount = "data", 1000
	case RemoveBucket:
		c.BucketID = "data"
	case ChangeLifecycle:
		c.Target = "suspended"
	}
	return c
}

func TestValidCommandsPass(t *testing.T) {
	for _, ty := range []Type{CreateSubscriber, TopUp, AdjustBalance, AddBundle, ReloadBucket, RemoveBucket, ChangeLifecycle} {
		if err := base(ty).Validate(lim); err != nil {
			t.Errorf("%s: %v", ty, err)
		}
	}
}

func TestValidationRejects(t *testing.T) {
	tests := []struct {
		name string
		ty   Type
		mod  func(*Command)
	}{
		{"empty id", TopUp, func(c *Command) { c.ID = "" }},
		{"id with a space", TopUp, func(c *Command) { c.ID = "a b" }},
		{"id with a slash", TopUp, func(c *Command) { c.ID = "a/b" }},
		{"id too long", TopUp, func(c *Command) { c.ID = strings.Repeat("a", 129) }},
		{"key with braces", TopUp, func(c *Command) { c.Key = "a{b}" }},
		{"empty key", TopUp, func(c *Command) { c.Key = "" }},
		{"unknown type", TopUp, func(c *Command) { c.Type = "launder" }},
		{"create: payment", CreateSubscriber, func(c *Command) { c.Payment = "free" }},
		{"create: no identifiers", CreateSubscriber, func(c *Command) { c.Identifiers = nil }},
		{"create: too many identifiers", CreateSubscriber, func(c *Command) {
			for i := 0; i < 9; i++ {
				c.Identifiers = append(c.Identifiers, Identifier{"msisdn", "4917000000" + string(rune('0'+i))})
			}
		}},
		{"create: bad identifier type", CreateSubscriber, func(c *Command) { c.Identifiers[0].Type = "sip" }},
		{"create: malformed msisdn", CreateSubscriber, func(c *Command) { c.Identifiers[0].Value = "12ab" }},
		{"create: duplicate identifier", CreateSubscriber, func(c *Command) { c.Identifiers[1] = Identifier{"msisdn", "+491701234567"} }},
		{"topup: zero", TopUp, func(c *Command) { c.Amount = 0 }},
		{"topup: negative", TopUp, func(c *Command) { c.Amount = -5 }},
		{"topup: over the limit", TopUp, func(c *Command) { c.Amount = lim.MaxTopUp + 1 }},
		{"topup: wrong currency", TopUp, func(c *Command) { c.Currency = "USD" }},
		{"topup: no currency", TopUp, func(c *Command) { c.Currency = "" }},
		{"topup: bad bucket", TopUp, func(c *Command) { c.BucketID = "a|b" }},
		{"adjust: zero", AdjustBalance, func(c *Command) { c.Amount = 0 }},
		{"adjust: over the limit", AdjustBalance, func(c *Command) { c.Amount = -lim.MaxAdjust - 1 }},
		{"adjust: no reason", AdjustBalance, func(c *Command) { c.Reason = "  " }},
		{"adjust: reason too long", AdjustBalance, func(c *Command) { c.Reason = strings.Repeat("x", 257) }},
		{"adjust: cash without currency", AdjustBalance, func(c *Command) { c.Currency = "" }},
		{"bundle: class", AddBundle, func(c *Command) { c.Class = "cash" }},
		{"bundle: unit", AddBundle, func(c *Command) { c.Unit = "litres" }},
		{"bundle: zero", AddBundle, func(c *Command) { c.Amount = 0 }},
		{"bundle: negative expiry", AddBundle, func(c *Command) { c.ExpiresAt = -1 }},
		{"reload: negative", ReloadBucket, func(c *Command) { c.Amount = -1 }},
		{"remove: bad bucket", RemoveBucket, func(c *Command) { c.BucketID = "" }},
		{"lifecycle: target", ChangeLifecycle, func(c *Command) { c.Target = "dormant" }},
		{"lifecycle: deadline outside grace", ChangeLifecycle, func(c *Command) { c.GraceUntil = 1 }},
	}
	for _, tc := range tests {
		c := base(tc.ty)
		tc.mod(c)
		err := c.Validate(lim)
		if err == nil || !IsValidation(err) {
			t.Errorf("%s: want a validation error, got %v", tc.name, err)
		}
	}
}

func TestAdjustOfANonCashBucketNeedsNoCurrency(t *testing.T) {
	c := base(AdjustBalance)
	c.BucketID, c.Currency = "data", ""
	if err := c.Validate(lim); err != nil {
		t.Fatal(err)
	}
}

func TestFingerprint(t *testing.T) {
	a := base(TopUp)
	fp := a.Fingerprint()

	// Not part of the content: id, acceptance time, caller.
	b := base(TopUp)
	b.ID, b.AcceptedAt, b.Principal = "another", 12345, "crm-2"
	if b.Fingerprint() != fp {
		t.Error("id, time and caller must not affect the fingerprint")
	}
	// Everything that changes what the command does must.
	for name, mod := range map[string]func(*Command){
		"amount": func(c *Command) { c.Amount++ },
		"key":    func(c *Command) { c.Key = "acct-2" },
		"bucket": func(c *Command) { c.BucketID = "other" },
		"type":   func(c *Command) { c.Type = AdjustBalance },
	} {
		c := base(TopUp)
		mod(c)
		if c.Fingerprint() == fp {
			t.Errorf("%s must change the fingerprint", name)
		}
	}

	// Identifier order and formatting do not matter.
	c1, c2 := base(CreateSubscriber), base(CreateSubscriber)
	c2.Identifiers = []Identifier{{"imsi", "262011234567890"}, {"msisdn", "+491701234567"}}
	if c1.Fingerprint() != c2.Fingerprint() {
		t.Error("the same identifiers in another order are the same request")
	}
	c2.Identifiers[1].Value = "491701234568"
	if c1.Fingerprint() == c2.Fingerprint() {
		t.Error("different identifiers are a different request")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	p := int32(7)
	c := base(AddBundle)
	c.Priority, c.ExpiresAt, c.Principal, c.AcceptedAt = &p, 1_900_000_000_000, "crm-1", 1_800_000_000_000
	raw, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw)
	if err != nil || got.ID != c.ID || got.Amount != c.Amount || *got.Priority != 7 || got.Principal != "crm-1" || got.V != SchemaVersion {
		t.Fatalf("%+v %v", got, err)
	}
	if got.Fingerprint() != c.Fingerprint() {
		t.Error("the fingerprint must survive the round trip")
	}
	if _, err := Decode([]byte(`{"v":99,"id":"x"}`)); err == nil {
		t.Error("an unknown schema version must be refused")
	}
	if _, err := Decode([]byte(`not json`)); err == nil {
		t.Error("garbage")
	}
}

func TestOrderAndExpiry(t *testing.T) {
	bundle, promo := base(AddBundle), base(AddBundle)
	promo.Class = "promo"
	if bundle.Order() != DefaultBundlePriority || promo.Order() != DefaultPromoPriority || base(TopUp).Order() != CashPriority {
		t.Error("defaults must consume promo, then bundles, then cash")
	}
	p := int32(3)
	bundle.Priority = &p
	if bundle.Order() != 3 {
		t.Error("an explicit priority wins")
	}
	if !base(AddBundle).ExpiresTime().IsZero() {
		t.Error("no expiry")
	}
	c := base(AddBundle)
	c.ExpiresAt = 1_700_000_000_000
	if !c.ExpiresTime().Equal(time.UnixMilli(1_700_000_000_000)) {
		t.Error("expiry")
	}
}

func TestPrivilegedTypes(t *testing.T) {
	for ty, want := range map[Type]bool{CreateSubscriber: false, TopUp: false, AddBundle: false, ReloadBucket: false,
		AdjustBalance: true, RemoveBucket: true, ChangeLifecycle: true} {
		if ty.Privileged() != want {
			t.Errorf("%s", ty)
		}
	}
}
