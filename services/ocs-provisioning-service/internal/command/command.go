// Package command is the provisioning command model: what the API accepts,
// what travels through Kafka, and what the workers apply.
package command

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/lifecycle"
)

// Type is the kind of command.
type Type string

const (
	CreateSubscriber Type = "create_subscriber"
	TopUp            Type = "top_up"
	AdjustBalance    Type = "adjust_balance"
	AddBundle        Type = "add_bundle"
	ReloadBucket     Type = "reload_bucket"
	RemoveBucket     Type = "remove_bucket"
	ChangeLifecycle  Type = "change_lifecycle"
	AttachOffering   Type = "attach_offering"
)

// Privileged commands move or destroy value in ways routine CRM flows do not
// (corrections, removals, termination), so they need a privileged caller.
func (t Type) Privileged() bool {
	return t == AdjustBalance || t == RemoveBucket || t == ChangeLifecycle
}

// SchemaVersion is the version of the Kafka message.
const SchemaVersion = 1

// Identifier is a subscriber identity to bind on creation.
type Identifier struct {
	Type  string `json:"type"` // msisdn | imsi | nai
	Value string `json:"value"`
}

// Command is one provisioning request. Only the fields of its Type are used.
type Command struct {
	V          int    `json:"v"`
	ID         string `json:"id"` // the caller's idempotency key
	Type       Type   `json:"type"`
	Key        string `json:"key"` // subscriber key
	AcceptedAt int64  `json:"at"`  // unix ms, set by the service
	Principal  string `json:"by,omitempty"`

	// create_subscriber
	Payment     string       `json:"payment,omitempty"`
	Identifiers []Identifier `json:"identifiers,omitempty"`

	// buckets and balances
	BucketID      string `json:"bucket,omitempty"`
	Amount        int64  `json:"amount,omitempty"` // credit; signed delta for adjust_balance
	Currency      string `json:"currency,omitempty"`
	Reason        string `json:"reason,omitempty"`
	AllowNegative bool   `json:"allowNegative,omitempty"`
	Class         string `json:"class,omitempty"` // promo | bundle
	Unit          string `json:"unit,omitempty"`  // bytes | seconds | events
	ExpiresAt     int64  `json:"expiresAt,omitempty"`
	KeepExpiry    bool   `json:"keepExpiry,omitempty"`
	Priority      *int32 `json:"priority,omitempty"`
	Force         bool   `json:"force,omitempty"`

	// change_lifecycle
	Target     string `json:"target,omitempty"`
	GraceUntil int64  `json:"graceUntil,omitempty"`

	// attach_offering. The caller supplies Offering and Renew; the service
	// fills Allowances and CatalogVersion from the catalog when it accepts the
	// command, so the command carries exactly what will be applied and a
	// redelivery after a catalog change still applies the same thing.
	Offering       string      `json:"offering,omitempty"`
	Renew          bool        `json:"renew,omitempty"`
	Allowances     []Allowance `json:"allowances,omitempty"`
	CatalogVersion int64       `json:"catalogVersion,omitempty"`
}

// Allowance is a bucket an offering creates.
type Allowance struct {
	ID        string  `json:"id"`
	Class     string  `json:"class"` // promo | bundle
	Unit      string  `json:"unit"`  // bytes | seconds | events
	Amount    int64   `json:"amount"`
	ExpiresAt int64   `json:"expiresAt,omitempty"` // unix ms, absolute; 0 = never
	Order     float64 `json:"order"`
}

// MaxAllowances bounds the allowances of one offering attachment.
const MaxAllowances = datalayer.MaxAllowances

// Limits bound what one command may do, to cap the damage of a mistake or a
// compromised caller.
type Limits struct {
	Currency  string // the deployment currency, required on money operations
	MaxTopUp  int64  // largest single top-up, minor units
	MaxAdjust int64  // largest single adjustment (absolute), money or units
}

// DefaultLimits are conservative placeholders; set them per deployment.
func DefaultLimits() Limits {
	return Limits{Currency: "EUR", MaxTopUp: 100_000_00, MaxAdjust: 10_000_00}
}

// DefaultCashBucket is the bucket money goes to unless the caller names one.
const DefaultCashBucket = "cash"

// Default consumption order (lower is consumed first): promo, bundle, cash.
const (
	DefaultPromoPriority  = 10
	DefaultBundlePriority = 100
	CashPriority          = 1000
)

var (
	idRe     = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	bucketRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// ValidationError describes a rejected command.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func bad(format string, a ...any) error { return &ValidationError{fmt.Sprintf(format, a...)} }

// IsValidation reports whether err is a ValidationError.
func IsValidation(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}

// ValidID reports whether s is acceptable as a command id.
func ValidID(s string) bool { return idRe.MatchString(s) }

// Validate checks the command statically (no I/O).
func (c *Command) Validate(l Limits) error {
	if !idRe.MatchString(c.ID) {
		return bad("idempotency key must be 1-128 characters from A-Z a-z 0-9 . _ : -")
	}
	if !identity.ValidKey(c.Key) {
		return bad("subscriber key must be 1-128 printable ASCII characters without spaces or braces")
	}
	bucket := func() error {
		if !bucketRe.MatchString(c.BucketID) {
			return bad("bucket id must be 1-64 characters from A-Z a-z 0-9 . _ -")
		}
		return nil
	}
	money := func() error {
		if c.Currency != l.Currency {
			return bad("currency must be %s", l.Currency)
		}
		return nil
	}
	switch c.Type {
	case CreateSubscriber:
		if c.Payment != "prepaid" && c.Payment != "postpaid" {
			return bad("payment type must be prepaid or postpaid")
		}
		if len(c.Identifiers) == 0 || len(c.Identifiers) > 8 {
			return bad("1 to 8 identifiers are required")
		}
		seen := map[Identifier]bool{}
		for i, id := range c.Identifiers {
			t, ok := identityType(id.Type)
			if !ok {
				return bad("identifier %d: type must be msisdn, imsi or nai", i)
			}
			v, ok := identity.Normalize(t, id.Value)
			if !ok {
				return bad("identifier %d: malformed %s", i, id.Type)
			}
			n := Identifier{id.Type, v}
			if seen[n] {
				return bad("identifier %d is a duplicate", i)
			}
			seen[n] = true
		}
	case TopUp:
		if err := bucket(); err != nil {
			return err
		}
		if err := money(); err != nil {
			return err
		}
		if c.Amount <= 0 || c.Amount > l.MaxTopUp {
			return bad("amount must be between 1 and %d minor units", l.MaxTopUp)
		}
	case AdjustBalance:
		if err := bucket(); err != nil {
			return err
		}
		if c.BucketID == DefaultCashBucket {
			if err := money(); err != nil {
				return err
			}
		}
		if c.Amount == 0 || c.Amount > l.MaxAdjust || c.Amount < -l.MaxAdjust {
			return bad("delta must be non-zero and at most %d in absolute value", l.MaxAdjust)
		}
		if strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 256 {
			return bad("a reason of 1-256 characters is required")
		}
	case AddBundle:
		if err := bucket(); err != nil {
			return err
		}
		if c.Class != "bundle" && c.Class != "promo" {
			return bad("class must be bundle or promo")
		}
		if !validUnit(c.Unit) {
			return bad("unit must be bytes, seconds or events")
		}
		if c.Amount <= 0 || c.Amount > datalayer.MaxBalance {
			return bad("amount must be positive and at most %d", int64(datalayer.MaxBalance))
		}
		if c.ExpiresAt < 0 {
			return bad("expiry must not be negative")
		}
	case ReloadBucket:
		if err := bucket(); err != nil {
			return err
		}
		if c.Amount < 0 || c.Amount > datalayer.MaxBalance {
			return bad("amount must be between 0 and %d", int64(datalayer.MaxBalance))
		}
		if !c.KeepExpiry && c.ExpiresAt < 0 {
			return bad("expiry must not be negative")
		}
	case RemoveBucket:
		if err := bucket(); err != nil {
			return err
		}
	case ChangeLifecycle:
		st, ok := lifecycle.Parse(c.Target)
		if !ok {
			return bad("target must be active, grace, suspended or terminated")
		}
		if c.GraceUntil != 0 && st != lifecycle.Grace {
			return bad("grace deadline only applies to the grace state")
		}
		if c.GraceUntil < 0 {
			return bad("grace deadline must not be negative")
		}
		if len(c.Reason) > 256 {
			return bad("reason is at most 256 characters")
		}
	case AttachOffering:
		if !bucketRe.MatchString(c.Offering) || c.Offering == "-" {
			return bad("offering id must be 1-64 characters from A-Z a-z 0-9 . _ -")
		}
		if len(c.Allowances) > MaxAllowances {
			return bad("an offering may create at most %d allowances", MaxAllowances)
		}
		seen := map[string]bool{}
		for i, a := range c.Allowances {
			switch {
			case !bucketRe.MatchString(a.ID) || seen[a.ID]:
				return bad("allowance %d: bad or duplicate id", i)
			case a.Class != "bundle" && a.Class != "promo":
				return bad("allowance %d: class must be bundle or promo", i)
			case !validUnit(a.Unit):
				return bad("allowance %d: unit must be bytes, seconds or events", i)
			case a.Amount <= 0 || a.Amount > datalayer.MaxBalance:
				return bad("allowance %d: amount out of range", i)
			case a.ExpiresAt < 0:
				return bad("allowance %d: negative expiry", i)
			}
			seen[a.ID] = true
		}
	default:
		return bad("unknown command type %q", c.Type)
	}
	return nil
}

// IdentityType parses an identifier type name (msisdn, imsi, nai).
func IdentityType(s string) (identity.Type, bool) { return identityType(s) }

func identityType(s string) (identity.Type, bool) {
	switch s {
	case "msisdn":
		return identity.MSISDN, true
	case "imsi":
		return identity.IMSI, true
	case "nai":
		return identity.NAI, true
	}
	return 0, false
}

// Identities returns the identifiers in normalised form. Call after Validate.
func (c *Command) Identities() []identity.Identity {
	out := make([]identity.Identity, 0, len(c.Identifiers))
	for _, id := range c.Identifiers {
		t, _ := identityType(id.Type)
		v, _ := identity.Normalize(t, id.Value)
		out = append(out, identity.Identity{Type: t, Value: v})
	}
	return out
}

func validUnit(s string) bool { return s == "bytes" || s == "seconds" || s == "events" }

// Fingerprint identifies the content of the request: everything except the
// id, the acceptance time and the caller. Two submissions with the same id
// are the same request only if their fingerprints match.
func (c *Command) Fingerprint() string {
	cp := *c
	cp.ID, cp.AcceptedAt, cp.Principal = "", 0, ""
	// Derived from the catalog at acceptance: a retry after a catalog change is
	// still the same request.
	cp.Allowances, cp.CatalogVersion = nil, 0
	if len(cp.Identifiers) > 0 { // identifier order is not meaningful
		ids := append([]Identifier(nil), cp.Identifiers...)
		for i := range ids {
			if t, ok := identityType(ids[i].Type); ok {
				if v, ok := identity.Normalize(t, ids[i].Value); ok {
					ids[i].Value = v
				}
			}
		}
		sort.Slice(ids, func(i, j int) bool { return less(ids[i], ids[j]) })
		cp.Identifiers = ids
	}
	b, _ := json.Marshal(cp)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func less(a, b Identifier) bool {
	if a.Type != b.Type {
		return a.Type < b.Type
	}
	return a.Value < b.Value
}

// Encode serialises the command for Kafka.
func (c *Command) Encode() ([]byte, error) {
	c.V = SchemaVersion
	return json.Marshal(c)
}

// Decode parses a Kafka message. Unknown schema versions are refused.
func Decode(b []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.V != SchemaVersion {
		return nil, fmt.Errorf("unsupported command schema version %d", c.V)
	}
	return &c, nil
}

// Order returns the consumption order of a bucket: the caller's priority, or
// the default of its class.
func (c *Command) Order() float64 {
	if c.Priority != nil {
		return float64(*c.Priority)
	}
	switch {
	case c.Type == AddBundle && c.Class == "promo":
		return DefaultPromoPriority
	case c.Type == AddBundle:
		return DefaultBundlePriority
	}
	return CashPriority
}

// ExpiresTime returns the expiry as a time (zero = never).
func (c *Command) ExpiresTime() time.Time {
	if c.ExpiresAt == 0 {
		return time.Time{}
	}
	return time.UnixMilli(c.ExpiresAt)
}
