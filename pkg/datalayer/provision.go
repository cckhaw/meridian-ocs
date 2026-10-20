package datalayer

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

//go:embed lua/provision.lua
var provisionLua string

// ProvisionKind selects the provisioning step.
type ProvisionKind byte

const (
	ProvCreate    ProvisionKind = 'C'
	ProvLifecycle ProvisionKind = 'L'
	ProvTopUp     ProvisionKind = 'T'
	ProvAdjust    ProvisionKind = 'A'
	ProvAddBundle ProvisionKind = 'B'
	ProvReload    ProvisionKind = 'R'
	ProvRemove    ProvisionKind = 'X'
	ProvOffering  ProvisionKind = 'P'
)

// ProvisionCode is the outcome of a provisioning step.
type ProvisionCode int

const (
	ProvApplied           ProvisionCode = 0
	ProvDuplicate         ProvisionCode = 1 // the token was seen: the stored original outcome is returned
	ProvNoSubscriber      ProvisionCode = 2
	ProvExists            ProvisionCode = 3
	ProvInvalidTransition ProvisionCode = 4
	ProvInsufficient      ProvisionCode = 5
	ProvTokenMismatch     ProvisionCode = 6 // token reused for a different request
	ProvNoBucket          ProvisionCode = 7
	ProvTerminated        ProvisionCode = 8
	ProvOverflow          ProvisionCode = 9
	ProvBucketNotEmpty    ProvisionCode = 10
	ProvNotCashBucket     ProvisionCode = 11
	ProvAllowanceExists   ProvisionCode = 12 // an allowance bucket of the offering exists and renew was not set
)

func (c ProvisionCode) String() string {
	names := map[ProvisionCode]string{
		ProvApplied: "applied", ProvDuplicate: "duplicate", ProvNoSubscriber: "no_such_subscriber",
		ProvExists: "already_exists", ProvInvalidTransition: "invalid_transition", ProvInsufficient: "insufficient_balance",
		ProvTokenMismatch: "idempotency_token_reused", ProvNoBucket: "no_such_bucket", ProvTerminated: "subscriber_terminated",
		ProvOverflow: "balance_overflow", ProvBucketNotEmpty: "bucket_not_empty", ProvNotCashBucket: "not_a_cash_bucket", ProvAllowanceExists: "allowance_exists",
	}
	if n, ok := names[c]; ok {
		return n
	}
	return "unknown"
}

// Failed reports whether the code is a business failure (a duplicate is not:
// it echoes an earlier outcome, see ProvisionResult.Effective).
func (c ProvisionCode) Failed() bool { return c != ProvApplied && c != ProvDuplicate }

// ProvisionOp is one provisioning step. Token is the idempotency key and
// Fingerprint identifies the request content: reusing a Token with a different
// Fingerprint is refused, and reusing it with the same one returns the stored
// outcome without applying anything again.
type ProvisionOp struct {
	Op          ProvisionKind
	Key         string // subscriber key
	Token       string
	Fingerprint string
	Now         time.Time
	IdemTTL     time.Duration // default 7 days

	Payment string // ProvCreate: "prepaid" | "postpaid"
	// Identifiers (ProvCreate) are recorded on the profile as "type:value"
	// (for example "msisdn:491701234567"), at most MaxIdentifiers. The identity
	// directory remains what resolves them; this copy is for display.
	Identifiers []string
	Status      string // ProvCreate: initial state (default "active")
	Target      string // ProvLifecycle: new state
	Until       time.Time

	BucketID      string
	Class         Class
	Unit          canonical.UnitKind // ProvAddBundle
	Amount        int64              // credit, delta (ProvAdjust, may be negative) or allowance
	ExpiresAt     time.Time
	KeepExpiry    bool // ProvReload
	Order         float64
	KeepOrder     bool // ProvReload
	AllowNegative bool
	Force         bool // ProvRemove

	Offering   string      // ProvOffering: the offering id
	Renew      bool        // ProvOffering: reset allowance buckets that already exist
	Allowances []Allowance // ProvOffering
}

// MaxIdentifiers is the most identifiers a profile records.
const MaxIdentifiers = 8

// MaxAllowances is the most allowances one offering attachment may create.
const MaxAllowances = 32

// Allowance is a bucket created when an offering is attached.
type Allowance struct {
	BucketID  string
	Class     Class // promo or bundle
	Unit      canonical.UnitKind
	Amount    int64
	ExpiresAt time.Time // zero = never
	Order     float64
}

// ProvisionResult is the decoded reply. Fields are filled per kind.
type ProvisionResult struct {
	Code ProvisionCode
	// OriginalCode is the outcome of the first execution when Code is
	// ProvDuplicate.
	OriginalCode ProvisionCode
	Balance      int64 // new balance (credit, adjust, bundle, reload) or the removed / current one
	OldBalance   int64 // ProvReload
	From, To     string
	Version      int64
	Replication  Replication
	// Offering results: the previous offering ("" if none), the new one, the
	// number of allowance buckets written, and for a failure the bucket id.
	PrevOffering, Offering string
	Buckets                int
	Conflict               string
}

// Effective returns the outcome that applies: the original one for a duplicate.
func (r ProvisionResult) Effective() ProvisionCode {
	if r.Code == ProvDuplicate {
		return r.OriginalCode
	}
	return r.Code
}

// MaxBalance is the largest balance a bucket may hold (2^52: exact in Lua).
const MaxBalance = 1 << 52

// DefaultIdemTTL is how long outcomes are remembered for redelivery.
const DefaultIdemTTL = 7 * 24 * time.Hour

func (o *ProvisionOp) args() ([]any, error) {
	if !validID(o.Key) || o.Token == "" || len(o.Token) > 128 || strings.ContainsAny(o.Token, ",\r\n") || o.Fingerprint == "" {
		return nil, fmt.Errorf("%w: key, token or fingerprint", ErrInvalid)
	}
	if o.BucketID != "" && (strings.ContainsAny(o.BucketID, "|;,{}") || len(o.BucketID) > 64) {
		return nil, fmt.Errorf("%w: bucket id", ErrInvalid)
	}
	ttl := o.IdemTTL
	if ttl <= 0 {
		ttl = DefaultIdemTTL
	}
	a := []any{string(rune(o.Op)), o.Token, o.Fingerprint, o.Now.UnixMilli(), ttl.Milliseconds()}
	ms := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.UnixMilli()
	}
	needID := func() error {
		if o.BucketID == "" {
			return fmt.Errorf("%w: bucket id", ErrInvalid)
		}
		return nil
	}
	flag := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	switch o.Op {
	case ProvCreate:
		st := o.Status
		if st == "" {
			st = "active"
		}
		if (o.Payment != "prepaid" && o.Payment != "postpaid") || st != "active" && st != "grace" && st != "suspended" {
			return nil, fmt.Errorf("%w: payment or status", ErrInvalid)
		}
		if len(o.Identifiers) > MaxIdentifiers {
			return nil, fmt.Errorf("%w: too many identifiers", ErrInvalid)
		}
		for _, id := range o.Identifiers {
			if id == "" || len(id) > 128 || strings.ContainsAny(id, ",\r\n") {
				return nil, fmt.Errorf("%w: identifier %q", ErrInvalid, id)
			}
		}
		a = append(a, o.Payment, st, strings.Join(o.Identifiers, ","))
	case ProvLifecycle:
		if o.Target != "active" && o.Target != "grace" && o.Target != "suspended" && o.Target != "terminated" {
			return nil, fmt.Errorf("%w: target state", ErrInvalid)
		}
		a = append(a, o.Target, ms(o.Until))
	case ProvTopUp, ProvAdjust:
		if err := needID(); err != nil {
			return nil, err
		}
		if o.Amount == 0 || o.Amount > MaxBalance || o.Amount < -MaxBalance || (o.Op == ProvTopUp && o.Amount < 0) {
			return nil, fmt.Errorf("%w: amount", ErrInvalid)
		}
		a = append(a, o.BucketID, o.Amount, o.Order, flag(o.AllowNegative && o.Op == ProvAdjust))
	case ProvAddBundle:
		if err := needID(); err != nil {
			return nil, err
		}
		if (o.Class != ClassBundle && o.Class != ClassPromo) || o.Amount <= 0 || o.Amount > MaxBalance {
			return nil, fmt.Errorf("%w: class or amount", ErrInvalid)
		}
		a = append(a, o.BucketID, string(o.Class), o.Unit.String(), o.Amount, ms(o.ExpiresAt), o.Order)
	case ProvReload:
		if err := needID(); err != nil {
			return nil, err
		}
		if o.Amount < 0 || o.Amount > MaxBalance {
			return nil, fmt.Errorf("%w: amount", ErrInvalid)
		}
		exp := any(int64(-1))
		if !o.KeepExpiry {
			exp = ms(o.ExpiresAt)
		}
		order := any("keep")
		if !o.KeepOrder {
			order = o.Order
		}
		a = append(a, o.BucketID, o.Amount, exp, order)
	case ProvRemove:
		if err := needID(); err != nil {
			return nil, err
		}
		a = append(a, o.BucketID, flag(o.Force))
	case ProvOffering:
		if o.Offering == "" || o.Offering == "-" || len(o.Offering) > 64 || strings.ContainsAny(o.Offering, ",\r\n{}") || len(o.Allowances) > MaxAllowances {
			return nil, fmt.Errorf("%w: offering", ErrInvalid)
		}
		a = append(a, o.Offering, flag(o.Renew), len(o.Allowances))
		seen := map[string]bool{}
		for _, al := range o.Allowances {
			if al.BucketID == "" || len(al.BucketID) > 64 || strings.ContainsAny(al.BucketID, "|;,{}") || seen[al.BucketID] ||
				(al.Class != ClassBundle && al.Class != ClassPromo) || al.Amount <= 0 || al.Amount > MaxBalance {
				return nil, fmt.Errorf("%w: allowance %q", ErrInvalid, al.BucketID)
			}
			seen[al.BucketID] = true
			a = append(a, al.BucketID, string(al.Class), al.Unit.String(), al.Amount, ms(al.ExpiresAt), al.Order)
		}
	default:
		return nil, fmt.Errorf("%w: kind", ErrInvalid)
	}
	return a, nil
}

// Provision applies one provisioning step atomically and idempotently. When
// durability is configured the call also waits for replica acknowledgement:
// a top-up that is lost is the worst outcome there is.
func (c *Client) Provision(ctx context.Context, op ProvisionOp) (ProvisionResult, error) {
	args, err := op.args()
	if err != nil {
		return ProvisionResult{}, err
	}
	v, rep, err := c.run(ctx, c.provision, []string{Prefix(op.Key)}, args, true)
	if err != nil {
		return ProvisionResult{}, err
	}
	raw, ok := v.([]any)
	if !ok || len(raw) == 0 {
		return ProvisionResult{}, errors.New("datalayer: unexpected provisioning reply")
	}
	res, err := decodeProvision(op.Op, raw)
	res.Replication = rep
	return res, err
}

func decodeProvision(kind ProvisionKind, raw []any) (ProvisionResult, error) {
	var r ProvisionResult
	code, _ := raw[0].(int64)
	r.Code = ProvisionCode(code)
	vals := make([]string, 0, len(raw)-1)
	for _, x := range raw[1:] {
		s, _ := x.(string)
		vals = append(vals, s)
	}
	if r.Code == ProvDuplicate {
		if len(vals) == 0 {
			return r, errors.New("datalayer: malformed duplicate reply")
		}
		oc, err := strconv.Atoi(vals[0])
		if err != nil {
			return r, fmt.Errorf("datalayer: malformed duplicate reply: %w", err)
		}
		r.OriginalCode = ProvisionCode(oc)
		vals = vals[1:]
	}
	if r.Effective() != ProvApplied {
		// A few failures carry a value: the balance, or the transition.
		switch r.Effective() {
		case ProvInsufficient, ProvBucketNotEmpty:
			if len(vals) > 0 {
				r.Balance, _ = strconv.ParseInt(vals[0], 10, 64)
			}
		case ProvInvalidTransition:
			if len(vals) > 1 {
				r.From, r.To = vals[0], vals[1]
			}
		case ProvAllowanceExists:
			if len(vals) > 0 {
				r.Conflict = vals[0]
			}
		}
		return r, nil
	}
	num := func(i int) int64 {
		if i >= len(vals) {
			return 0
		}
		n, _ := strconv.ParseInt(vals[i], 10, 64)
		return n
	}
	switch kind {
	case ProvCreate:
		r.Version = num(0)
	case ProvLifecycle:
		if len(vals) < 3 {
			return r, errors.New("datalayer: malformed lifecycle reply")
		}
		r.From, r.To, r.Version = vals[0], vals[1], num(2)
	case ProvTopUp, ProvAdjust, ProvAddBundle, ProvRemove:
		r.Balance, r.Version = num(0), num(1)
	case ProvReload:
		r.Balance, r.OldBalance, r.Version = num(0), num(1), num(2)
	case ProvOffering:
		if len(vals) < 4 {
			return r, errors.New("datalayer: malformed offering reply")
		}
		r.PrevOffering, r.Offering, r.Buckets, r.Version = vals[0], vals[1], int(num(2)), num(3)
		if r.PrevOffering == "-" {
			r.PrevOffering = ""
		}
	}
	return r, nil
}

// Profile is a subscriber's provisioning record.
type Profile struct {
	Payment    string
	Status     string
	Version    int64
	CreatedAt  time.Time
	StatusAt   time.Time
	GraceUntil time.Time // zero unless in grace with a deadline
	Offering   string    // the product offering the subscriber is on ("" = none)
	// Identifiers recorded at creation as "type:value" (empty for subscribers
	// created before they were recorded).
	Identifiers []string
}

// ErrNoSubscriber is returned by reads for a subscriber without a profile.
var ErrNoSubscriber = errors.New("datalayer: no such subscriber")

func unixMilli(s string) time.Time {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	return time.UnixMilli(n)
}

// GetProfile reads the profile.
func (c *Client) GetProfile(ctx context.Context, key string) (Profile, error) {
	if !validID(key) {
		return Profile{}, fmt.Errorf("%w: key", ErrInvalid)
	}
	m, err := c.rdb.HGetAll(ctx, Prefix(key)+":profile").Result()
	if err != nil {
		return Profile{}, err
	}
	if len(m) == 0 {
		return Profile{}, ErrNoSubscriber
	}
	ver, _ := strconv.ParseInt(m["ver"], 10, 64)
	return Profile{Payment: m["payment"], Status: m["status"], Version: ver,
		CreatedAt: unixMilli(m["createdAt"]), StatusAt: unixMilli(m["statusAt"]), GraceUntil: unixMilli(m["graceUntil"]), Offering: m["offering"], Identifiers: splitIDs(m["ids"])}, nil
}

// BucketInfo is a bucket as stored.
type BucketInfo struct {
	ID        string
	Class     Class
	Kind      string // bytes | seconds | events | any
	Balance   int64
	ExpiresAt time.Time
	Order     float64
}

// ListBuckets returns a subscriber's buckets in consumption order.
func (c *Client) ListBuckets(ctx context.Context, key string) ([]BucketInfo, error) {
	if !validID(key) {
		return nil, fmt.Errorf("%w: key", ErrInvalid)
	}
	p := Prefix(key)
	ids, err := c.rdb.ZRangeWithScores(ctx, p+":ord", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, z := range ids {
		cmds[i] = pipe.HGetAll(ctx, p+":b:"+z.Member.(string))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]BucketInfo, 0, len(ids))
	for i, z := range ids {
		m := cmds[i].Val()
		if len(m) == 0 {
			continue
		}
		bal, _ := strconv.ParseInt(m["bal"], 10, 64)
		out = append(out, BucketInfo{ID: z.Member.(string), Class: Class(m["cls"]), Kind: m["kind"],
			Balance: bal, ExpiresAt: unixMilli(m["exp"]), Order: z.Score})
	}
	return out, nil
}

// Offering returns the offering id on the subscriber's profile: "" when the
// subscriber has none, ErrNoSubscriber when there is no profile. It is one
// HGET, cheap enough for a cache miss on the rating path.
func (c *Client) Offering(ctx context.Context, key string) (string, error) {
	if !validID(key) {
		return "", fmt.Errorf("%w: key", ErrInvalid)
	}
	pk := Prefix(key) + ":profile"
	vals, err := c.rdb.HMGet(ctx, pk, "status", "offering").Result()
	if err != nil {
		return "", err
	}
	if vals[0] == nil {
		return "", ErrNoSubscriber
	}
	s, _ := vals[1].(string)
	return s, nil
}

func splitIDs(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
