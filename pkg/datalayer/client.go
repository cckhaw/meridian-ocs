// Package datalayer is the Go client for the hot subscriber state held in
// Redis. All monetary mutations run as single Lua scripts so that bucket
// debit, reservation, session state and ledger intent are one atomic unit.
package datalayer

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

//go:embed lua/common.lua
var commonLua string

//go:embed lua/charge.lua
var chargeLua string

//go:embed lua/sweep.lua
var sweepLua string

// Mode is the credit-control step performed by Charge.
type Mode byte

const (
	ModeInitial   Mode = 'I'
	ModeUpdate    Mode = 'U'
	ModeTerminate Mode = 'T'
	ModeEvent     Mode = 'E'
	// ModeLate and ModeLateUnapplied settle the usage of an update that was refused while the data tier was down,
	// after the fact, without reserving anything (see charge.lua). ModeLateUnapplied is for one that provably never
	// reached the data tier; ModeLate for one whose outcome is unknown.
	ModeLate          Mode = 'L'
	ModeLateUnapplied Mode = 'N'
)

// Class orders and prices buckets: promo, then bundle, then cash.
type Class string

const (
	ClassPromo  Class = "promo"
	ClassBundle Class = "bundle"
	ClassCash   Class = "cash"
)

// Numeric limits imposed by Lua's double arithmetic (see lua/common.lua).
const (
	MaxUnits = 1 << 40
	MaxNum   = 1 << 20
	MaxDen   = 1 << 12
)

// Rate prices one unit in a bucket class: cost = ceil(units * Num / Den).
// Promo and bundle buckets use 1/1.
type Rate struct {
	Class Class
	Num   int64
	Den   int64
}

// Item is one rating group inside a Request.
type Item struct {
	RatingGroup uint32
	Kind        canonical.UnitKind
	Requested   int64
	Used        int64
	Rates       []Rate
}

// Request is one atomic charging step. Callers may reuse a Request (and its
// slices) across calls; the client does not retain it.
type Request struct {
	SubscriberID string
	SessionID    string
	Mode         Mode
	ReqNo        uint32
	Now          time.Time
	Validity     time.Duration
	Grace        time.Duration
	Items        []Item
}

// Code is the script-level outcome.
type Code int64

const (
	CodeOK             Code = 0
	CodeReplayed       Code = 1 // stored answer returned; no state changed
	CodeUnknownSession Code = 2
	CodeStale          Code = 3
)

// ItemCode is the per-item outcome.
type ItemCode int64

const (
	ItemGranted ItemCode = iota
	ItemPartialFinal
	ItemLimitReached
	ItemNoBucket
	ItemBarred // the subscriber is suspended or terminated
)

// ItemResult is the answer for one Item.
type ItemResult struct {
	RatingGroup uint32
	Code        ItemCode
	Granted     int64
	ValidityS   uint32
	Final       bool
	Shortfall   int64
}

// Response is reusable across calls.
type Response struct {
	Code  Code
	Items []ItemResult
	// Replication tells whether a committing step reached the configured
	// number of replicas (see WithDurability).
	Replication Replication
}

// Client talks to Redis (standalone, sentinel or cluster).
type Client struct {
	rdb       redis.UniversalClient
	charge    *redis.Script
	sweep     *redis.Script
	provision *redis.Script
	dur       *durabilityState
}

// New wraps an existing go-redis client.
func New(rdb redis.UniversalClient) *Client {
	return &Client{
		rdb:       rdb,
		charge:    redis.NewScript(commonLua + "\n" + chargeLua),
		sweep:     redis.NewScript(commonLua + "\n" + sweepLua),
		provision: redis.NewScript(commonLua + "\n" + provisionLua),
	}
}

// Ping checks connectivity.
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

var argPool = sync.Pool{New: func() any { s := make([]any, 0, 64); return &s }}

// Prefix returns the hash-tagged key prefix for a subscriber.
func Prefix(subscriberID string) string { return "{sub:" + subscriberID + "}" }

func validID(s string) bool {
	return s != "" && !strings.ContainsAny(s, "{}")
}

// ErrInvalid is returned for requests that violate documented bounds.
var ErrInvalid = errors.New("datalayer: invalid request")

func (r *Request) validate() error {
	if !validID(r.SubscriberID) || r.SessionID == "" {
		return fmt.Errorf("%w: subscriber/session id", ErrInvalid)
	}
	switch r.Mode {
	case ModeInitial, ModeUpdate, ModeTerminate, ModeEvent, ModeLate, ModeLateUnapplied:
	default:
		return fmt.Errorf("%w: mode", ErrInvalid)
	}
	if r.Validity < 0 || r.Grace < 0 || r.Validity > time.Hour {
		return fmt.Errorf("%w: validity", ErrInvalid)
	}
	for i := range r.Items {
		it := &r.Items[i]
		if it.Requested < 0 || it.Requested > MaxUnits || it.Used < 0 || it.Used > MaxUnits {
			return fmt.Errorf("%w: units out of range", ErrInvalid)
		}
		for _, rt := range it.Rates {
			if rt.Num <= 0 || rt.Num > MaxNum || rt.Den <= 0 || rt.Den > MaxDen {
				return fmt.Errorf("%w: rate out of range", ErrInvalid)
			}
		}
	}
	return nil
}

// NotSent reports whether err proves that the command never reached the server: the connection could not be made.
// Anything else (a timeout, a reset, a closed connection) leaves it unknown whether the command was applied.
func NotSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// Charge executes one credit-control step atomically. A non-nil error means
// the data tier was unreachable or the script failed; business outcomes
// (unknown session, limit reached, ...) are reported in resp.
func (c *Client) Charge(ctx context.Context, req *Request, resp *Response) error {
	if err := req.validate(); err != nil {
		return err
	}
	ap := argPool.Get().(*[]any)
	args := (*ap)[:0]
	args = append(args, string(rune(req.Mode)), req.SessionID, int64(req.ReqNo),
		req.Now.UnixMilli(), int64(req.Validity/time.Second), req.Grace.Milliseconds(), int64(len(req.Items)))
	for i := range req.Items {
		it := &req.Items[i]
		args = append(args, int64(it.RatingGroup), it.Kind.String(), it.Requested, it.Used, int64(len(it.Rates)))
		for _, rt := range it.Rates {
			args = append(args, string(rt.Class), rt.Num, rt.Den)
		}
	}
	// Updates, terminates and events commit usage: their writes must survive
	// a primary failure. An initial reservation need not: if it is lost, the
	// final settlement finds no reservation and charges the reported usage.
	commit := req.Mode != ModeInitial
	v, rep, err := c.run(ctx, c.charge, []string{Prefix(req.SubscriberID)}, args, commit)
	clear(args)
	*ap = args[:0]
	argPool.Put(ap)
	if err != nil {
		return err
	}
	res, ok := v.([]any)
	if !ok {
		return errors.New("datalayer: unexpected script reply type")
	}
	resp.Replication = rep
	return decode(res, resp)
}

const itemWidth = 6

func decode(res []any, resp *Response) error {
	resp.Items = resp.Items[:0]
	if len(res) == 0 {
		return errors.New("datalayer: empty script reply")
	}
	code, _ := res[0].(int64)
	resp.Code = Code(code)
	if resp.Code != CodeOK && resp.Code != CodeReplayed {
		return nil
	}
	if len(res) < 2 {
		return errors.New("datalayer: truncated script reply")
	}
	n, _ := res[1].(int64)
	if int64(len(res)) != 2+n*itemWidth {
		return fmt.Errorf("datalayer: malformed script reply (%d values for %d items)", len(res), n)
	}
	for i := int64(0); i < n; i++ {
		v := res[2+i*itemWidth:]
		get := func(j int) int64 { x, _ := v[j].(int64); return x }
		resp.Items = append(resp.Items, ItemResult{
			RatingGroup: uint32(get(0)),
			Code:        ItemCode(get(1)),
			Granted:     get(2),
			ValidityS:   uint32(get(3)),
			Final:       get(4) == 1,
			Shortfall:   get(5),
		})
	}
	return nil
}

// SweepExpired refunds in full any reservation of the subscriber whose expiry
// has passed. It returns how many were processed (at most 64 per call).
func (c *Client) SweepExpired(ctx context.Context, subscriberID string, now time.Time) (int, error) {
	if !validID(subscriberID) {
		return 0, fmt.Errorf("%w: subscriber id", ErrInvalid)
	}
	n, err := c.sweep.Run(ctx, c.rdb, []string{Prefix(subscriberID)}, now.UnixMilli()).Int()
	return n, err
}

// Bucket is a balance holder. Order decides consumption order (lowest first).
type Bucket struct {
	ID        string
	Class     Class
	Kind      canonical.UnitKind
	AnyKind   bool // cash buckets pay for any unit kind
	Balance   int64
	ExpiresAt time.Time // zero = never
	Order     float64
}

// PutBucket creates or replaces a bucket. It is for provisioning and tests;
// balance changes on live buckets must go through idempotent provisioning
// commands, not this call.
func (c *Client) PutBucket(ctx context.Context, subscriberID string, b Bucket) error {
	if !validID(subscriberID) || b.ID == "" || strings.ContainsAny(b.ID, "|;") {
		return fmt.Errorf("%w: id", ErrInvalid)
	}
	p := Prefix(subscriberID)
	kind := b.Kind.String()
	if b.AnyKind {
		kind = "any"
	}
	var exp int64
	if !b.ExpiresAt.IsZero() {
		exp = b.ExpiresAt.UnixMilli()
	}
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, p+":b:"+b.ID, "cls", string(b.Class), "kind", kind, "bal", b.Balance, "exp", exp)
	pipe.ZAdd(ctx, p+":ord", redis.Z{Score: b.Order, Member: b.ID})
	_, err := pipe.Exec(ctx)
	return err
}

// Balance returns the current balance of a bucket.
func (c *Client) Balance(ctx context.Context, subscriberID, bucketID string) (int64, error) {
	s, err := c.rdb.HGet(ctx, Prefix(subscriberID)+":b:"+bucketID, "bal").Result()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// OpenReservations returns the number of reservations not yet settled or swept.
func (c *Client) OpenReservations(ctx context.Context, subscriberID string) (int64, error) {
	return c.rdb.ZCard(ctx, Prefix(subscriberID)+":rx").Result()
}
