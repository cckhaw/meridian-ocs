// Package failpolicy decides what to do with a charging request when the
// authoritative data tier cannot be reached.
//
// The policy is hybrid:
//
//   - Prepaid subscribers fail CLOSED: no authoritative balance means no grant,
//     because usage cannot be recovered from a wallet that was never debited.
//   - Postpaid and high-value subscribers fail OPEN WITH A CAP: a small,
//     short-lived grant is issued from locally cached profile data, journalled
//     for later reconciliation, and bounded per grant, per subscriber and
//     process-wide so worst-case leakage is a known number.
//
// Anything the engine cannot positively classify as eligible is denied.
package failpolicy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// PaymentType is the subscriber's billing relationship.
type PaymentType uint8

const (
	PaymentUnknown PaymentType = iota
	Prepaid
	Postpaid
)

// Status mirrors the Module 5 lifecycle state machine.
type Status uint8

const (
	StatusUnknown Status = iota
	StatusActive
	StatusGrace
	StatusSuspended
	StatusTerminated
)

// Profile is the locally cached subscriber classification. It must be served
// from process memory: the whole point of this package is that the data tier
// is unavailable.
type Profile struct {
	Payment   PaymentType
	HighValue bool
	Status    Status
}

// ProfileSource looks up a cached profile without network I/O.
type ProfileSource interface {
	Lookup(subscriberID string) (Profile, bool)
}

// UnitKind is the unit a grant is expressed in.
type UnitKind uint8

const (
	UnitBytes UnitKind = iota
	UnitSeconds
	UnitEvents
	numKinds
)

// Caps bound fail-open grants for one unit kind.
type Caps struct {
	PerGrant      int64 // max units in a single grant
	PerSubscriber int64 // max cumulative units per subscriber per outage
	Global        int64 // max cumulative units across all subscribers per outage on this process
}

// Config is immutable once published; replace it wholesale with SetConfig.
type Config struct {
	// FailOpenEnabled is the kill switch. When false every request is denied
	// regardless of class, i.e. the system behaves as pure fail-closed.
	FailOpenEnabled bool
	// AllowPostpaid permits fail-open for postpaid subscribers.
	AllowPostpaid bool
	// AllowHighValue permits fail-open for high-value subscribers, including
	// high-value PREPAID ones. Their usage is recovered by reconciliation
	// from the journal, so unlike postpaid it carries real revenue risk.
	AllowHighValue bool
	// Validity is the grant validity time; keep it short so the network
	// element returns quickly and the data tier is retried.
	Validity time.Duration
	Caps     [numKinds]Caps
}

// DefaultConfig returns conservative defaults; operators are expected to
// tune the caps per deployment.
func DefaultConfig() *Config {
	c := &Config{
		FailOpenEnabled: true,
		AllowPostpaid:   true,
		AllowHighValue:  true,
		Validity:        30 * time.Second,
	}
	c.Caps[UnitBytes] = Caps{PerGrant: 10 << 20, PerSubscriber: 100 << 20, Global: 50 << 30}
	c.Caps[UnitSeconds] = Caps{PerGrant: 300, PerSubscriber: 1800, Global: 5_000_000}
	c.Caps[UnitEvents] = Caps{PerGrant: 5, PerSubscriber: 25, Global: 1_000_000}
	return c
}

// Request is the minimal slice of a charging request the policy needs.
type Request struct {
	SubscriberID string
	SessionID    string
	Kind         UnitKind
	Requested    int64
}

// Reason explains a decision; it is exported to metrics and logs.
type Reason uint8

const (
	ReasonGranted Reason = iota
	ReasonDisabled
	ReasonUnknownProfile
	ReasonNotActive
	ReasonPrepaid
	ReasonClassNotAllowed
	ReasonSubscriberCap
	ReasonGlobalBudget
	ReasonJournalFailed
	ReasonInvalidRequest
)

func (r Reason) String() string {
	switch r {
	case ReasonGranted:
		return "granted"
	case ReasonDisabled:
		return "fail_open_disabled"
	case ReasonUnknownProfile:
		return "unknown_profile"
	case ReasonNotActive:
		return "subscriber_not_active"
	case ReasonPrepaid:
		return "prepaid_fail_closed"
	case ReasonClassNotAllowed:
		return "class_not_allowed"
	case ReasonSubscriberCap:
		return "subscriber_cap_exhausted"
	case ReasonGlobalBudget:
		return "global_budget_exhausted"
	case ReasonJournalFailed:
		return "journal_failed"
	case ReasonInvalidRequest:
		return "invalid_request"
	}
	return "unknown"
}

// Decision is the outcome. Granted == 0 means deny.
type Decision struct {
	Granted   int64
	Validity  time.Duration
	FinalUnit bool // grant is the last one: signal Final-Unit-Indication to the NE
	Reason    Reason
}

// Allowed reports whether any units were granted.
func (d Decision) Allowed() bool { return d.Granted > 0 }

// Grant is the reconciliation record for one fail-open grant.
type Grant struct {
	SubscriberID string
	SessionID    string
	Kind         UnitKind
	Units        int64
	Epoch        uint64
	At           time.Time
	Prepaid      bool // true: must be debited from the wallet on reconciliation
}

// Journal durably (or at least reliably) records grants. Record must not
// block the signalling path; return an error when it cannot accept the entry.
// The engine refuses to grant when Record fails, so no grant is ever
// untracked.
type Journal interface {
	Record(Grant) error
}

const numShards = 256

type subUsage struct {
	epoch uint64
	used  [numKinds]int64
}

type shard struct {
	mu sync.Mutex
	m  map[string]*subUsage
}

// Engine makes fail-open/closed decisions. It is safe for concurrent use.
type Engine struct {
	cfg      atomic.Pointer[Config]
	profiles ProfileSource
	journal  Journal
	now      func() time.Time

	epoch      atomic.Uint64
	globalUsed [numKinds]atomic.Int64
	shards     [numShards]shard
}

// NewEngine builds an Engine. cfg may be nil to use DefaultConfig.
func NewEngine(profiles ProfileSource, journal Journal, cfg *Config) *Engine {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	e := &Engine{profiles: profiles, journal: journal, now: time.Now}
	e.cfg.Store(cfg)
	e.epoch.Store(1)
	for i := range e.shards {
		e.shards[i].m = make(map[string]*subUsage)
	}
	return e
}

// SetConfig atomically publishes a new configuration (AI Ops / config reload).
func (e *Engine) SetConfig(c *Config) { e.cfg.Store(c) }

// BeginOutage starts a new outage epoch: per-subscriber and global counters
// are logically reset. Wire it to Breaker.OnOpen.
func (e *Engine) BeginOutage() {
	e.epoch.Add(1)
	for i := range e.globalUsed {
		e.globalUsed[i].Store(0)
	}
}

// Epoch returns the current outage epoch.
func (e *Engine) Epoch() uint64 { return e.epoch.Load() }

var errJournalFull = errors.New("failpolicy: journal rejected entry")

// Decide returns the grant (possibly zero) for a request made while the data
// tier is unavailable.
func (e *Engine) Decide(req Request) Decision {
	cfg := e.cfg.Load()
	if req.SubscriberID == "" || req.Requested <= 0 || req.Kind >= numKinds {
		return Decision{Reason: ReasonInvalidRequest}
	}
	if !cfg.FailOpenEnabled {
		return Decision{Reason: ReasonDisabled}
	}

	p, ok := e.profiles.Lookup(req.SubscriberID)
	if !ok || p.Payment == PaymentUnknown || p.Status == StatusUnknown {
		return Decision{Reason: ReasonUnknownProfile}
	}
	if p.Status != StatusActive {
		return Decision{Reason: ReasonNotActive}
	}

	switch {
	case p.Payment == Postpaid && cfg.AllowPostpaid:
	case p.HighValue && cfg.AllowHighValue:
	case p.Payment == Prepaid:
		return Decision{Reason: ReasonPrepaid}
	default:
		return Decision{Reason: ReasonClassNotAllowed}
	}

	caps := cfg.Caps[req.Kind]
	epoch := e.epoch.Load()
	sh := &e.shards[fnv32(req.SubscriberID)%numShards]

	sh.mu.Lock()
	defer sh.mu.Unlock()
	u := sh.m[req.SubscriberID]
	if u == nil {
		u = &subUsage{epoch: epoch}
		sh.m[req.SubscriberID] = u
	} else if u.epoch != epoch {
		*u = subUsage{epoch: epoch}
	}

	want := min(req.Requested, caps.PerGrant, caps.PerSubscriber-u.used[req.Kind])
	if want <= 0 {
		return Decision{Reason: ReasonSubscriberCap}
	}
	got := e.reserveGlobal(req.Kind, want, caps.Global)
	if got <= 0 {
		return Decision{Reason: ReasonGlobalBudget}
	}

	err := e.journal.Record(Grant{
		SubscriberID: req.SubscriberID,
		SessionID:    req.SessionID,
		Kind:         req.Kind,
		Units:        got,
		Epoch:        epoch,
		At:           e.now(),
		Prepaid:      p.Payment == Prepaid,
	})
	if err != nil {
		e.globalUsed[req.Kind].Add(-got) // return the reservation
		return Decision{Reason: ReasonJournalFailed}
	}

	u.used[req.Kind] += got
	return Decision{
		Granted:   got,
		Validity:  cfg.Validity,
		FinalUnit: u.used[req.Kind] >= caps.PerSubscriber,
		Reason:    ReasonGranted,
	}
}

// reserveGlobal atomically takes up to want units from the process-wide
// budget and returns how many it got (possibly fewer, possibly 0).
func (e *Engine) reserveGlobal(k UnitKind, want, limit int64) int64 {
	c := &e.globalUsed[k]
	for {
		cur := c.Load()
		avail := limit - cur
		if avail <= 0 {
			return 0
		}
		take := min(want, avail)
		if c.CompareAndSwap(cur, cur+take) {
			return take
		}
	}
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ChanJournal is a bounded, non-blocking in-process Journal that hands
// entries to a drain goroutine (typically a Kafka producer). Record fails
// when the buffer is full, which makes the engine deny rather than lose track.
type ChanJournal struct{ C chan Grant }

// NewChanJournal creates a journal with the given buffer size.
func NewChanJournal(size int) *ChanJournal { return &ChanJournal{C: make(chan Grant, size)} }

// Record implements Journal.
func (j *ChanJournal) Record(g Grant) error {
	select {
	case j.C <- g:
		return nil
	default:
		return errJournalFull
	}
}
