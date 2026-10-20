// Package control is the guarded control plane through which an AI Ops agent
// (or a human operator) may tune a running process.
//
// The design assumes the caller can be wrong, compromised or prompt-injected,
// so safety lives here, not in the caller:
//
//   - Allow-list. Only parameters the process registered can be touched. There
//     is no way to name anything else.
//   - Clamping. A numeric value is forced into the parameter's [Min, Max]; an
//     enum or boolean must be one of its legal values. The caller is told what
//     was actually applied.
//   - Auto-revert. Every change is temporary: it reverts after its TTL (default
//     5 minutes, at most 1 hour) unless a principal confirms it. A confirmed
//     change lasts until the process restarts; durable configuration belongs to
//     the deployment (GitOps), not to this channel.
//   - Two-person rule. A parameter marked Risky is not applied when proposed: a
//     different principal must approve it, and a different principal from the
//     proposer must confirm it.
//   - One change per parameter at a time, a per-principal rate limit, and a
//     freeze switch that stops all new changes (reverts still run).
//   - Audit. Every step is recorded with who, what, why and the before/after.
package control

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kind is the type of a parameter's value.
type Kind uint8

const (
	Int Kind = iota + 1
	Float
	Bool
	Enum
)

func (k Kind) String() string {
	switch k {
	case Int:
		return "int"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case Enum:
		return "enum"
	}
	return "unknown"
}

// Param describes one tunable.
type Param struct {
	Name        string
	Description string
	Kind        Kind
	Min, Max    float64  // Int and Float: the legal range, inclusive
	Values      []string // Enum: the legal values
	// Risky parameters need a second principal's approval before they apply.
	Risky bool
	// Get returns the current value in the form Set accepts.
	Get func() string
	// Set applies a value that has already been validated and clamped. It must
	// be safe for concurrent use with the code that reads the setting.
	Set func(string) error
}

// State of a change.
type State string

const (
	PendingApproval State = "pending_approval"
	Applied         State = "applied"   // in effect, will revert at RevertsAt unless confirmed
	Confirmed       State = "confirmed" // permanent until restart
	Reverted        State = "reverted"  // reverted by a principal
	Expired         State = "expired"   // reverted by its timer
	Cancelled       State = "cancelled" // approval never came, or the controller closed
	Failed          State = "failed"    // the setter refused the value
)

// Change is the record of one proposal.
type Change struct {
	ID                               string
	Param                            string
	Previous                         string
	Requested                        string
	Applied                          string
	Clamped                          bool
	Principal                        string
	Approver                         string
	Reason                           string
	State                            State
	ProposedAt, AppliedAt, RevertsAt time.Time
	TTL                              time.Duration
}

// Event is one audit record.
type Event struct {
	At        time.Time
	Type      string // propose, apply, approve, confirm, revert, expire, cancel, fail, freeze, reject
	ChangeID  string
	Param     string
	Principal string
	Detail    string
}

var (
	ErrUnknownParam = errors.New("control: unknown parameter")
	ErrInvalidValue = errors.New("control: invalid value")
	ErrReason       = errors.New("control: a reason of 1-256 characters is required")
	ErrBusy         = errors.New("control: the parameter already has an unconfirmed change")
	ErrFrozen       = errors.New("control: changes are frozen")
	ErrRateLimited  = errors.New("control: too many changes from this principal")
	ErrNotFound     = errors.New("control: no such change")
	ErrState        = errors.New("control: the change is not in a state that allows this")
	ErrSamePerson   = errors.New("control: this needs a different principal than the proposer")
	ErrNoPrincipal  = errors.New("control: an authenticated principal is required")
)

// Timer is the part of time.Timer the controller needs.
type Timer interface{ Stop() bool }

// Config configures a Controller.
type Config struct {
	Service string
	// DefaultTTL applies when a proposal gives none (default 5m); MaxTTL caps
	// any TTL (default 1h).
	DefaultTTL, MaxTTL time.Duration
	// ApprovalTTL is how long a risky proposal waits for approval (default 10m).
	ApprovalTTL time.Duration
	// ChangesPerMinute is each principal's rate limit (default 10, burst equal).
	ChangesPerMinute int
	// Audit receives every event (optional; must not block).
	Audit func(Event)
	// Now and AfterFunc are injectable for tests.
	Now       func() time.Time
	AfterFunc func(d time.Duration, f func()) Timer
}

const auditKeep = 1000

// Controller owns the registered parameters and the changes made to them.
type Controller struct {
	cfg Config

	mu      sync.Mutex
	params  map[string]*Param
	changes map[string]*Change
	active  map[string]string // param -> id of its pending or applied change
	timers  map[string]Timer
	buckets map[string]*bucket
	audit   []Event
	frozen  bool
	seq     uint64
	closed  bool
}

type bucket struct {
	tokens float64
	at     time.Time
}

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,63}$`)

// New creates a Controller.
func New(cfg Config) *Controller {
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 5 * time.Minute
	}
	if cfg.MaxTTL <= 0 {
		cfg.MaxTTL = time.Hour
	}
	if cfg.ApprovalTTL <= 0 {
		cfg.ApprovalTTL = 10 * time.Minute
	}
	if cfg.ChangesPerMinute <= 0 {
		cfg.ChangesPerMinute = 10
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.AfterFunc == nil {
		cfg.AfterFunc = func(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
	}
	return &Controller{cfg: cfg, params: map[string]*Param{}, changes: map[string]*Change{},
		active: map[string]string{}, timers: map[string]Timer{}, buckets: map[string]*bucket{}}
}

// Service is the name the controller was created for.
func (c *Controller) Service() string { return c.cfg.Service }

// Register adds a parameter. It validates the definition, including that the
// current value is legal, so a mis-declared range is caught at start-up.
func (c *Controller) Register(p Param) error {
	switch {
	case !nameRe.MatchString(p.Name):
		return fmt.Errorf("control: bad parameter name %q", p.Name)
	case p.Get == nil || p.Set == nil:
		return fmt.Errorf("control: %s needs Get and Set", p.Name)
	case p.Kind < Int || p.Kind > Enum:
		return fmt.Errorf("control: %s has no kind", p.Name)
	case (p.Kind == Int || p.Kind == Float) && !(p.Min <= p.Max) || math.IsNaN(p.Min) || math.IsNaN(p.Max) || math.IsInf(p.Min, 0) || math.IsInf(p.Max, 0):
		return fmt.Errorf("control: %s has an invalid range", p.Name)
	case p.Kind == Enum && len(p.Values) < 2:
		return fmt.Errorf("control: %s needs at least two enum values", p.Name)
	}
	if _, clamped, err := normalize(&p, p.Get()); err != nil || clamped {
		return fmt.Errorf("control: the current value of %s (%q) is not within its declared limits: %v", p.Name, p.Get(), err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, dup := c.params[p.Name]; dup {
		return fmt.Errorf("control: %s is already registered", p.Name)
	}
	cp := p
	c.params[p.Name] = &cp
	return nil
}

// ParamInfo describes a parameter and its current value.
type ParamInfo struct {
	Name, Description string
	Kind              Kind
	Current           string
	Min, Max          float64
	Values            []string
	Risky             bool
	// ActiveChange is the id of its unconfirmed change, if any.
	ActiveChange string
}

// List returns the parameters sorted by name.
func (c *Controller) List() []ParamInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ParamInfo, 0, len(c.params))
	for _, p := range c.params {
		out = append(out, ParamInfo{Name: p.Name, Description: p.Description, Kind: p.Kind, Current: p.Get(),
			Min: p.Min, Max: p.Max, Values: append([]string(nil), p.Values...), Risky: p.Risky, ActiveChange: c.active[p.Name]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// normalize validates value for p and returns the value to apply and whether
// it was clamped.
func normalize(p *Param, value string) (string, bool, error) {
	value = strings.TrimSpace(value)
	switch p.Kind {
	case Int, Float:
		f, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return "", false, fmt.Errorf("%w: %q is not a finite number", ErrInvalidValue, value)
		}
		if p.Kind == Int {
			f = math.Round(f)
		}
		clamped := false
		if f < p.Min {
			f, clamped = p.Min, true
		} else if f > p.Max {
			f, clamped = p.Max, true
		}
		if p.Kind == Int {
			return strconv.FormatInt(int64(f), 10), clamped, nil
		}
		return strconv.FormatFloat(f, 'g', -1, 64), clamped, nil
	case Bool:
		if value != "true" && value != "false" {
			return "", false, fmt.Errorf("%w: want true or false", ErrInvalidValue)
		}
		return value, false, nil
	case Enum:
		for _, v := range p.Values {
			if v == value {
				return value, false, nil
			}
		}
		return "", false, fmt.Errorf("%w: want one of %s", ErrInvalidValue, strings.Join(p.Values, ", "))
	}
	return "", false, ErrInvalidValue
}

func (c *Controller) record(typ string, ch *Change, principal, detail string) {
	e := Event{At: c.cfg.Now(), Type: typ, Principal: principal, Detail: detail}
	if ch != nil {
		e.ChangeID, e.Param = ch.ID, ch.Param
	}
	c.audit = append(c.audit, e)
	if len(c.audit) > auditKeep {
		c.audit = append([]Event(nil), c.audit[len(c.audit)-auditKeep:]...)
	}
	if c.cfg.Audit != nil {
		c.cfg.Audit(e)
	}
}

// allow takes a token from the principal's bucket.
func (c *Controller) allow(principal string, now time.Time) bool {
	rate := float64(c.cfg.ChangesPerMinute)
	b := c.buckets[principal]
	if b == nil {
		if len(c.buckets) > 1000 { // bounded: forget idle principals
			for k, v := range c.buckets {
				if now.Sub(v.at) > time.Hour {
					delete(c.buckets, k)
				}
			}
		}
		b = &bucket{tokens: rate, at: now}
		c.buckets[principal] = b
	}
	b.tokens = math.Min(rate, b.tokens+now.Sub(b.at).Minutes()*rate)
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Propose requests a change. For an ordinary parameter it is applied at once
// (and reverts after ttl); for a risky one it waits for Approve.
func (c *Controller) Propose(principal, param, value string, ttl time.Duration, reason string) (Change, error) {
	reason = strings.TrimSpace(reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Now()
	reject := func(err error) (Change, error) {
		c.record("reject", nil, principal, fmt.Sprintf("%s=%q: %v", param, value, err))
		return Change{}, err
	}
	switch {
	case principal == "":
		return reject(ErrNoPrincipal)
	case reason == "" || len(reason) > 256:
		return reject(ErrReason)
	case c.closed || c.frozen:
		return reject(ErrFrozen)
	}
	p := c.params[param]
	if p == nil {
		return reject(fmt.Errorf("%w: %q", ErrUnknownParam, param))
	}
	if !c.allow(principal, now) {
		return reject(ErrRateLimited)
	}
	if id, busy := c.active[param]; busy {
		return reject(fmt.Errorf("%w (%s)", ErrBusy, id))
	}
	applied, clamped, err := normalize(p, value)
	if err != nil {
		return reject(err)
	}
	if ttl <= 0 {
		ttl = c.cfg.DefaultTTL
	}
	if ttl > c.cfg.MaxTTL {
		ttl = c.cfg.MaxTTL
	}
	c.seq++
	ch := &Change{ID: fmt.Sprintf("chg-%d-%d", now.Unix(), c.seq), Param: param, Previous: p.Get(), Requested: value,
		Applied: applied, Clamped: clamped, Principal: principal, Reason: reason, ProposedAt: now, TTL: ttl}
	c.changes[ch.ID] = ch
	c.active[param] = ch.ID
	c.record("propose", ch, principal, fmt.Sprintf("%s: %s -> %s (ttl %s): %s", param, ch.Previous, applied, ttl, reason))

	if p.Risky {
		ch.State = PendingApproval
		c.timers[ch.ID] = c.cfg.AfterFunc(c.cfg.ApprovalTTL, func() { c.expirePending(ch.ID) })
		return *ch, nil
	}
	if err := c.apply(ch, p, principal); err != nil {
		return *ch, err
	}
	return *ch, nil
}

// apply sets the value and arms the revert timer. c.mu is held.
func (c *Controller) apply(ch *Change, p *Param, by string) error {
	if err := p.Set(ch.Applied); err != nil {
		ch.State = Failed
		delete(c.active, ch.Param)
		c.record("fail", ch, by, err.Error())
		return fmt.Errorf("control: setting %s: %w", ch.Param, err)
	}
	now := c.cfg.Now()
	ch.State, ch.AppliedAt, ch.RevertsAt = Applied, now, now.Add(ch.TTL)
	c.record("apply", ch, by, fmt.Sprintf("%s = %s, reverts at %s", ch.Param, ch.Applied, ch.RevertsAt.UTC().Format(time.RFC3339)))
	c.timers[ch.ID] = c.cfg.AfterFunc(ch.TTL, func() { c.expire(ch.ID) })
	return nil
}

func (c *Controller) stopTimer(id string) {
	if t := c.timers[id]; t != nil {
		t.Stop()
		delete(c.timers, id)
	}
}

func (c *Controller) expirePending(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.changes[id]
	if ch == nil || ch.State != PendingApproval {
		return
	}
	ch.State = Cancelled
	delete(c.active, ch.Param)
	delete(c.timers, id)
	c.record("cancel", ch, "", "no approval before the deadline")
}

func (c *Controller) expire(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.changes[id]
	if ch == nil || ch.State != Applied {
		return
	}
	c.revertLocked(ch, Expired, "expire", "")
}

// revertLocked restores the previous value. A setter that refuses to restore is
// loud (an audit event) and the change stays active so it is not forgotten.
func (c *Controller) revertLocked(ch *Change, to State, typ, by string) {
	p := c.params[ch.Param]
	if err := p.Set(ch.Previous); err != nil {
		c.record("revert_failed", ch, by, fmt.Sprintf("could not restore %s to %s: %v", ch.Param, ch.Previous, err))
		return
	}
	c.stopTimer(ch.ID)
	ch.State = to
	delete(c.active, ch.Param)
	c.record(typ, ch, by, fmt.Sprintf("%s restored to %s", ch.Param, ch.Previous))
}

// Approve applies a pending risky change. The approver must not be the proposer.
func (c *Controller) Approve(id, principal string) (Change, error) {
	if principal == "" {
		return Change{}, ErrNoPrincipal
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.changes[id]
	switch {
	case ch == nil:
		return Change{}, ErrNotFound
	case ch.State != PendingApproval:
		return *ch, ErrState
	case c.closed || c.frozen:
		return *ch, ErrFrozen
	case principal == ch.Principal:
		c.record("reject", ch, principal, "self-approval refused")
		return *ch, ErrSamePerson
	}
	c.stopTimer(id)
	ch.Approver = principal
	c.record("approve", ch, principal, "")
	// The value is re-read: the world may have moved while the proposal waited.
	ch.Previous = c.params[ch.Param].Get()
	if err := c.apply(ch, c.params[ch.Param], principal); err != nil {
		return *ch, err
	}
	return *ch, nil
}

// Confirm makes an applied change permanent for the life of the process. For a
// risky parameter the confirmer must not be the proposer.
func (c *Controller) Confirm(id, principal string) error {
	if principal == "" {
		return ErrNoPrincipal
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.changes[id]
	switch {
	case ch == nil:
		return ErrNotFound
	case ch.State != Applied:
		return ErrState
	case c.params[ch.Param].Risky && principal == ch.Principal:
		c.record("reject", ch, principal, "confirming one's own risky change refused")
		return ErrSamePerson
	}
	c.stopTimer(id)
	ch.State = Confirmed
	delete(c.active, ch.Param)
	c.record("confirm", ch, principal, "")
	return nil
}

// Revert undoes an applied change at once, or cancels a pending one.
func (c *Controller) Revert(id, principal string) error {
	if principal == "" {
		return ErrNoPrincipal
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.changes[id]
	switch {
	case ch == nil:
		return ErrNotFound
	case ch.State == PendingApproval:
		c.stopTimer(id)
		ch.State = Cancelled
		delete(c.active, ch.Param)
		c.record("cancel", ch, principal, "withdrawn")
		return nil
	case ch.State != Applied:
		return ErrState
	}
	c.revertLocked(ch, Reverted, "revert", principal)
	if ch.State != Reverted {
		return fmt.Errorf("control: could not restore %s", ch.Param)
	}
	return nil
}

// Freeze stops (or resumes) new proposals and approvals. Reverts continue.
func (c *Controller) Freeze(on bool, principal, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen = on
	c.record("freeze", nil, principal, fmt.Sprintf("frozen=%v: %s", on, reason))
}

// Frozen reports the freeze switch.
func (c *Controller) Frozen() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.frozen }

// Changes returns recent changes, newest first.
func (c *Controller) Changes() []Change {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Change, 0, len(c.changes))
	for _, ch := range c.changes {
		out = append(out, *ch)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ProposedAt.After(out[j].ProposedAt) || (out[i].ProposedAt.Equal(out[j].ProposedAt) && out[i].ID > out[j].ID)
	})
	return out
}

// Audit returns the retained audit events, oldest first.
func (c *Controller) Audit() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.audit...)
}

// Close reverts every unconfirmed change and refuses new ones.
func (c *Controller) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, ch := range c.changes {
		switch ch.State {
		case PendingApproval:
			c.stopTimer(ch.ID)
			ch.State = Cancelled
			delete(c.active, ch.Param)
			c.record("cancel", ch, "", "controller closed")
		case Applied:
			c.revertLocked(ch, Reverted, "revert", "")
		}
	}
}
