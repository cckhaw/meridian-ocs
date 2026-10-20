// Package status keeps the record of each command that the API reports through
// GetCommand: accepted, then applied or failed.
//
// It is advisory. What guarantees a command is applied once is the idempotency
// token inside the subscriber's own Redis slot (datalayer.Provision); losing a
// status record can only cause a command to be queued again, which is
// harmless.
package status

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

// State of a command.
type State string

const (
	// PendingApproval: waiting for a second person (package approval). Nothing has been queued.
	PendingApproval State = "pending_approval"
	Accepted        State = "accepted"
	Applied         State = "applied"
	Failed          State = "failed"
	// Rejected: an approver (or the requester) declined it; it will never be applied.
	Rejected State = "rejected"
	// Expired: nobody approved it in time; it will never be applied. Reported when read, never stored.
	Expired State = "expired"
)

// Record is what is known about a command.
type Record struct {
	ID          string
	Type        command.Type
	Key         string
	State       State
	Outcome     string
	Message     string
	Balance     int64
	OldBalance  int64
	From, To    string
	Version     int64
	Fingerprint string
	AcceptedAt  time.Time
	CompletedAt time.Time

	// Approval (only for commands that needed it).
	Rule         string // the policy rule that required approval
	RequestedBy  string
	ApprovedBy   string
	ApprovedAt   time.Time
	RejectedBy   string
	RejectReason string
	ExpiresAt    time.Time // while pending approval
}

// Final reports whether the command has finished: nothing more will happen to it.
func (r Record) Final() bool {
	return r.State == Applied || r.State == Failed || r.State == Rejected || r.State == Expired
}

// Result is the final outcome written by a worker.
type Result struct {
	State      State
	Outcome    string
	Message    string
	Balance    int64
	OldBalance int64
	From, To   string
	Version    int64
}

// ErrNotFound: no record for the id (never seen, or older than the retention).
var ErrNotFound = errors.New("status: command not found")

// Store is the Redis-backed status store.
type Store struct {
	rdb      redis.UniversalClient
	prefix   string
	ttl      time.Duration
	now      func() time.Time
	begin    *redis.Script
	complete *redis.Script
	abort    *redis.Script
}

const beginLua = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'fp', ARGV[1], 'type', ARGV[2], 'key', ARGV[3], 'state', 'accepted', 'at', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1`

// A record is completed once: the first final outcome wins.
const completeLua = `
if redis.call('HGET', KEYS[1], 'state') ~= 'accepted' then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[1], 'outcome', ARGV[2], 'msg', ARGV[3], 'bal', ARGV[4], 'old', ARGV[5],
           'from', ARGV[6], 'to', ARGV[7], 'ver', ARGV[8], 'done', ARGV[9])
return 1`

const abortLua = `
if redis.call('HGET', KEYS[1], 'state') == 'accepted' then return redis.call('DEL', KEYS[1]) end
return 0`

// New builds a Store. prefix defaults to "cmd:", ttl to 7 days.
func New(rdb redis.UniversalClient, prefix string, ttl time.Duration) *Store {
	if prefix == "" {
		prefix = "cmd:"
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &Store{rdb: rdb, prefix: prefix, ttl: ttl, now: time.Now,
		begin: redis.NewScript(beginLua), complete: redis.NewScript(completeLua), abort: redis.NewScript(abortLua)}
}

// Begin records a newly accepted command. If the id is already known it
// returns the existing record and created == false; the caller compares
// Fingerprint to tell a repeat from a conflicting reuse.
func (s *Store) Begin(ctx context.Context, c *command.Command) (rec Record, created bool, err error) {
	k := s.prefix + c.ID
	n, err := s.begin.Run(ctx, s.rdb, []string{k}, c.Fingerprint(), string(c.Type), c.Key, c.AcceptedAt, s.ttl.Milliseconds()).Int()
	if err != nil {
		return Record{}, false, err
	}
	if n == 1 {
		return Record{ID: c.ID, Type: c.Type, Key: c.Key, State: Accepted, Fingerprint: c.Fingerprint(),
			AcceptedAt: time.UnixMilli(c.AcceptedAt)}, true, nil
	}
	rec, err = s.Get(ctx, c.ID)
	return rec, false, err
}

// Complete records the final outcome. It reports whether this call was the
// one that completed the record.
func (s *Store) Complete(ctx context.Context, id string, r Result) (bool, error) {
	n, err := s.complete.Run(ctx, s.rdb, []string{s.prefix + id}, string(r.State), r.Outcome, r.Message,
		r.Balance, r.OldBalance, r.From, r.To, r.Version, s.now().UnixMilli()).Int()
	return n == 1, err
}

// Abort forgets a command that was accepted but could not be queued, so the
// caller can retry with the same id.
func (s *Store) Abort(ctx context.Context, id string) error {
	return s.abort.Run(ctx, s.rdb, []string{s.prefix + id}).Err()
}

// Get reads a record.
func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	m, err := s.rdb.HGetAll(ctx, s.prefix+id).Result()
	if err != nil {
		return Record{}, err
	}
	if len(m) == 0 {
		return Record{}, ErrNotFound
	}
	num := func(k string) int64 { n, _ := strconv.ParseInt(m[k], 10, 64); return n }
	ms := func(k string) time.Time {
		if n := num(k); n != 0 {
			return time.UnixMilli(n)
		}
		return time.Time{}
	}
	rec := Record{ID: id, Type: command.Type(m["type"]), Key: m["key"], State: State(m["state"]),
		Outcome: m["outcome"], Message: m["msg"], Balance: num("bal"), OldBalance: num("old"),
		From: m["from"], To: m["to"], Version: num("ver"), Fingerprint: m["fp"],
		AcceptedAt: ms("at"), CompletedAt: ms("done"),
		Rule: m["rule"], RequestedBy: m["by"], ApprovedBy: m["appr"], ApprovedAt: ms("apprat"),
		RejectedBy: m["rejby"], RejectReason: m["rejwhy"], ExpiresAt: ms("exp")}
	// A request nobody approved in time is reported as expired; the record is not rewritten.
	if rec.State == PendingApproval && !rec.ExpiresAt.IsZero() && !s.now().Before(rec.ExpiresAt) {
		rec.State = Expired
	}
	return rec, nil
}

// ---- approval ---------------------------------------------------------------
//
// A command that needs a second person is stored whole, in state
// pending_approval, and indexed by expiry in a sorted set. Approving it is one
// Lua call that checks the state, the expiry and that the approver is not the
// requester, and flips the record to accepted; only then is the command queued.
// The decisions are in Redis, not in the process, so two approvers racing for
// the same request cannot both win.

// Outcome of an approval decision.
type Outcome int

const (
	Done         Outcome = iota // the decision was recorded
	NotFound                    // no such command
	NotPending                  // already decided, or never needed approval
	TimedOut                    // the request expired
	SelfApproval                // the approver is the requester
)

func (s *Store) pendingKey() string { return s.prefix + "~pending" } // '~' cannot occur in a command id

const beginPendingLua = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'fp', ARGV[1], 'type', ARGV[2], 'key', ARGV[3], 'state', 'pending_approval', 'at', ARGV[4],
           'by', ARGV[5], 'rule', ARGV[6], 'exp', ARGV[7], 'cmd', ARGV[8])
redis.call('PEXPIRE', KEYS[1], ARGV[9])
redis.call('ZADD', KEYS[2], ARGV[7], ARGV[10])
return 1`

// ARGV: approver, now, allowSelf, retention ms
const approveLua = `
local st = redis.call('HGET', KEYS[1], 'state')
if not st then return {0} end
if st ~= 'pending_approval' then return {2} end
if tonumber(redis.call('HGET', KEYS[1], 'exp')) <= tonumber(ARGV[2]) then return {3} end
if redis.call('HGET', KEYS[1], 'by') == ARGV[1] and ARGV[3] ~= '1' then return {4} end
redis.call('HSET', KEYS[1], 'state', 'accepted', 'appr', ARGV[1], 'apprat', ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[5])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {1, redis.call('HGET', KEYS[1], 'cmd')}`

// ARGV: approver
const unapproveLua = `
if redis.call('HGET', KEYS[1], 'state') ~= 'accepted' or redis.call('HGET', KEYS[1], 'appr') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'pending_approval')
redis.call('HDEL', KEYS[1], 'appr', 'apprat')
redis.call('ZADD', KEYS[2], redis.call('HGET', KEYS[1], 'exp'), ARGV[2])
return 1`

// ARGV: by, reason, now
const rejectLua = `
local st = redis.call('HGET', KEYS[1], 'state')
if not st then return 0 end
if st ~= 'pending_approval' then return 2 end
if tonumber(redis.call('HGET', KEYS[1], 'exp')) <= tonumber(ARGV[3]) then return 3 end
redis.call('HSET', KEYS[1], 'state', 'rejected', 'rejby', ARGV[1], 'rejwhy', ARGV[2], 'done', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[4])
return 1`

// BeginPending records a command that has to be approved before it is queued.
// As with Begin, an existing id returns the existing record and created == false.
func (s *Store) BeginPending(ctx context.Context, c *command.Command, rule string, ttl time.Duration) (rec Record, created bool, err error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return Record{}, false, err
	}
	exp := time.UnixMilli(c.AcceptedAt).Add(ttl)
	n, err := redis.NewScript(beginPendingLua).Run(ctx, s.rdb, []string{s.prefix + c.ID, s.pendingKey()},
		c.Fingerprint(), string(c.Type), c.Key, c.AcceptedAt, c.Principal, rule, exp.UnixMilli(), string(raw),
		s.ttl.Milliseconds(), c.ID).Int()
	if err != nil {
		return Record{}, false, err
	}
	if n == 1 {
		return Record{ID: c.ID, Type: c.Type, Key: c.Key, State: PendingApproval, Fingerprint: c.Fingerprint(),
			AcceptedAt: time.UnixMilli(c.AcceptedAt), Rule: rule, RequestedBy: c.Principal, ExpiresAt: exp}, true, nil
	}
	rec, err = s.Get(ctx, c.ID)
	return rec, false, err
}

// Approve records the approval and returns the stored command, ready to queue.
// Only one of any number of concurrent approvers gets Done.
func (s *Store) Approve(ctx context.Context, id, approver string, allowSelf bool) (*command.Command, Outcome, error) {
	self := "0"
	if allowSelf {
		self = "1"
	}
	res, err := redis.NewScript(approveLua).Run(ctx, s.rdb, []string{s.prefix + id, s.pendingKey()},
		approver, s.now().UnixMilli(), self, s.ttl.Milliseconds(), id).Slice()
	if err != nil {
		return nil, 0, err
	}
	switch res[0].(int64) {
	case 1:
		var c command.Command
		if err := json.Unmarshal([]byte(res[1].(string)), &c); err != nil {
			return nil, 0, err
		}
		c.ApprovedBy, c.ApprovedAt = approver, s.now().UnixMilli()
		return &c, Done, nil
	case 2:
		return nil, NotPending, nil
	case 3:
		return nil, TimedOut, nil
	case 4:
		return nil, SelfApproval, nil
	}
	return nil, NotFound, nil
}

// Unapprove puts an approved request back to pending, for the approver whose
// attempt to queue it failed, so the approval can be retried.
func (s *Store) Unapprove(ctx context.Context, id, approver string) error {
	return redis.NewScript(unapproveLua).Run(ctx, s.rdb, []string{s.prefix + id, s.pendingKey()}, approver, id).Err()
}

// Reject declines a pending request.
func (s *Store) Reject(ctx context.Context, id, by, reason string) (Outcome, error) {
	n, err := redis.NewScript(rejectLua).Run(ctx, s.rdb, []string{s.prefix + id, s.pendingKey()},
		by, reason, s.now().UnixMilli(), id).Int()
	if err != nil {
		return 0, err
	}
	return [...]Outcome{NotFound, Done, NotPending, TimedOut}[n], nil
}

// StoredCommand returns the command held in a record (for re-queuing an
// approval whose message may not have reached Kafka).
func (s *Store) StoredCommand(ctx context.Context, id string) (*command.Command, error) {
	raw, err := s.rdb.HGet(ctx, s.prefix+id, "cmd").Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var c command.Command
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Pending is a request waiting for approval, with the command it would run.
type Pending struct {
	Record
	Command *command.Command
}

// ListPending returns the requests still waiting, oldest expiry first.
func (s *Store) ListPending(ctx context.Context, limit int) ([]Pending, error) {
	now := s.now().UnixMilli()
	// Expired entries are dropped from the index (their records keep saying "expired").
	if err := s.rdb.ZRemRangeByScore(ctx, s.pendingKey(), "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	ids, err := s.rdb.ZRangeByScore(ctx, s.pendingKey(), &redis.ZRangeBy{Min: "(" + strconv.FormatInt(now, 10), Max: "+inf", Count: int64(limit)}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Pending, 0, len(ids))
	for _, id := range ids {
		rec, err := s.Get(ctx, id)
		if err != nil || rec.State != PendingApproval {
			continue // decided since, or aged out of the store
		}
		c, err := s.StoredCommand(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, Pending{Record: rec, Command: c})
	}
	return out, nil
}

// PendingStats counts the requests still waiting and tells how long the oldest has waited.
func (s *Store) PendingStats(ctx context.Context) (count int64, oldest time.Duration, err error) {
	now := s.now()
	min := "(" + strconv.FormatInt(now.UnixMilli(), 10)
	if count, err = s.rdb.ZCount(ctx, s.pendingKey(), min, "+inf").Result(); err != nil || count == 0 {
		return count, 0, err
	}
	ids, err := s.rdb.ZRangeByScore(ctx, s.pendingKey(), &redis.ZRangeBy{Min: min, Max: "+inf", Count: 1}).Result()
	if err != nil || len(ids) == 0 {
		return count, 0, err
	}
	at, err := s.rdb.HGet(ctx, s.prefix+ids[0], "at").Int64()
	if err != nil {
		return count, 0, nil
	}
	return count, now.Sub(time.UnixMilli(at)), nil
}

// WithClock sets the time source (for tests that move time) and returns the store.
func (s *Store) WithClock(now func() time.Time) *Store {
	s.now = now
	return s
}
