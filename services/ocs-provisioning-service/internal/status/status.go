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
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

// State of a command.
type State string

const (
	Accepted State = "accepted"
	Applied  State = "applied"
	Failed   State = "failed"
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
}

// Final reports whether the command has finished.
func (r Record) Final() bool { return r.State == Applied || r.State == Failed }

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
	return Record{ID: id, Type: command.Type(m["type"]), Key: m["key"], State: State(m["state"]),
		Outcome: m["outcome"], Message: m["msg"], Balance: num("bal"), OldBalance: num("old"),
		From: m["from"], To: m["to"], Version: num("ver"), Fingerprint: m["fp"],
		AcceptedAt: ms("at"), CompletedAt: ms("done")}, nil
}
