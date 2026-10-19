// Package dedup detects duplicate charging records.
//
// A record's key (cdr.Record.Key) is stored in Redis with the position of the
// copy that claimed it. A later copy from a different position is a duplicate.
// A copy from the *same* position is the same record seen again after a crash
// or replay, and is not a duplicate: that is what lets a replayed batch rebuild
// exactly the output the first attempt would have produced.
//
// The store is exact. A Bloom filter in front of it was considered and left
// out: an exact check against Redis is needed anyway to record the key for the
// other nodes, so a filter cannot save the round trip it was meant to save.
package dedup

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is the duplicate window.
type Store struct {
	rdb    redis.UniversalClient
	prefix string
	ttl    time.Duration
	sha    string
}

const script = `
local v = redis.call('GET', KEYS[1])
if not v then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 1
end
if v == ARGV[1] then return 1 end
return 0`

// New creates a Store with a duplicate window of ttl (default 72 hours).
func New(ctx context.Context, rdb redis.UniversalClient, prefix string, ttl time.Duration) (*Store, error) {
	if prefix == "" {
		prefix = "med:dup:"
	}
	if ttl <= 0 {
		ttl = 72 * time.Hour
	}
	s := &Store{rdb: rdb, prefix: prefix, ttl: ttl}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load(ctx context.Context) (err error) {
	s.sha, err = redis.NewScript(script).Load(ctx, s.rdb).Result()
	return
}

// Window is the duplicate window.
func (s *Store) Window() time.Duration { return s.ttl }

// Claim checks and records keys in one pipelined round trip. positions[i] is
// where keys[i] came from. isNew[i] is true when the record is not a duplicate
// (first sighting, or a replay from the same position).
func (s *Store) Claim(ctx context.Context, keys, positions []string) ([]bool, error) {
	if len(keys) != len(positions) {
		return nil, errors.New("dedup: keys and positions differ in length")
	}
	for attempt := 0; ; attempt++ {
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.Cmd, len(keys))
		ttl := int64(s.ttl / time.Second)
		for i, k := range keys {
			cmds[i] = pipe.EvalSha(ctx, s.sha, []string{s.prefix + k}, positions[i], ttl)
		}
		_, err := pipe.Exec(ctx)
		if err != nil && attempt == 0 && anyNoScript(cmds) {
			// Redis restarted or the script cache was flushed.
			if lerr := s.load(ctx); lerr != nil {
				return nil, lerr
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		out := make([]bool, len(keys))
		for i, c := range cmds {
			n, err := c.Int()
			if err != nil {
				return nil, err
			}
			out[i] = n == 1
		}
		return out, nil
	}
}

func anyNoScript(cmds []*redis.Cmd) bool {
	for _, c := range cmds {
		if err := c.Err(); err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
			return true
		}
	}
	return false
}
