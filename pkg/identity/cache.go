package identity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CacheConfig tunes CachedDirectory.
type CacheConfig struct {
	TTL         time.Duration // positive entries, default 30s
	NegativeTTL time.Duration // "unknown" entries, default 2s
	MaxEntries  int           // total, default 1<<20
	Now         func() time.Time
}

const cacheShards = 64

type entry struct {
	key     string
	unknown bool
	expires int64 // unix nanos
	done    chan struct{}
	err     error // lookup failure, only meaningful to waiters of an in-flight call
}

type cacheShard struct {
	mu sync.Mutex
	m  map[Identity]*entry
}

// CachedDirectory is a bounded read-through cache in front of a Directory.
//
//   - Concurrent misses for one identity share a single lookup.
//   - Unknown identities are cached briefly so a flood of bad identifiers
//     cannot turn into a flood of directory lookups; the cache is bounded so
//     the flood cannot exhaust memory either.
//   - Directory failures are never cached.
//
// A binding changed in the directory (SIM swap, number reassignment) is seen
// here after at most TTL; until then the old mapping is used.
type CachedDirectory struct {
	inner  Directory
	cfg    CacheConfig
	perMax int
	shards [cacheShards]cacheShard

	hits, misses, shared atomic.Uint64
}

// NewCachedDirectory wraps inner.
func NewCachedDirectory(inner Directory, cfg CacheConfig) *CachedDirectory {
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Second
	}
	if cfg.NegativeTTL <= 0 {
		cfg.NegativeTTL = 2 * time.Second
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 1 << 20
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	c := &CachedDirectory{inner: inner, cfg: cfg, perMax: (cfg.MaxEntries + cacheShards - 1) / cacheShards}
	for i := range c.shards {
		c.shards[i].m = make(map[Identity]*entry)
	}
	return c
}

// CacheStats are cumulative counters.
type CacheStats struct {
	Hits, Misses, Shared uint64
	Entries              int
}

// Stats returns a snapshot of the counters.
func (c *CachedDirectory) Stats() CacheStats {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return CacheStats{c.hits.Load(), c.misses.Load(), c.shared.Load(), n}
}

func shardOf(id Identity) uint32 {
	h := uint32(2166136261) ^ uint32(id.Type)
	h *= 16777619
	for i := 0; i < len(id.Value); i++ {
		h ^= uint32(id.Value[i])
		h *= 16777619
	}
	return h % cacheShards
}

// Lookup implements Directory.
func (c *CachedDirectory) Lookup(ctx context.Context, id Identity) (string, error) {
	sh := &c.shards[shardOf(id)]
	now := c.cfg.Now().UnixNano()

	sh.mu.Lock()
	if e, ok := sh.m[id]; ok {
		if e.done == nil && e.expires > now { // settled and fresh
			key, unknown := e.key, e.unknown
			sh.mu.Unlock()
			c.hits.Add(1)
			if unknown {
				return "", ErrUnknown
			}
			return key, nil
		}
		if e.done != nil { // someone else is already asking
			done := e.done
			sh.mu.Unlock()
			c.shared.Add(1)
			select {
			case <-done:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			if e.err != nil {
				return "", e.err
			}
			if e.unknown {
				return "", ErrUnknown
			}
			return e.key, nil
		}
		// expired: fall through and refresh
	}
	if len(sh.m) >= c.perMax {
		c.evict(sh, now)
	}
	e := &entry{done: make(chan struct{})}
	sh.m[id] = e
	sh.mu.Unlock()

	c.misses.Add(1)
	key, err := c.inner.Lookup(context.WithoutCancel(ctx), id)

	sh.mu.Lock()
	switch {
	case err == nil:
		e.key, e.expires = key, c.cfg.Now().Add(c.cfg.TTL).UnixNano()
	case isUnknown(err):
		e.unknown, e.expires = true, c.cfg.Now().Add(c.cfg.NegativeTTL).UnixNano()
	default:
		e.err = err
		delete(sh.m, id) // failures are never cached
	}
	done := e.done
	e.done = nil
	sh.mu.Unlock()
	close(done)

	switch {
	case err == nil:
		return key, nil
	case isUnknown(err):
		return "", ErrUnknown
	}
	return "", err
}

func isUnknown(err error) bool { return errors.Is(err, ErrUnknown) }

// evict makes room in a full shard: expired entries first, then arbitrary
// settled ones. In-flight entries are never removed.
func (c *CachedDirectory) evict(sh *cacheShard, now int64) {
	for k, e := range sh.m {
		if e.done == nil && e.expires <= now {
			delete(sh.m, k)
		}
	}
	for k, e := range sh.m {
		if len(sh.m) < c.perMax {
			return
		}
		if e.done == nil {
			delete(sh.m, k)
		}
	}
}
