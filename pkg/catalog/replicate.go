package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Replicator keeps every rating node's Store in step through Redis, the
// system of record for published catalog versions: a list whose element i is
// the JSON of version i+1, plus a pub/sub channel announcing new versions.
//
// Publishing is one atomic script (append only if the list length is exactly
// the version before it), so two editors can never publish the same version.
// Pub/sub is only a wake-up: a node that misses a message still catches up on
// its periodic resync. A node never replaces a good snapshot with a failure;
// it keeps serving the last version it holds.
type Replicator struct {
	Redis   redis.UniversalClient
	Store   *Store
	Key     string        // default "ocs:catalog:versions"
	Channel string        // default "ocs:catalog:updates"
	Resync  time.Duration // default 30s
	// OnError receives background errors (resync, bad stored document).
	OnError func(error)

	lastOK atomic.Int64  // unix nanoseconds of the last successful Sync, 0 = never
	errs   atomic.Uint64 // background errors reported
}

func (r *Replicator) key() string {
	if r.Key == "" {
		return "ocs:catalog:versions"
	}
	return r.Key
}

func (r *Replicator) channel() string {
	if r.Channel == "" {
		return "ocs:catalog:updates"
	}
	return r.Channel
}

func (r *Replicator) report(err error) {
	if err == nil {
		return
	}
	r.errs.Add(1)
	if r.OnError != nil {
		r.OnError(err)
	}
}

var appendScript = redis.NewScript(`
if redis.call('LLEN', KEYS[1]) == tonumber(ARGV[1]) then
  redis.call('RPUSH', KEYS[1], ARGV[2])
  redis.call('PUBLISH', KEYS[2], ARGV[1] + 1)
  return 1
end
return 0`)

// Sync loads any versions this node does not have yet. On an empty store it
// loads only the newest Store-retain versions.
func (r *Replicator) Sync(ctx context.Context) error {
	err := r.sync(ctx)
	if err == nil {
		r.lastOK.Store(time.Now().UnixNano())
	}
	return err
}

func (r *Replicator) sync(ctx context.Context) error {
	n, err := r.Redis.LLen(ctx, r.key()).Result()
	if err != nil {
		return err
	}
	have := r.Store.LatestVersion()
	from := have + 1
	if have == 0 && n > int64(r.Store.retain) {
		from = n - int64(r.Store.retain) + 1
	}
	if from > n {
		return nil
	}
	docs, err := r.Redis.LRange(ctx, r.key(), from-1, n-1).Result()
	if err != nil {
		return err
	}
	for i, raw := range docs {
		var d Document
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return fmt.Errorf("catalog: stored version %d is unreadable: %w", from+int64(i), err)
		}
		d.Version = from + int64(i)
		sn, _, err := Compile(d)
		if err != nil {
			return fmt.Errorf("catalog: stored version %d does not compile: %w", d.Version, err)
		}
		if err := r.Store.Install(sn); err != nil {
			if errors.Is(err, ErrVersionConflict) { // another goroutine got there first
				continue
			}
			return err
		}
	}
	return nil
}

// Publish validates doc against the shared timeline and appends it as the next
// version cluster-wide. expect, when non-zero, is the version the editor
// believes will be next.
func (r *Replicator) Publish(ctx context.Context, doc Document, expect int64) (*Snapshot, []Problem, error) {
	if err := r.Sync(ctx); err != nil {
		return nil, nil, err
	}
	next := r.Store.LatestVersion() + 1
	if expect != 0 && expect != next {
		return nil, nil, fmt.Errorf("%w: next version is %d, caller expected %d", ErrVersionConflict, next, expect)
	}
	doc.Version = next
	sn, warns, err := Compile(doc)
	if err != nil {
		return nil, warns, err
	}
	// Check ordering against the local copy of the timeline before spending a
	// round trip; Install repeats it atomically for the local store.
	if last := r.Store.Latest(); last != nil && !sn.EffectiveFrom.After(last.EffectiveFrom) {
		return nil, warns, fmt.Errorf("%w (%s <= %s)", ErrNotMonotonic,
			sn.EffectiveFrom.Format(time.RFC3339), last.EffectiveFrom.Format(time.RFC3339))
	}
	raw, err := json.Marshal(sn.Document())
	if err != nil {
		return nil, warns, err
	}
	ok, err := appendScript.Run(ctx, r.Redis, []string{r.key(), r.channel()}, next-1, raw).Int()
	if err != nil {
		return nil, warns, err
	}
	if ok != 1 {
		_ = r.Sync(ctx)
		return nil, warns, fmt.Errorf("%w: another editor published version %d first", ErrVersionConflict, next)
	}
	if err := r.Store.Install(sn); err != nil && !errors.Is(err, ErrVersionConflict) {
		return sn, warns, err
	}
	return sn, warns, nil
}

// Run syncs once, then follows the update channel and resyncs periodically
// until ctx ends. Errors, including the first sync's, go to OnError and the
// loop keeps trying: a node that must not start without a catalog calls Sync
// itself first and decides.
func (r *Replicator) Run(ctx context.Context) error {
	r.report(r.Sync(ctx))
	every := r.Resync
	if every <= 0 {
		every = 30 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for ctx.Err() == nil {
		sub := r.Redis.Subscribe(ctx, r.channel())
		if _, err := sub.Receive(ctx); err != nil {
			sub.Close()
			if ctx.Err() != nil {
				return nil
			}
			r.report(fmt.Errorf("catalog: subscribe: %w", err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		// A message may have been missed between Sync and Subscribe.
		r.report(r.Sync(ctx))
		ch := sub.Channel()
	recv:
		for {
			select {
			case <-ctx.Done():
				sub.Close()
				return nil
			case _, ok := <-ch:
				if !ok {
					break recv
				}
				r.report(r.Sync(ctx))
			case <-tick.C:
				r.report(r.Sync(ctx))
			}
		}
		sub.Close()
	}
	return nil
}
