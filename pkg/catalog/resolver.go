package catalog

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
)

// ErrUnknownOffering: the subscriber's offering is not in the catalog version
// in effect. Usage is not priced on a guess; the operator has to fix the
// catalog (offerings must outlive their subscribers) or move the subscriber.
var ErrUnknownOffering = errors.New("catalog: the subscriber's offering is not in the catalog")

// OfferingSource reads the offering id from a subscriber's profile
// (*datalayer.Client). It returns datalayer.ErrNoSubscriber for no profile and
// "" for a profile without an offering.
type OfferingSource interface {
	Offering(ctx context.Context, subscriberKey string) (string, error)
}

// ProfileResolver maps subscriber -> offering (from the profile, cached) ->
// plan (from the catalog version in effect at the time asked), so a catalog
// change to an offering's plan applies at once, while a change of the
// subscriber's own offering is seen after at most TTL.
//
// A lookup failure is an error, never the default plan: pricing a subscriber
// on someone else's tariff because Redis blinked would be wrong money.
type ProfileResolver struct {
	Source  OfferingSource
	Store   *Store
	TTL     time.Duration // cache lifetime of a profile lookup (default 30s)
	Timeout time.Duration // bound on one lookup (default 100ms)
	MaxSize int           // entries kept (default 1,000,000)

	shards [resolverShards]resolverShard
	now    func() time.Time
}

const resolverShards = 64

type resolverEntry struct {
	offering string
	expires  int64 // unix nano
}

type resolverShard struct {
	mu sync.Mutex
	m  map[string]resolverEntry
}

// NewProfileResolver builds a resolver.
func NewProfileResolver(src OfferingSource, store *Store) *ProfileResolver {
	return &ProfileResolver{Source: src, Store: store, now: time.Now}
}

func (r *ProfileResolver) ttl() time.Duration {
	if r.TTL > 0 {
		return r.TTL
	}
	return 30 * time.Second
}

// PlanFor implements PlanResolver.
func (r *ProfileResolver) PlanFor(subscriber string, at time.Time) (string, error) {
	off, err := r.offering(subscriber)
	if err != nil {
		return "", fmt.Errorf("looking up the subscriber's offering: %w", err)
	}
	if off == "" {
		return "", ErrNoPlan
	}
	sn := r.Store.At(at)
	if sn == nil {
		return "", ErrNoVersion
	}
	o, ok := sn.Offering(off)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownOffering, off)
	}
	return o.PlanID, nil
}

func (r *ProfileResolver) offering(subscriber string) (string, error) {
	h := fnv.New32a()
	h.Write([]byte(subscriber))
	sh := &r.shards[h.Sum32()%resolverShards]
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	t := now().UnixNano()

	sh.mu.Lock()
	if e, ok := sh.m[subscriber]; ok && e.expires > t {
		sh.mu.Unlock()
		return e.offering, nil
	}
	sh.mu.Unlock()

	to := r.Timeout
	if to <= 0 {
		to = 100 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	off, err := r.Source.Offering(ctx, subscriber)
	if errors.Is(err, datalayer.ErrNoSubscriber) {
		off, err = "", nil // no profile: served on the default plan, like the charge script treats it
	}
	if err != nil {
		return "", err
	}

	max := r.MaxSize
	if max <= 0 {
		max = 1_000_000
	}
	sh.mu.Lock()
	if sh.m == nil {
		sh.m = make(map[string]resolverEntry)
	}
	if len(sh.m) >= max/resolverShards {
		for k, e := range sh.m { // drop what has expired; if nothing has, drop everything
			if e.expires <= t {
				delete(sh.m, k)
			}
		}
		if len(sh.m) >= max/resolverShards {
			sh.m = make(map[string]resolverEntry)
		}
	}
	sh.m[subscriber] = resolverEntry{off, t + int64(r.ttl())}
	sh.mu.Unlock()
	return off, nil
}

// Forget drops a subscriber from the cache (tests, and push invalidation if a
// future provisioning event feed calls it).
func (r *ProfileResolver) Forget(subscriber string) {
	h := fnv.New32a()
	h.Write([]byte(subscriber))
	sh := &r.shards[h.Sum32()%resolverShards]
	sh.mu.Lock()
	delete(sh.m, subscriber)
	sh.mu.Unlock()
}
