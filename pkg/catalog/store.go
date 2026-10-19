package catalog

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrVersionConflict: the version being published is not the next one.
	ErrVersionConflict = errors.New("catalog: version conflict")
	// ErrNotMonotonic: the effective time does not follow the latest version's.
	ErrNotMonotonic = errors.New("catalog: effectiveFrom must be later than the latest version's")
	// ErrNoVersion: no version applies at the requested time.
	ErrNoVersion = errors.New("catalog: no catalog version in effect")
)

// DefaultRetain is how many past versions a Store keeps for time travel.
const DefaultRetain = 64

// timeline is an immutable list of snapshots ordered by EffectiveFrom (and by
// Version, since both only grow).
type timeline []*Snapshot

// Store holds the catalog timeline. Reads (At, Current) are one atomic load and
// a binary search; publishing builds a new timeline and swaps it in, so a
// rating node never waits for, or restarts because of, a publish.
type Store struct {
	tl     atomic.Pointer[timeline]
	mu     sync.Mutex // serialises writers only
	retain int
	now    func() time.Time
}

// NewStore creates an empty store. retain <= 0 uses DefaultRetain.
func NewStore(retain int) *Store {
	if retain <= 0 {
		retain = DefaultRetain
	}
	s := &Store{retain: retain, now: time.Now}
	empty := timeline{}
	s.tl.Store(&empty)
	return s
}

// Latest returns the newest version (which may be future-dated), or nil.
func (s *Store) Latest() *Snapshot {
	tl := *s.tl.Load()
	if len(tl) == 0 {
		return nil
	}
	return tl[len(tl)-1]
}

// LatestVersion is the newest version number, 0 when empty.
func (s *Store) LatestVersion() int64 {
	if l := s.Latest(); l != nil {
		return l.Version
	}
	return 0
}

// At returns the snapshot in effect at t: the newest whose EffectiveFrom is
// not after t. It returns nil when t precedes every retained version.
func (s *Store) At(t time.Time) *Snapshot {
	tl := *s.tl.Load()
	i := sort.Search(len(tl), func(i int) bool { return tl[i].EffectiveFrom.After(t) })
	if i == 0 {
		return nil
	}
	return tl[i-1]
}

// Current is At(now).
func (s *Store) Current() *Snapshot { return s.At(s.now()) }

// Version returns a retained version by number.
func (s *Store) Version(v int64) (*Snapshot, bool) {
	for _, sn := range *s.tl.Load() {
		if sn.Version == v {
			return sn, true
		}
	}
	return nil, false
}

// Versions lists the retained versions, oldest first.
func (s *Store) Versions() []*Snapshot { return append([]*Snapshot(nil), *s.tl.Load()...) }

// Install appends an already compiled snapshot. Its Version must be the
// latest + 1 and its EffectiveFrom later than the latest's. It is what Publish
// and the replication loader use.
func (s *Store) Install(sn *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := *s.tl.Load()
	var last *Snapshot
	var lastV int64
	if n := len(old); n > 0 {
		last = old[n-1]
		lastV = last.Version
	}
	// Version first: an editor working from a stale view needs to hear that,
	// not a date complaint about a version it has never seen.
	// An empty store accepts any starting version: a node that joins late loads
	// only the most recent versions, not the whole history.
	if (last != nil || sn.Version < 1) && sn.Version != lastV+1 {
		return fmt.Errorf("%w: have %d, got %d", ErrVersionConflict, lastV, sn.Version)
	}
	if last != nil && !sn.EffectiveFrom.After(last.EffectiveFrom) {
		return fmt.Errorf("%w (%s <= %s)", ErrNotMonotonic,
			sn.EffectiveFrom.Format(time.RFC3339), last.EffectiveFrom.Format(time.RFC3339))
	}
	next := append(append(make(timeline, 0, len(old)+1), old...), sn)
	// Trim history, but never the version in effect now or anything newer.
	if drop := len(next) - s.retain; drop > 0 {
		inEffect := sort.Search(len(next), func(i int) bool { return next[i].EffectiveFrom.After(s.now()) }) - 1
		if drop > inEffect {
			drop = inEffect
		}
		if drop > 0 {
			next = append(timeline(nil), next[drop:]...)
		}
	}
	s.tl.Store(&next)
	return nil
}

// Publish validates doc and appends it as the next version. expect, when
// non-zero, must equal that next version (optimistic concurrency for editors).
func (s *Store) Publish(doc Document, expect int64) (*Snapshot, []Problem, error) {
	next := s.LatestVersion() + 1
	if expect != 0 && expect != next {
		return nil, nil, fmt.Errorf("%w: next version is %d, caller expected %d", ErrVersionConflict, next, expect)
	}
	doc.Version = next
	sn, warns, err := Compile(doc)
	if err != nil {
		return nil, warns, err
	}
	if err := s.Install(sn); err != nil {
		return nil, warns, err
	}
	return sn, warns, nil
}
