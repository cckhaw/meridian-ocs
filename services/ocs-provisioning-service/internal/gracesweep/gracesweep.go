// Package gracesweep moves subscribers from Grace to Suspended when their grace period ends.
//
// Provisioning records the end of a grace period (the deadline given with the change to Grace) in
// an index in Redis that is kept in step with the profile by the same atomic script. This sweeper
// asks the index which subscribers are overdue and applies, per subscriber, an "expire grace" step
// that suspends only if the subscriber is still in the very grace period the sweeper saw: a top-up
// that restored the subscriber, or a longer grace given in the meantime, always wins. Charging
// refuses new quota to a Suspended subscriber the moment the step is applied.
//
// Any number of provisioning instances may run a sweeper. Each step carries a token derived from
// the subscriber and the deadline, so two sweepers (or one sweeper after a crash) cannot suspend
// twice, and a lifecycle.changed event is published at least once: the index entry is removed only
// after the event was published, so a failed publish is retried on the next pass.
package gracesweep

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/events"
)

// By is the principal recorded on the events this sweeper publishes.
const By = "system:grace-expiry"

// Data is the part of the data layer the sweeper uses.
type Data interface {
	DueGrace(ctx context.Context, now time.Time, limit int) ([]datalayer.GraceDue, int64, error)
	Provision(ctx context.Context, op datalayer.ProvisionOp) (datalayer.ProvisionResult, error)
	ClearGraceDue(ctx context.Context, key string, until time.Time) error
}

// Config configures a Sweeper.
type Config struct {
	Data     Data
	Prod     bus.Producer
	Topic    string        // events topic
	Interval time.Duration // between passes
	Batch    int           // subscribers per round, default 200
	Now      func() time.Time
	Logf     func(format string, args ...any)
}

// Stats are counters since start.
type Stats struct {
	Suspended uint64 // grace ended and the subscriber was suspended
	Stale     uint64 // overdue entries whose subscriber had recovered, been extended or gone
	Errors    uint64 // passes or subscribers that failed on Redis or Kafka and will be retried
	Overdue   int64  // subscribers past their deadline after the last pass
	OldestAge time.Duration
}

// Sweeper applies the end of grace periods.
type Sweeper struct {
	cfg                      Config
	suspended, stale, errors atomic.Uint64
	overdue, oldestMs        atomic.Int64
}

// New returns a Sweeper.
func New(cfg Config) *Sweeper {
	if cfg.Batch <= 0 {
		cfg.Batch = 200
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Sweeper{cfg: cfg}
}

// Stats returns the counters.
func (s *Sweeper) Stats() Stats {
	return Stats{Suspended: s.suspended.Load(), Stale: s.stale.Load(), Errors: s.errors.Load(),
		Overdue: s.overdue.Load(), OldestAge: time.Duration(s.oldestMs.Load()) * time.Millisecond}
}

// Run sweeps every Interval until ctx ends.
func (s *Sweeper) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := s.Once(ctx); err != nil && ctx.Err() == nil {
			s.errors.Add(1)
			s.cfg.Logf("grace sweep: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

const maxRounds = 50

// Once makes one pass and returns how many subscribers it suspended. It stops at the first
// infrastructure error (the rest is retried on the next pass).
func (s *Sweeper) Once(ctx context.Context) (int, error) {
	done := 0
	// The backlog is measured on every way out, so that a broken dependency shows up as overdue
	// subscribers rather than as a gauge frozen at its last good value.
	defer func() {
		if n, oldest, err := s.overdueNow(ctx); err == nil {
			s.overdue.Store(n)
			s.oldestMs.Store(oldest.Milliseconds())
		}
	}()
	for round := 0; round < maxRounds; round++ {
		now := s.cfg.Now()
		due, _, err := s.cfg.Data.DueGrace(ctx, now, s.cfg.Batch)
		if err != nil {
			return done, fmt.Errorf("read the grace index: %w", err)
		}
		cleared := 0
		for _, d := range due {
			ok, suspended, err := s.expire(ctx, d, now)
			if err != nil {
				s.errors.Add(1)
				s.cfg.Logf("grace sweep: subscriber %s: %v", d.Key, err)
				return done, nil // keep the rest for the next pass; do not hammer a broken dependency
			}
			if ok {
				cleared++
			}
			if suspended {
				done++
			}
		}
		if len(due) < s.cfg.Batch || cleared == 0 {
			break
		}
	}
	return done, nil
}

func (s *Sweeper) overdueNow(ctx context.Context) (int64, time.Duration, error) {
	list, total, err := s.cfg.Data.DueGrace(ctx, s.cfg.Now(), 1)
	if err != nil || len(list) == 0 {
		return total, 0, err
	}
	return total, s.cfg.Now().Sub(list[0].Until), nil
}

// Token identifies the end of one grace period of one subscriber.
func Token(key string, until time.Time) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("gx-%s-%d", hex.EncodeToString(h[:16]), until.UnixMilli())
}

// expire handles one overdue subscriber. handled reports that the entry is finished with (and was
// removed from the index); suspended that this call (or an earlier call with the same token) put the
// subscriber into Suspended.
func (s *Sweeper) expire(ctx context.Context, d datalayer.GraceDue, now time.Time) (handled, suspended bool, err error) {
	token := Token(d.Key, d.Until)
	res, err := s.cfg.Data.Provision(ctx, datalayer.ProvisionOp{Op: datalayer.ProvExpireGrace, Key: d.Key, Token: token,
		Fingerprint: fmt.Sprintf("expire-grace|%s|%d", d.Key, d.Until.UnixMilli()), Now: now, Until: d.Until})
	if err != nil {
		return false, false, err
	}
	if res.Code == datalayer.ProvApplied {
		s.suspended.Add(1) // counted where it happened, not on a replay after a failed publish
	}
	switch res.Effective() {
	case datalayer.ProvApplied:
		ev := events.Event{V: events.EventVersion, Type: events.EventLifecycle, CommandID: token, Key: d.Key, At: now.UnixMilli(), By: By,
			Operation: "expire_grace", Version: res.Version, From: "grace", To: "suspended", Reason: "grace period ended"}
		raw, err := ev.Encode()
		if err != nil {
			return false, false, err
		}
		if err := s.cfg.Prod.Produce(ctx, s.cfg.Topic, d.Key, raw, map[string]string{"command-id": token}); err != nil {
			return false, false, fmt.Errorf("publish lifecycle event: %w", err)
		}
		if err := s.cfg.Data.ClearGraceDue(ctx, d.Key, d.Until); err != nil {
			return false, false, err
		}
		return true, true, nil
	case datalayer.ProvNoSubscriber:
		if err := s.cfg.Data.ClearGraceDue(ctx, d.Key, d.Until); err != nil {
			return false, false, err
		}
		s.stale.Add(1)
		return true, false, nil
	case datalayer.ProvInvalidTransition, datalayer.ProvGraceChanged:
		// The script already corrected the index entry.
		s.stale.Add(1)
		return true, false, nil
	case datalayer.ProvGraceNotOver:
		// Only if this host's clock is behind the one that wrote the deadline's reference; try again later.
		return false, false, nil
	default:
		return false, false, fmt.Errorf("unexpected outcome %s", res.Effective())
	}
}
