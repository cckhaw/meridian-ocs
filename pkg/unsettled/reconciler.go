package unsettled

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

// Settler submits a terminate to the data tier and reports what became of it. The engine implements it: an
// error means "not now" (the data tier is down, the breaker is open, the engine is busy) and the record stays
// pending; a canonical.Result of ResultUnknownSession or ResultStaleRequest means the session cannot take the
// usage any more; anything else means the usage was charged (or had been already: replays are idempotent).
type Settler interface {
	Settle(ctx context.Context, req *canonical.Request) (canonical.Result, error)
}

// Reconciler replays the spool's records against a Settler until each is settled.
type Reconciler struct {
	Spool    *Spool
	Settler  Settler
	Interval time.Duration // between passes while there is work (default 2 s)
	Backoff  time.Duration // after a pass that could not settle everything (default 5 s)
	Batch    int           // records per pass (default 200)
	Logf     func(format string, args ...any)

	settled, dead, deferred atomic.Uint64
	failing                 atomic.Bool
}

// ReconcilerStats are cumulative counters.
type ReconcilerStats struct {
	Settled  uint64 // records charged and removed
	Dead     uint64 // records that could never be charged (see Spool.DeadLetters)
	Deferred uint64 // passes that stopped because the data tier was not available
}

// Stats returns the counters.
func (r *Reconciler) Stats() ReconcilerStats {
	return ReconcilerStats{r.settled.Load(), r.dead.Load(), r.deferred.Load()}
}

func (r *Reconciler) logf(f string, a ...any) {
	if r.Logf != nil {
		r.Logf(f, a...)
	}
}

// Pass settles as many pending records as it can, oldest first, and stops at the first one the data tier
// cannot take now (preserving order and not hammering a struggling store). It returns how many it settled.
func (r *Reconciler) Pass(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = 200
	}
	n := 0
	for _, rec := range r.Spool.Pending(batch) {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		req := rec.Request()
		res, err := r.Settler.Settle(ctx, req)
		canonical.ReleaseRequest(req)
		if err != nil {
			r.deferred.Add(1)
			return n, err
		}
		switch res {
		case canonical.ResultUnknownSession, canonical.ResultStaleRequest:
			why := "session unknown: its state expired before the usage could be charged"
			if res == canonical.ResultStaleRequest {
				why = "stale: a later request of the session was already processed"
			}
			if err := r.Spool.Dead(rec.ID, why); err != nil {
				return n, err
			}
			r.dead.Add(1)
			r.logf("unsettled usage for %s session %s request %d can never be charged: %s", rec.SubscriberID, rec.SessionID, rec.RequestNumber, why)
		default:
			if err := r.Spool.Done(rec.ID); err != nil {
				return n, err
			}
			r.settled.Add(1)
			n++
		}
	}
	return n, nil
}

// Run reconciles until ctx is done.
func (r *Reconciler) Run(ctx context.Context) {
	interval, backoff := r.Interval, r.Backoff
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	wait := interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if r.Spool.Stats().Pending == 0 {
			wait = interval
			continue
		}
		n, err := r.Pass(ctx)
		switch {
		case err == nil:
			if r.failing.Swap(false) {
				r.logf("unsettled usage: reconciliation caught up (%d settled in this pass)", n)
			}
			wait = interval
		case errors.Is(err, context.Canceled):
			return
		default:
			if !r.failing.Swap(true) {
				r.logf("unsettled usage: cannot settle now (%v); %d record(s) stay pending", err, r.Spool.Stats().Pending)
			}
			wait = backoff
		}
	}
}
