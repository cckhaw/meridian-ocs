package datalayer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Replication reports whether a write was acknowledged by enough replicas
// before the call returned.
type Replication uint8

const (
	// ReplicationNotRequested: the step is not a commit (or waiting is off).
	ReplicationNotRequested Replication = iota
	// ReplicationAcked: at least the configured number of replicas had the
	// write when the call returned.
	ReplicationAcked
	// ReplicationShort: fewer replicas acknowledged before the timeout, or
	// waiting is suspended. The write is applied on the primary but could be
	// lost if the primary fails before its replicas catch up.
	ReplicationShort
)

// Durability configures how many replicas must acknowledge a committed write.
type Durability struct {
	// Replicas is the number of replicas that must acknowledge (0 = don't wait).
	Replicas int
	// Timeout bounds the wait. It is added to the commit latency in the worst
	// case, so keep it small (default 25ms).
	Timeout time.Duration
	// SuspendAfter consecutive shortfalls stop waiting for SuspendFor, so a
	// dead replica does not add Timeout to every commit. Waiting resumes with
	// a probe afterwards. Defaults: 5 and 5s.
	SuspendAfter int
	SuspendFor   time.Duration
}

// DurabilityStats are cumulative counters.
type DurabilityStats struct {
	Waits      uint64 // commits that waited for replicas
	Short      uint64 // commits that finished with fewer acks than required
	WaitErrors uint64 // WAIT itself failed
	Suspended  uint64 // commits that skipped the wait while suspended
}

type durabilityState struct {
	cfg         Durability
	now         func() time.Time
	consecShort atomic.Int32
	suspendEnd  atomic.Int64 // unix nanos; 0 = not suspended

	waits, short, waitErrs, suspended atomic.Uint64
}

// ErrClusterWait: WAIT cannot be pinned to the primary that owns a slot
// through go-redis's cluster client, so replica acknowledgement is only
// supported for standalone and sentinel clients.
var ErrClusterWait = errors.New("datalayer: replica acknowledgement is not supported with a Redis Cluster client")

// WithDurability turns on replica acknowledgement for committing steps
// (updates, terminates, events and provisioning writes). Call it before the
// client is used.
func (c *Client) WithDurability(d Durability) (*Client, error) {
	if d.Replicas < 0 {
		return nil, errors.New("datalayer: Replicas must not be negative")
	}
	if _, ok := c.rdb.(*redis.ClusterClient); ok && d.Replicas > 0 {
		return nil, ErrClusterWait
	}
	if d.Timeout <= 0 {
		d.Timeout = 25 * time.Millisecond
	}
	if d.SuspendAfter <= 0 {
		d.SuspendAfter = 5
	}
	if d.SuspendFor <= 0 {
		d.SuspendFor = 5 * time.Second
	}
	c.dur = &durabilityState{cfg: d, now: time.Now}
	return c, nil
}

// DurabilityStats returns a snapshot of the replication counters.
func (c *Client) DurabilityStats() DurabilityStats {
	if c.dur == nil {
		return DurabilityStats{}
	}
	return DurabilityStats{c.dur.waits.Load(), c.dur.short.Load(), c.dur.waitErrs.Load(), c.dur.suspended.Load()}
}

func (d *durabilityState) active() bool { return d != nil && d.cfg.Replicas > 0 }

// run executes a script. With commit set and durability on, the script and a
// WAIT are sent in one pipeline so they share a connection: WAIT only covers
// the writes made on its own connection.
func (c *Client) run(ctx context.Context, s *redis.Script, keys []string, args []any, commit bool) (any, Replication, error) {
	d := c.dur
	if !commit || !d.active() {
		v, err := s.Run(ctx, c.rdb, keys, args...).Result()
		return v, ReplicationNotRequested, err
	}
	if end := d.suspendEnd.Load(); end != 0 {
		if d.now().UnixNano() < end {
			d.suspended.Add(1)
			v, err := s.Run(ctx, c.rdb, keys, args...).Result()
			return v, ReplicationShort, err
		}
		d.suspendEnd.CompareAndSwap(end, 0) // probe: wait again
	}

	exec := func(eval func(redis.Scripter) *redis.Cmd) (*redis.Cmd, *redis.Cmd) {
		pipe := c.rdb.Pipeline()
		cmd := eval(pipe)
		w := pipe.Do(ctx, "WAIT", d.cfg.Replicas, d.cfg.Timeout.Milliseconds())
		_, _ = pipe.Exec(ctx) // errors are read from the individual commands
		return cmd, w
	}
	cmd, w := exec(func(p redis.Scripter) *redis.Cmd { return s.EvalSha(ctx, p, keys, args...) })
	if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
		cmd, w = exec(func(p redis.Scripter) *redis.Cmd { return s.Eval(ctx, p, keys, args...) })
	}
	if err := cmd.Err(); err != nil {
		return nil, ReplicationNotRequested, err
	}

	d.waits.Add(1)
	n, err := w.Int()
	switch {
	case err != nil:
		d.waitErrs.Add(1)
		d.noteShort()
		return cmd.Val(), ReplicationShort, nil
	case n < d.cfg.Replicas:
		d.noteShort()
		return cmd.Val(), ReplicationShort, nil
	}
	d.consecShort.Store(0)
	return cmd.Val(), ReplicationAcked, nil
}

func (d *durabilityState) noteShort() {
	d.short.Add(1)
	if int(d.consecShort.Add(1)) >= d.cfg.SuspendAfter {
		d.consecShort.Store(0)
		d.suspendEnd.Store(d.now().Add(d.cfg.SuspendFor).UnixNano())
	}
}

// ConnectedReplicas returns how many replicas the primary reports as
// connected (INFO replication).
func (c *Client) ConnectedReplicas(ctx context.Context) (int, error) {
	info, err := c.rdb.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "connected_slaves:"); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return 0, fmt.Errorf("datalayer: bad INFO replication: %w", err)
			}
			return n, nil
		}
	}
	return 0, errors.New("datalayer: INFO replication has no connected_slaves")
}
