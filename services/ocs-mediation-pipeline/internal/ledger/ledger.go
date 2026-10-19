// Package ledger records which input has already been turned into committed
// output, so that re-reading input after a crash, a rebalance or a re-sent
// file never produces output twice. It is the authority on "done"; Kafka
// offsets and file moves are only hints that let the pipeline skip work.
package ledger

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Kind of input unit.
type Kind uint8

const (
	// Offsets is a contiguous-or-gapped range of one Kafka partition.
	Offsets Kind = iota + 1
	// File is a whole input file.
	File
)

// Unit is the smallest piece of input that is committed atomically with its
// output.
type Unit struct {
	Kind Kind
	// Stream is "topic/partition" for Offsets.
	Stream string
	// Name is deterministic from the input (so a replay of the same input names
	// its output the same) and safe as a file name.
	Name string
	// First and Last are the first and last offsets of an Offsets unit.
	First, Last int64
	// ID identifies a File unit: its name plus a content hash.
	ID string
}

// Ledger is stored in Redis.
type Ledger struct {
	rdb    redis.UniversalClient
	prefix string

	// Replicas, when > 0, makes Commit wait (Redis WAIT) until that many
	// replicas hold the commit, so a primary failure right after cannot lose
	// the record that the output exists. A shortfall is counted, not an error:
	// the output is already staged and refusing to publish it would not help.
	Replicas int
	Timeout  time.Duration
	short    atomic.Uint64
}

// Unreplicated counts commits that returned before enough replicas had them.
func (l *Ledger) Unreplicated() uint64 { return l.short.Load() }

func (l *Ledger) wait(ctx context.Context) {
	if l.Replicas <= 0 {
		return
	}
	t := l.Timeout
	if t <= 0 {
		t = 50 * time.Millisecond
	}
	n, err := l.rdb.Do(ctx, "WAIT", l.Replicas, t.Milliseconds()).Int()
	if err != nil || n < l.Replicas {
		l.short.Add(1)
	}
}

// New creates a Ledger. prefix defaults to "med:ledger:".
func New(rdb redis.UniversalClient, prefix string) *Ledger {
	if prefix == "" {
		prefix = "med:ledger:"
	}
	return &Ledger{rdb: rdb, prefix: prefix}
}

func (l *Ledger) offsetKey(stream string) string { return l.prefix + "wm:" + stream }
func (l *Ledger) fileKey(id string) string       { return l.prefix + "file:" + id }

// Watermark is the last offset of stream whose output is committed, or -1.
func (l *Ledger) Watermark(ctx context.Context, stream string) (int64, error) {
	s, err := l.rdb.Get(ctx, l.offsetKey(stream)).Result()
	if err == redis.Nil {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// An offsets unit commits only if it lies wholly above the watermark. A unit
// that overlaps committed output (another process got there first after a
// rebalance) must be discarded, not trimmed: its output file already contains
// the overlapping records.
var commitOffsets = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur and tonumber(cur) >= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
return 1`)

// Commit records the unit as done. It returns false when the unit was not
// recorded because it overlaps or repeats committed work; the caller must then
// discard its staged output.
func (l *Ledger) Commit(ctx context.Context, u Unit) (bool, error) {
	switch u.Kind {
	case Offsets:
		n, err := commitOffsets.Run(ctx, l.rdb, []string{l.offsetKey(u.Stream)}, u.First, u.Last).Int()
		if err == nil && n == 1 {
			l.wait(ctx)
		}
		return n == 1, err
	case File:
		ok, err := l.rdb.SetNX(ctx, l.fileKey(u.ID), "1", 0).Result()
		if err == nil && ok {
			l.wait(ctx)
		}
		return ok, err
	}
	return false, fmt.Errorf("ledger: unknown unit kind %d", u.Kind)
}

// Committed reports whether the unit's output is already committed. It is used
// when recovering staged output after a crash between commit and publish.
func (l *Ledger) Committed(ctx context.Context, u Unit) (bool, error) {
	switch u.Kind {
	case Offsets:
		wm, err := l.Watermark(ctx, u.Stream)
		return wm >= u.Last, err
	case File:
		n, err := l.rdb.Exists(ctx, l.fileKey(u.ID)).Result()
		return n == 1, err
	}
	return false, fmt.Errorf("ledger: unknown unit kind %d", u.Kind)
}

// FileDone reports whether a file was fully processed already.
func (l *Ledger) FileDone(ctx context.Context, id string) (bool, error) {
	return l.Committed(ctx, Unit{Kind: File, ID: id})
}
