// Package worker consumes provisioning commands from Kafka and applies them.
//
// Ordering. Commands are keyed by subscriber on the producer side, so one
// subscriber's commands sit in one partition in order. Inside a polled batch
// each command is handed to a worker chosen by hashing the subscriber key, and
// a worker handles its queue strictly in order: two commands for one
// subscriber are never applied concurrently or out of order, while different
// subscribers proceed in parallel.
//
// Delivery. A batch's offsets are committed only after every command in it has
// been applied (or finally failed), its status recorded and its event
// published. A crash before the commit redelivers the batch; applying is
// idempotent (the command id is the token inside the subscriber's Redis slot),
// so redelivery is harmless, and the event is published again (at least once).
//
// Failure. Infrastructure errors (Redis, Kafka) are retried with backoff and
// hold up that partition rather than skipping a command. Business failures
// (insufficient balance, invalid transition, ...) are final outcomes, recorded
// and published. A message that cannot be decoded goes to the dead-letter
// topic.
package worker

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// Config configures a Worker.
type Config struct {
	Bus        bus.Config
	Limits     command.Limits
	Workers    int           // parallel workers, default 16
	MaxPoll    int           // records per batch, default 500
	QueueDepth int           // per worker, default 256
	MinBackoff time.Duration // default 50ms
	MaxBackoff time.Duration // default 2s
	Logf       func(format string, args ...any)
}

func (c *Config) setDefaults() {
	c.Bus = c.Bus.WithDefaults()
	if c.Workers <= 0 {
		c.Workers = 16
	}
	if c.MaxPoll <= 0 {
		c.MaxPoll = 500
	}
	if c.QueueDepth <= 0 {
		c.QueueDepth = 256
	}
	if c.MinBackoff <= 0 {
		c.MinBackoff = 50 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 2 * time.Second
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

// Stats are cumulative counters.
type Stats struct {
	Applied      uint64
	Failed       uint64 // final business failures
	DeadLettered uint64
	Retries      uint64 // infrastructure retries
	Commits      uint64 // batches whose offsets were committed
}

// Worker is the command consumer.
type Worker struct {
	cfg    Config
	cl     *kgo.Client
	apply  Applier
	status *status.Store
	prod   bus.Producer

	queues []chan job
	wg     sync.WaitGroup

	applied, failedN, dead, retries, commits atomic.Uint64
}

type job struct {
	rec  *kgo.Record
	done *sync.WaitGroup
}

// New builds a Worker around an already-connected consumer client.
func New(cfg Config, cl *kgo.Client, apply Applier, st *status.Store, prod bus.Producer) *Worker {
	cfg.setDefaults()
	return &Worker{cfg: cfg, cl: cl, apply: apply, status: st, prod: prod}
}

// Stats returns a snapshot of the counters.
func (w *Worker) Stats() Stats {
	return Stats{w.applied.Load(), w.failedN.Load(), w.dead.Load(), w.retries.Load(), w.commits.Load()}
}

func shard(key []byte, n int) int {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n))
}

type tp struct {
	topic string
	part  int32
}

// Run consumes until ctx ends. It does not close the client.
func (w *Worker) Run(ctx context.Context) error {
	w.queues = make([]chan job, w.cfg.Workers)
	for i := range w.queues {
		w.queues[i] = make(chan job, w.cfg.QueueDepth)
		w.wg.Add(1)
		go w.loop(ctx, w.queues[i])
	}
	defer func() {
		for _, q := range w.queues {
			close(q)
		}
		w.wg.Wait()
		// BlockRebalanceOnPoll holds rebalances until this is called after a
		// poll. Leaving it unset on shutdown would make Close wait forever to
		// leave the consumer group.
		w.cl.AllowRebalance()
	}()

	for ctx.Err() == nil {
		fetches := w.cl.PollRecords(ctx, w.cfg.MaxPoll)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				w.cfg.Logf("worker: fetch %s/%d: %v", t, p, err)
			}
		})
		if ctx.Err() != nil {
			break
		}

		var batch sync.WaitGroup
		last := map[tp]*kgo.Record{}
	enqueue:
		for _, part := range fetches.Records() {
			batch.Add(1) // before queueing: a fast worker may finish at once
			select {
			case w.queues[shard(part.Key, w.cfg.Workers)] <- job{part, &batch}:
				last[tp{part.Topic, part.Partition}] = part
			case <-ctx.Done():
				batch.Done()
				break enqueue
			}
		}
		batch.Wait()
		if ctx.Err() != nil {
			break // some commands may not have finished: commit nothing
		}
		if len(last) > 0 {
			recs := make([]*kgo.Record, 0, len(last))
			for _, r := range last {
				recs = append(recs, r)
			}
			if err := w.cl.CommitRecords(ctx, recs...); err != nil {
				if ctx.Err() == nil {
					// The batch will be redelivered; applying again is harmless.
					w.cfg.Logf("worker: commit: %v", err)
				}
			} else {
				w.commits.Add(1)
			}
		}
		w.cl.AllowRebalance()
	}
	return nil
}

func (w *Worker) loop(ctx context.Context, q <-chan job) {
	defer w.wg.Done()
	for j := range q {
		w.process(ctx, j.rec)
		j.done.Done()
	}
}

// retry runs fn until it succeeds or ctx ends, with exponential backoff.
func (w *Worker) retry(ctx context.Context, what string, fn func(context.Context) error) error {
	delay := w.cfg.MinBackoff
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.retries.Add(1)
		w.cfg.Logf("worker: %s: %v (retrying in %s)", what, err, delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		if delay *= 2; delay > w.cfg.MaxBackoff {
			delay = w.cfg.MaxBackoff
		}
	}
}

// process handles one record. It returns only when the record is finished or
// ctx ended; an unfinished record is simply not committed.
func (w *Worker) process(ctx context.Context, rec *kgo.Record) {
	c, err := command.Decode(rec.Value)
	if err != nil {
		w.deadLetter(ctx, rec, "undecodable: "+err.Error(), rec.Value)
		return
	}
	if string(rec.Key) != c.Key {
		// The partition key is what guarantees ordering; a mismatch means the
		// producer is broken, and applying could break the guarantee.
		w.deadLetter(ctx, rec, "record key does not match the subscriber key", rec.Value)
		return
	}
	if err := c.Validate(w.cfg.Limits); err != nil {
		w.finish(ctx, c, Outcome{State: status.Failed, Code: "invalid", Message: err.Error()})
		return
	}
	var out Outcome
	if err := w.retry(ctx, "apply "+c.ID, func(ctx context.Context) error {
		var e error
		out, e = w.apply.Apply(ctx, c)
		return e
	}); err != nil {
		return
	}
	w.finish(ctx, c, out)
}

// finish records the outcome and publishes the event.
func (w *Worker) finish(ctx context.Context, c *command.Command, out Outcome) {
	res := status.Result{State: out.State, Outcome: out.Code, Message: out.Message,
		Balance: out.Res.Balance, OldBalance: out.Res.OldBalance, From: out.Res.From, To: out.Res.To, Version: out.Res.Version}
	if c.Type == command.AttachOffering { // the record's from / to carry the offerings
		res.From, res.To, res.Balance = out.Res.PrevOffering, out.Res.Offering, int64(out.Res.Buckets)
	}
	if w.retry(ctx, "record "+c.ID, func(ctx context.Context) error {
		_, err := w.status.Complete(ctx, c.ID, res)
		return err
	}) != nil {
		return
	}
	raw, err := eventFor(c, out).encode()
	if err != nil {
		w.cfg.Logf("worker: encode event for %s: %v", c.ID, err)
		return
	}
	if w.retry(ctx, "publish event for "+c.ID, func(ctx context.Context) error {
		return w.prod.Produce(ctx, w.cfg.Bus.EventsTopic, c.Key, raw, map[string]string{"command-id": c.ID})
	}) != nil {
		return
	}
	if out.State == status.Applied {
		w.applied.Add(1)
	} else {
		w.failedN.Add(1)
	}
}

// deadLetter parks a message that cannot be processed and marks its command
// failed when the id can still be read.
func (w *Worker) deadLetter(ctx context.Context, rec *kgo.Record, reason string, value []byte) {
	if w.retry(ctx, "dead-letter", func(ctx context.Context) error {
		return w.prod.Produce(ctx, w.cfg.Bus.DLQTopic, string(rec.Key), value, map[string]string{
			"reason": reason, "source-topic": rec.Topic,
			"source-partition": strconv.Itoa(int(rec.Partition)), "source-offset": strconv.FormatInt(rec.Offset, 10),
		})
	}) != nil {
		return
	}
	var peek struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(rec.Value, &peek) == nil && command.ValidID(peek.ID) {
		_ = w.retry(ctx, "record "+peek.ID, func(ctx context.Context) error {
			_, err := w.status.Complete(ctx, peek.ID, status.Result{State: status.Failed, Outcome: "dead_lettered", Message: reason})
			return err
		})
	}
	w.dead.Add(1)
	w.cfg.Logf("worker: dead-lettered %s/%d@%d: %s", rec.Topic, rec.Partition, rec.Offset, reason)
}
