package source

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/pipeline"
)

// Watermarks answers how far each partition's output is committed.
type Watermarks interface {
	Watermark(ctx context.Context, stream string) (int64, error)
}

// KafkaConfig configures the stream consumer.
type KafkaConfig struct {
	Brokers    []string
	Topic      string
	Group      string
	MaxPoll    int           // records per poll (default 5000)
	MinBackoff time.Duration // retry delay after an infrastructure error (default 100ms)
	MaxBackoff time.Duration // (default 5s)
	Logf       func(string, ...any)
}

// Kafka consumes normalized JSON CDRs. Each partition's slice of a poll is one
// unit: records at or below the partition's ledger watermark are skipped (their
// output exists), the rest are processed and committed through the ledger, and
// only then are Kafka offsets committed. Kafka offsets are a bookmark; the
// ledger is the truth, so losing or rewinding the group offsets cannot cause
// duplicated or missing output.
type Kafka struct {
	cfg  KafkaConfig
	proc Processor
	wm   Watermarks
	cl   *kgo.Client
}

// NewKafka connects a consumer. Extra options (TLS, SASL...) pass through.
func NewKafka(cfg KafkaConfig, proc Processor, wm Watermarks, opts ...kgo.Opt) (*Kafka, error) {
	if cfg.MaxPoll == 0 {
		cfg.MaxPoll = 5000
	}
	if cfg.MinBackoff == 0 {
		cfg.MinBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)...)
	if err != nil {
		return nil, err
	}
	return &Kafka{cfg: cfg, proc: proc, wm: wm, cl: cl}, nil
}

// Close leaves the group.
func (k *Kafka) Close() { k.cl.Close() }

// Run consumes until ctx ends.
func (k *Kafka) Run(ctx context.Context) error {
	defer k.cl.AllowRebalance()
	for ctx.Err() == nil {
		fetches := k.cl.PollRecords(ctx, k.cfg.MaxPoll)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) { k.cfg.Logf("fetch %s/%d: %v", t, p, err) })

		var commit []*kgo.Record
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 || ctx.Err() != nil {
				return
			}
			if last := k.partition(ctx, p.Topic, p.Partition, p.Records); last != nil {
				commit = append(commit, last)
			}
		})
		if len(commit) > 0 {
			if err := k.cl.CommitRecords(ctx, commit...); err != nil && ctx.Err() == nil {
				k.cfg.Logf("commit offsets: %v", err) // harmless: the ledger skips re-read records
			}
		}
		k.cl.AllowRebalance()
	}
	return nil
}

// partition processes one partition's records and returns the record whose
// offset may be committed (nil if nothing was done because ctx ended).
func (k *Kafka) partition(ctx context.Context, topic string, part int32, recs []*kgo.Record) *kgo.Record {
	sort.Slice(recs, func(i, j int) bool { return recs[i].Offset < recs[j].Offset })
	last := recs[len(recs)-1]
	backoff := k.cfg.MinBackoff
	for ctx.Err() == nil {
		err := k.process(ctx, topic, part, recs)
		if err == nil {
			return last
		}
		k.cfg.Logf("%s/%d offsets %d-%d: %v (retrying in %v)", topic, part, recs[0].Offset, last.Offset, err, backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > k.cfg.MaxBackoff {
			backoff = k.cfg.MaxBackoff
		}
	}
	return nil
}

func (k *Kafka) process(ctx context.Context, topic string, part int32, recs []*kgo.Record) error {
	stream := fmt.Sprintf("%s/%d", topic, part)
	wm, err := k.wm.Watermark(ctx, stream)
	if err != nil {
		return err
	}
	var fresh []*kgo.Record
	for _, r := range recs {
		if r.Offset > wm {
			fresh = append(fresh, r)
		}
	}
	if len(fresh) == 0 {
		return nil // all of it was committed before; just move the bookmark
	}
	items := make([]pipeline.Item, len(fresh))
	for i, r := range fresh {
		it := pipeline.Item{Pos: fmt.Sprintf("%s@%d", stream, r.Offset), Raw: r.Value}
		if rec, err := cdr.FromJSON(r.Value); err != nil {
			it.Err = err
		} else {
			it.Rec = &rec
		}
		items[i] = it
	}
	u := pipeline.OffsetUnit(topic, part, fresh[0].Offset, fresh[len(fresh)-1].Offset)
	_, err = k.proc.Process(ctx, u, items)
	if err != nil && isStale(err) {
		return nil // another consumer committed an overlapping range; our records are in its output
	}
	return err
}

func isStale(err error) bool { return errors.Is(err, pipeline.ErrStale) }
