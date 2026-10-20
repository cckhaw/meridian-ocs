// Package pipeline is the mediation core: parse -> deduplicate -> enrich ->
// rate -> publish, applied to one input unit at a time.
//
// Exactly-once effect. Input is delivered at least once (Kafka redelivery,
// a re-sent file). The pipeline makes the *output* exactly-once by committing
// a unit's output through the ledger:
//
//  1. process the unit and stage its output files (nothing visible yet);
//  2. ledger.Commit: one atomic Redis step that succeeds for exactly one
//     attempt at a unit, and refuses a unit overlapping committed work;
//  3. publish (rename) the staged files.
//
// A crash after 2 and before 3 is repaired at start-up by Sink.Recover; a crash
// before 2 leaves only staging, which is discarded. Records seen again from a
// *different* source position are duplicates (dedup.Store); records seen again
// from the same position are replays and are processed normally, so a retried
// unit rebuilds the same output.
package pipeline

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/dedup"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/sink"
)

// Item is one input record after decoding. Exactly one of Rec and Err is set.
type Item struct {
	Pos string // source position: "file#12" or "topic/3@4711"
	Rec *cdr.Record
	Err error
	Raw []byte // original bytes, kept for the reject file (truncated there)
}

// Enricher resolves the internal subscriber key. ErrUnknown, ErrConflict and
// ErrInvalid send the record to suspense; any other error is retried.
type Enricher interface {
	SubscriberKey(ctx context.Context, r *cdr.Record) (string, error)
}

// Rater adds a list-price charge to a record.
type Rater interface {
	Rate(r *cdr.Record) (unrated bool)
}

// Pipeline wires the stages.
type Pipeline struct {
	Dedup    *dedup.Store
	Ledger   *ledger.Ledger
	Sink     *sink.Sink
	Enricher Enricher
	Rater    Rater // optional
	Now      func() time.Time

	totals Totals
}

// Totals are cumulative counters since start-up, for metrics.
type Totals struct {
	Units, StaleUnits, Failures                         atomic.Uint64
	In, Output, Suspense, Rejected, Duplicates, Unrated uint64Counter
}

// uint64Counter is an atomic counter with a Load, to keep Totals' zero value usable.
type uint64Counter struct{ v atomic.Uint64 }

func (c *uint64Counter) add(n int)    { c.v.Add(uint64(n)) }
func (c *uint64Counter) Load() uint64 { return c.v.Load() }

// Totals returns the counters.
func (p *Pipeline) Totals() *Totals { return &p.totals }

// Controls are the audit totals of one unit. They are written next to the
// output and must balance: In = Output + Suspense + Rejected + Duplicates.
type Controls struct {
	Unit       string    `json:"unit"`
	Stream     string    `json:"stream,omitempty"`
	First      int64     `json:"firstOffset,omitempty"`
	Last       int64     `json:"lastOffset,omitempty"`
	ProcessedA time.Time `json:"processedAt"`

	In         int  `json:"in"`
	Output     int  `json:"output"`
	Suspense   int  `json:"suspense"`
	Rejected   int  `json:"rejected"`
	Duplicates int  `json:"duplicates"`
	Unrated    int  `json:"unrated"`
	Balanced   bool `json:"balanced"`

	// Sums over the records written to cdr/.
	UplinkBytes   int64  `json:"uplinkBytes"`
	DownlinkBytes int64  `json:"downlinkBytes"`
	DurationSecs  int64  `json:"durationSeconds"`
	ChargeTotal   int64  `json:"chargeTotal"`
	FirstOpen     string `json:"firstOpenTime,omitempty"`
	LastOpen      string `json:"lastOpenTime,omitempty"`
}

// ErrStale means the unit overlaps output committed by someone else and was
// discarded. It is not a failure; the caller re-reads its position and moves on.
var ErrStale = errors.New("pipeline: unit already committed")

type rejectLine struct {
	Pos   string `json:"pos"`
	Error string `json:"error"`
	Raw   string `json:"rawBase64,omitempty"`
}

type suspenseLine struct {
	Reason string      `json:"reason"`
	Record *cdr.Record `json:"record"`
}

type dupLine struct {
	Key    string `json:"key"`
	Pos    string `json:"pos"`
	Type   string `json:"type"`
	Node   string `json:"nodeId"`
	Charge uint64 `json:"chargingId"`
	Seq    int64  `json:"sequence"`
}

const maxRawInReject = 4096

// Process runs one unit end to end. On success the unit's output is published
// and its controls returned. A non-nil error that is not ErrStale means
// nothing was committed and the unit must be retried.
func (p *Pipeline) Process(ctx context.Context, u ledger.Unit, items []Item) (Controls, error) {
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	c := Controls{Unit: u.Name, Stream: u.Stream, First: u.First, Last: u.Last, ProcessedA: now().UTC(), In: len(items)}

	var rejects, dups bytes.Buffer
	suspense := map[string]*bytes.Buffer{}
	out := map[string]*bytes.Buffer{}
	enc := func(b *bytes.Buffer, v any) error { return json.NewEncoder(b).Encode(v) }

	// 1. Decode errors are rejects; the rest go on to duplicate detection.
	var live []Item
	for _, it := range items {
		if it.Err != nil {
			c.Rejected++
			raw := it.Raw
			if len(raw) > maxRawInReject {
				raw = raw[:maxRawInReject]
			}
			if err := enc(&rejects, rejectLine{it.Pos, it.Err.Error(), base64.StdEncoding.EncodeToString(raw)}); err != nil {
				return c, err
			}
			continue
		}
		live = append(live, it)
	}

	// 2. Duplicates: against the window in Redis, and within this unit.
	keys := make([]string, len(live))
	poss := make([]string, len(live))
	for i, it := range live {
		keys[i], poss[i] = it.Rec.Key(), it.Pos
	}
	isNew := make([]bool, len(live))
	if len(live) > 0 {
		var err error
		if isNew, err = p.Dedup.Claim(ctx, keys, poss); err != nil {
			return c, fmt.Errorf("duplicate check: %w", err)
		}
	}
	seenHere := map[string]bool{}
	var fresh []Item
	for i, it := range live {
		if !isNew[i] || seenHere[keys[i]] {
			c.Duplicates++
			r := it.Rec
			if err := enc(&dups, dupLine{keys[i], it.Pos, r.Type, r.NodeID, r.ChargingID, r.Sequence}); err != nil {
				return c, err
			}
			continue
		}
		seenHere[keys[i]] = true
		fresh = append(fresh, it)
	}

	// 3. Enrich and rate.
	for _, it := range fresh {
		r := it.Rec
		r.Source = it.Pos
		key, err := p.Enricher.SubscriberKey(ctx, r)
		day := r.OpenTime.UTC().Format("2006-01-02")
		switch {
		case err == nil:
			r.SubscriberKey = key
		case errors.Is(err, identity.ErrUnknown), errors.Is(err, identity.ErrConflict), errors.Is(err, identity.ErrInvalid), errors.Is(err, identity.ErrNoIdentity):
			c.Suspense++
			b := suspense[day]
			if b == nil {
				b = new(bytes.Buffer)
				suspense[day] = b
			}
			if err := enc(b, suspenseLine{Reason: reasonOf(err), Record: r}); err != nil {
				return c, err
			}
			continue
		default:
			return c, fmt.Errorf("subscriber lookup: %w", err)
		}
		if p.Rater != nil && p.Rater.Rate(r) {
			c.Unrated++
		}
		up, down := r.Totals()
		c.Output++
		c.UplinkBytes += up
		c.DownlinkBytes += down
		c.DurationSecs += r.Duration
		if r.Charge != nil {
			c.ChargeTotal += *r.Charge
		}
		ot := r.OpenTime.UTC().Format(time.RFC3339Nano)
		if c.FirstOpen == "" || ot < c.FirstOpen {
			c.FirstOpen = ot
		}
		if ot > c.LastOpen {
			c.LastOpen = ot
		}
		b := out[day]
		if b == nil {
			b = new(bytes.Buffer)
			out[day] = b
		}
		if err := enc(b, r); err != nil {
			return c, err
		}
	}
	c.Balanced = c.In == c.Output+c.Suspense+c.Rejected+c.Duplicates

	// 4. Stage, commit, publish.
	var files []sink.File
	for _, day := range sortedKeys(out) {
		files = append(files, sink.File{Rel: fmt.Sprintf("cdr/dt=%s/%s.jsonl", day, u.Name), Data: out[day].Bytes()})
	}
	for _, day := range sortedKeys(suspense) {
		files = append(files, sink.File{Rel: fmt.Sprintf("suspense/dt=%s/%s.jsonl", day, u.Name), Data: suspense[day].Bytes()})
	}
	if rejects.Len() > 0 {
		files = append(files, sink.File{Rel: fmt.Sprintf("rejects/%s.jsonl", u.Name), Data: rejects.Bytes()})
	}
	if dups.Len() > 0 {
		files = append(files, sink.File{Rel: fmt.Sprintf("duplicates/%s.jsonl", u.Name), Data: dups.Bytes()})
	}
	cb, _ := json.MarshalIndent(c, "", "  ")
	files = append(files, sink.File{Rel: fmt.Sprintf("controls/%s.json", u.Name), Data: cb})

	if err := p.Sink.Stage(u, files); err != nil {
		return c, fmt.Errorf("staging output: %w", err)
	}
	ok, err := p.Ledger.Commit(ctx, u)
	if err != nil {
		return c, fmt.Errorf("ledger commit: %w", err)
	}
	if !ok {
		_ = p.Sink.Discard(u.Name)
		p.totals.StaleUnits.Add(1)
		return c, ErrStale
	}
	p.totals.Units.Add(1)
	p.totals.In.add(c.In)
	p.totals.Output.add(c.Output)
	p.totals.Suspense.add(c.Suspense)
	p.totals.Rejected.add(c.Rejected)
	p.totals.Duplicates.add(c.Duplicates)
	p.totals.Unrated.add(c.Unrated)
	if err := p.Sink.Publish(u.Name); err != nil {
		// Committed but not yet visible: Recover (at the next start) finishes it.
		return c, fmt.Errorf("publishing committed output (will be recovered at start-up): %w", err)
	}
	return c, nil
}

func reasonOf(err error) string {
	switch {
	case errors.Is(err, identity.ErrUnknown):
		return "unknown_subscriber"
	case errors.Is(err, identity.ErrConflict):
		return "conflicting_identities"
	case errors.Is(err, identity.ErrInvalid):
		return "invalid_identifier"
	}
	return "no_identifier"
}

func sortedKeys(m map[string]*bytes.Buffer) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// IdentityEnricher resolves IMSI / MSISDN through the identity directory.
type IdentityEnricher struct{ Resolver *identity.Resolver }

// SubscriberKey implements Enricher. The IMSI takes precedence: it is what the
// network authenticated.
func (e IdentityEnricher) SubscriberKey(ctx context.Context, r *cdr.Record) (string, error) {
	var ids []identity.Identity
	if r.IMSI != "" {
		ids = append(ids, identity.Identity{Type: identity.IMSI, Value: r.IMSI})
	}
	if r.MSISDN != "" {
		ids = append(ids, identity.Identity{Type: identity.MSISDN, Value: r.MSISDN})
	}
	return e.Resolver.Resolve(ctx, ids)
}

// FileUnit names the unit for a whole input file. The name keeps the original
// base name for operators and adds a content hash so a different file with the
// same name is a different unit.
func FileUnit(name, sha string) ledger.Unit {
	base := strings.NewReplacer("/", "_", "\\", "_").Replace(name)
	base = strings.TrimLeft(base, ".")
	return ledger.Unit{Kind: ledger.File, Name: "file-" + base + "-" + sha[:12], ID: name + "@" + sha}
}

// OffsetUnit names the unit for a range of one partition.
func OffsetUnit(topic string, partition int32, first, last int64) ledger.Unit {
	return ledger.Unit{
		Kind: ledger.Offsets, Stream: fmt.Sprintf("%s/%d", topic, partition),
		Name:  fmt.Sprintf("kafka-%s-%d-%d-%d", strings.ReplaceAll(topic, "/", "_"), partition, first, last),
		First: first, Last: last,
	}
}
