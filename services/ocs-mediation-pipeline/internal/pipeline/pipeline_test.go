package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/dedup"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/sink"
)

var t0 = time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)

type mapEnricher struct {
	mu   sync.Mutex
	subs map[string]string // imsi -> key
	down error
}

func (m *mapEnricher) SubscriberKey(_ context.Context, r *cdr.Record) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down != nil {
		return "", m.down
	}
	if k, ok := m.subs[r.IMSI]; ok {
		return k, nil
	}
	return "", identity.ErrUnknown
}

type env struct {
	p   *Pipeline
	dir string
	enr *mapEnricher
	led *ledger.Ledger
	snk *sink.Sink
	dd  *dedup.Store
}

func setup(t *testing.T) *env {
	t.Helper()
	rdb := redistest.Start(t)
	dd, err := dedup.New(context.Background(), rdb, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	snk, err := sink.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	led := ledger.New(rdb, "")
	enr := &mapEnricher{subs: map[string]string{"262010000000001": "alice", "262010000000002": "bob"}}
	return &env{p: &Pipeline{Dedup: dd, Ledger: led, Sink: snk, Enricher: enr, Now: func() time.Time { return t0 }}, dir: dir, enr: enr, led: led, snk: snk, dd: dd}
}

func rec(imsi string, seq int64, up, down int64) *cdr.Record {
	return &cdr.Record{Type: "PGW", NodeID: "pgw-1", ChargingID: 7, Sequence: seq, IMSI: imsi, OpenTime: t0.Add(time.Duration(seq) * time.Minute),
		Duration: 60, Containers: []cdr.Container{{RatingGroup: 100, Uplink: up, Downlink: down}}}
}

func item(pos string, r *cdr.Record) Item { return Item{Pos: pos, Rec: r} }

func lines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func glob(t *testing.T, dir, pattern string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, pattern))
	sort.Strings(m)
	return m
}

func TestOutputControlsAndEnrichment(t *testing.T) {
	e := setup(t)
	u := OffsetUnit("cdr", 0, 0, 5)
	items := []Item{
		item("cdr/0@0", rec("262010000000001", 1, 100, 900)),
		item("cdr/0@1", rec("262010000000002", 2, 10, 20)),
		item("cdr/0@2", rec("262019999999999", 3, 5, 5)),                                 // unknown subscriber
		{Pos: "cdr/0@3", Err: errors.New("cdr: invalid record: x"), Raw: []byte("junk")}, // undecodable
		item("cdr/0@4", rec("262010000000001", 1, 100, 900)),                             // duplicate of @0 within the unit
	}
	c, err := e.p.Process(context.Background(), u, items)
	if err != nil {
		t.Fatal(err)
	}
	if c.In != 5 || c.Output != 2 || c.Suspense != 1 || c.Rejected != 1 || c.Duplicates != 1 || !c.Balanced {
		t.Fatalf("%+v", c)
	}
	if c.UplinkBytes != 110 || c.DownlinkBytes != 920 || c.DurationSecs != 120 {
		t.Fatalf("totals %+v", c)
	}
	files := glob(t, e.dir, "cdr/dt=2030-06-01/*.jsonl")
	if len(files) != 1 {
		t.Fatalf("%v", files)
	}
	got := lines(t, files[0])
	if len(got) != 2 || got[0]["subscriberKey"] != "alice" || got[1]["subscriberKey"] != "bob" || got[0]["source"] != "cdr/0@0" {
		t.Fatalf("%v", got)
	}
	if s := lines(t, glob(t, e.dir, "suspense/dt=*/*.jsonl")[0]); s[0]["reason"] != "unknown_subscriber" {
		t.Fatalf("%v", s)
	}
	if r := lines(t, glob(t, e.dir, "rejects/*.jsonl")[0]); r[0]["rawBase64"] != "anVuaw==" {
		t.Fatalf("%v", r)
	}
	if d := lines(t, glob(t, e.dir, "duplicates/*.jsonl")[0]); d[0]["pos"] != "cdr/0@4" {
		t.Fatalf("%v", d)
	}
	// The controls file on disk matches what was returned, and nothing is left staged.
	var onDisk Controls
	raw, _ := os.ReadFile(glob(t, e.dir, "controls/*.json")[0])
	if err := json.Unmarshal(raw, &onDisk); err != nil || onDisk.Output != 2 || !onDisk.Balanced {
		t.Fatalf("%v %+v", err, onDisk)
	}
	if left, _ := os.ReadDir(filepath.Join(e.dir, ".staging")); len(left) != 0 {
		t.Fatalf("staging not cleaned: %v", left)
	}
}

func TestDuplicateAcrossUnitsButNotReplay(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	r := rec("262010000000001", 1, 1, 1)
	if _, err := e.p.Process(ctx, OffsetUnit("cdr", 0, 0, 0), []Item{item("cdr/0@0", r)}); err != nil {
		t.Fatal(err)
	}
	// The same CDR arrives again on another partition: a duplicate.
	c, err := e.p.Process(ctx, OffsetUnit("cdr", 1, 0, 0), []Item{item("cdr/1@0", rec("262010000000001", 1, 1, 1))})
	if err != nil || c.Duplicates != 1 || c.Output != 0 {
		t.Fatalf("%v %+v", err, c)
	}
	// And in a re-sent file under a different name.
	c, err = e.p.Process(ctx, FileUnit("copy.json", strings.Repeat("a", 64)), []Item{item("copy.json#0", rec("262010000000001", 1, 1, 1))})
	if err != nil || c.Duplicates != 1 {
		t.Fatalf("%v %+v", err, c)
	}
}

func TestReplayAfterCrashRebuildsTheSameOutput(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	u := OffsetUnit("cdr", 2, 10, 11)
	items := []Item{item("cdr/2@10", rec("262010000000001", 1, 1, 1)), item("cdr/2@11", rec("262010000000002", 2, 2, 2))}

	// First attempt: claims the duplicate keys, then the process dies before commit.
	var keys, poss []string
	for _, it := range items {
		keys, poss = append(keys, it.Rec.Key()), append(poss, it.Pos)
	}
	if _, err := e.dd.Claim(ctx, keys, poss); err != nil {
		t.Fatal(err)
	}
	// Second attempt after restart: the same positions are not duplicates of themselves.
	c, err := e.p.Process(ctx, u, items)
	if err != nil || c.Duplicates != 0 || c.Output != 2 {
		t.Fatalf("replay misread as duplicates: %v %+v", err, c)
	}
	// A third attempt (the unit already committed) is stale and changes nothing.
	before := glob(t, e.dir, "cdr/dt=*/*")
	if _, err := e.p.Process(ctx, u, items); !errors.Is(err, ErrStale) {
		t.Fatalf("%v", err)
	}
	if after := glob(t, e.dir, "cdr/dt=*/*"); fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatal("output changed on a stale replay")
	}
}

func TestOverlappingUnitIsDiscardedNotTrimmed(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	if _, err := e.p.Process(ctx, OffsetUnit("cdr", 0, 0, 9), []Item{item("cdr/0@5", rec("262010000000001", 1, 1, 1))}); err != nil {
		t.Fatal(err)
	}
	// Another process, after a rebalance, produced 5..14 from the same partition.
	_, err := e.p.Process(ctx, OffsetUnit("cdr", 0, 5, 14), []Item{item("cdr/0@12", rec("262010000000002", 2, 1, 1))})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("%v", err)
	}
	if files := glob(t, e.dir, "cdr/dt=*/kafka-cdr-0-5-14*"); len(files) != 0 {
		t.Fatalf("an overlapping unit published %v", files)
	}
	if left, _ := os.ReadDir(filepath.Join(e.dir, ".staging")); len(left) != 0 {
		t.Fatalf("staging not cleaned: %v", left)
	}
	if wm, _ := e.led.Watermark(ctx, "cdr/0"); wm != 9 {
		t.Fatalf("watermark %d", wm)
	}
}

func TestInfrastructureFailureCommitsNothingAndRetrySucceeds(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	u := OffsetUnit("cdr", 0, 0, 0)
	items := []Item{item("cdr/0@0", rec("262010000000001", 1, 1, 1))}
	e.enr.down = errors.New("directory unavailable")
	if _, err := e.p.Process(ctx, u, items); err == nil {
		t.Fatal("a directory outage must fail the unit, not send the record to suspense")
	}
	if wm, _ := e.led.Watermark(ctx, "cdr/0"); wm != -1 {
		t.Fatalf("watermark advanced to %d", wm)
	}
	if files := glob(t, e.dir, "*/*/*.jsonl"); len(files) != 0 {
		t.Fatalf("output published: %v", files)
	}
	e.enr.down = nil
	if c, err := e.p.Process(ctx, u, items); err != nil || c.Output != 1 || c.Duplicates != 0 {
		t.Fatalf("%v %+v", err, c)
	}
}

func TestRecoverPublishesCommittedAndDropsUncommitted(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	mk := func(u ledger.Unit) {
		if err := e.snk.Stage(u, []sink.File{{Rel: "cdr/dt=2030-06-01/" + u.Name + ".jsonl", Data: []byte("{}\n")}}); err != nil {
			t.Fatal(err)
		}
	}
	committed, pending := OffsetUnit("cdr", 0, 0, 4), OffsetUnit("cdr", 0, 5, 9)
	mk(committed)
	mk(pending)
	// A staging directory that never got its meta.json (crash while staging).
	os.MkdirAll(filepath.Join(e.dir, ".staging", "half", "data"), 0o755)
	if ok, err := e.led.Commit(ctx, committed); !ok || err != nil { // the crash happened right after this
		t.Fatal(ok, err)
	}
	pub, dis, err := e.snk.Recover(ctx, e.led)
	if err != nil || pub != 1 || dis != 2 {
		t.Fatalf("%d %d %v", pub, dis, err)
	}
	if files := glob(t, e.dir, "cdr/dt=*/*.jsonl"); len(files) != 1 || !strings.Contains(files[0], "-0-0-4.") {
		t.Fatalf("%v", files)
	}
	if left, _ := os.ReadDir(filepath.Join(e.dir, ".staging")); len(left) != 0 {
		t.Fatalf("%v", left)
	}
}

func TestEveryRecordLandsInExactlyOneOutput(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var items []Item
	for i := 0; i < 300; i++ {
		imsi := []string{"262010000000001", "262010000000002", "262017777777777"}[i%3]
		r := rec(imsi, int64(i%100), 1, 1) // sequence repeats: duplicates by design
		var it Item
		if i%50 == 49 {
			it = Item{Pos: fmt.Sprintf("p@%d", i), Err: errors.New("bad"), Raw: []byte("x")}
		} else {
			it = item(fmt.Sprintf("p@%d", i), r)
		}
		items = append(items, it)
	}
	c, err := e.p.Process(ctx, OffsetUnit("cdr", 0, 0, 299), items)
	if err != nil {
		t.Fatal(err)
	}
	count := func(pattern string) (n int) {
		for _, f := range glob(t, e.dir, pattern) {
			n += len(lines(t, f))
		}
		return
	}
	got := count("cdr/dt=*/*.jsonl") + count("suspense/dt=*/*.jsonl") + count("rejects/*.jsonl") + count("duplicates/*.jsonl")
	if got != 300 || !c.Balanced || c.In != 300 {
		t.Fatalf("%d records accounted for in files, controls %+v", got, c)
	}
	if c.Output != count("cdr/dt=*/*.jsonl") || c.Suspense != count("suspense/dt=*/*.jsonl") {
		t.Fatalf("controls disagree with files: %+v", c)
	}
}

func TestRatingUsesThePriceInEffectWhenTheRecordOpened(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	cat := catalog.NewStore(0)
	pub := func(from time.Time, num int64) {
		d := catalog.Document{EffectiveFrom: from, Plans: []catalog.PricePlan{{ID: "std", Rules: []catalog.ChargeRule{
			{ID: "data", When: `rg == 100`, Cash: &catalog.Price{Num: num, Den: 1}},
			{ID: "barred-rg", When: `rg == 200`, Classes: []string{"promo"}},
		}}}}
		if _, _, err := cat.Publish(d, 0); err != nil {
			t.Fatal(err)
		}
	}
	pub(t0.Add(-24*time.Hour), 2)
	pub(t0.Add(30*time.Minute), 10) // price rises 30 minutes after t0
	e.p.Rater = CatalogRater{Tariff: &catalog.Tariff{Store: cat}, Plan: "std"}

	early := rec("262010000000001", 0, 10, 20) // opens at t0: price 2
	late := rec("262010000000002", 60, 10, 20) // opens at t0+60m: price 10
	noCash := rec("262010000000001", 5, 1, 1)
	noCash.Containers[0].RatingGroup = 200
	c, err := e.p.Process(ctx, OffsetUnit("cdr", 0, 0, 2), []Item{item("a", early), item("b", late), item("c", noCash)})
	if err != nil {
		t.Fatal(err)
	}
	if c.Output != 3 || c.Unrated != 1 || c.ChargeTotal != 30*2+30*10 {
		t.Fatalf("%+v", c)
	}
	var got []map[string]any
	for _, f := range glob(t, e.dir, "cdr/dt=*/*.jsonl") {
		got = append(got, lines(t, f)...)
	}
	byKey := map[string]map[string]any{}
	for _, m := range got {
		byKey[m["subscriberKey"].(string)+fmt.Sprint(m["sequence"])] = m
	}
	if byKey["alice0"]["charge"].(float64) != 60 || byKey["bob60"]["charge"].(float64) != 300 {
		t.Fatalf("%v", got)
	}
	if byKey["alice0"]["catalogVersion"].(float64) != 1 || byKey["bob60"]["catalogVersion"].(float64) != 2 {
		t.Fatalf("%v", got)
	}
	if _, has := byKey["alice5"]["charge"]; has {
		t.Fatalf("an unrated record carries a charge: %v", byKey["alice5"])
	}
}

func TestIdentityEnricherAgainstTheDirectory(t *testing.T) {
	rdb := redistest.Start(t)
	ctx := context.Background()
	dir := identity.NewRedisDirectory(rdb, "")
	if err := dir.Bind(ctx, identity.Identity{Type: identity.IMSI, Value: "262010000000001"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := dir.Bind(ctx, identity.Identity{Type: identity.MSISDN, Value: "491701234567"}, "bob"); err != nil {
		t.Fatal(err)
	}
	en := IdentityEnricher{Resolver: identity.NewResolver(dir, identity.ConflictReject)}
	r := &cdr.Record{IMSI: "262010000000001"}
	if k, err := en.SubscriberKey(ctx, r); err != nil || k != "alice" {
		t.Fatalf("%q %v", k, err)
	}
	r = &cdr.Record{IMSI: "262010000000001", MSISDN: "491701234567"} // two subscribers: refuse
	if _, err := en.SubscriberKey(ctx, r); !errors.Is(err, identity.ErrConflict) {
		t.Fatalf("%v", err)
	}
	if _, err := en.SubscriberKey(ctx, &cdr.Record{IMSI: "262019999999999"}); !errors.Is(err, identity.ErrUnknown) {
		t.Fatalf("%v", err)
	}
}

func BenchmarkProcessUnitOf1000(b *testing.B) {
	rdb := redistest.Start(b)
	dd, _ := dedup.New(context.Background(), rdb, "", time.Hour)
	snk, _ := sink.New(b.TempDir())
	p := &Pipeline{Dedup: dd, Ledger: ledger.New(rdb, ""), Sink: snk,
		Enricher: &mapEnricher{subs: map[string]string{"262010000000001": "alice"}}}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		items := make([]Item, 1000)
		for i := range items {
			r := rec("262010000000001", int64(n*1000+i), 1, 1)
			items[i] = Item{Pos: fmt.Sprintf("b@%d", n*1000+i), Rec: r}
		}
		if _, err := p.Process(context.Background(), OffsetUnit("bench", 0, int64(n*1000), int64(n*1000+999)), items); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "records/s")
}
