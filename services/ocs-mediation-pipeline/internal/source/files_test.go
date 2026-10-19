package source

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/pipeline"
)

type fakeProc struct {
	mu    sync.Mutex
	units []ledger.Unit
	items [][]pipeline.Item
	err   error
}

func (f *fakeProc) Process(_ context.Context, u ledger.Unit, items []pipeline.Item) (pipeline.Controls, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return pipeline.Controls{}, f.err
	}
	f.units, f.items = append(f.units, u), append(f.items, items)
	return pipeline.Controls{In: len(items), Output: len(items), Balanced: true}, nil
}

type doneSet map[string]bool

func (d doneSet) FileDone(_ context.Context, id string) (bool, error) { return d[id], nil }

const line = `{"type":"PGW","nodeId":"n","chargingId":1,"sequence":%d,"imsi":"262010000000001","openTime":"2030-06-01T12:00:00Z","durationSeconds":1}`

func newFiles(t *testing.T, p *fakeProc, done doneSet, mut func(*FileConfig)) (*Files, string, *time.Time) {
	t.Helper()
	dir := t.TempDir()
	cfg := FileConfig{Dir: dir, Settle: time.Minute, Mapping: cdr.PGWMapping()}
	if mut != nil {
		mut(&cfg)
	}
	f, err := NewFiles(cfg, p, done)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	f.now = func() time.Time { return now }
	return f, dir, &now
}

func TestFilesAreReadOnlyOnceSettled(t *testing.T) {
	p := &fakeProc{}
	f, dir, now := newFiles(t, p, doneSet{}, nil)
	body := strings.Replace(line, "%d", "1", 1) + "\n\n" + strings.Replace(line, "%d", "2", 1) + "\n"
	os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte(body), 0o644)
	os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, ".hidden.jsonl"), []byte(body), 0o644)

	if err := f.Scan(context.Background()); err != nil || len(p.units) != 0 {
		t.Fatalf("a file modified a moment ago was read: %v %d", err, len(p.units))
	}
	*now = now.Add(2 * time.Minute)
	if err := f.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.units) != 1 || len(p.items[0]) != 2 || p.items[0][1].Pos != "a.jsonl#2" { // line numbers count blank lines
		t.Fatalf("%d units, items %+v", len(p.units), p.items)
	}
	if _, err := os.Stat(filepath.Join(dir, "processed", "a.jsonl")); err != nil {
		t.Fatal("processed file not moved")
	}
	for _, n := range []string{"ignored.txt", ".hidden.jsonl"} {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s must be left alone", n)
		}
	}
}

func TestInfrastructureErrorLeavesTheFileForRetry(t *testing.T) {
	p := &fakeProc{err: errors.New("redis down")}
	f, dir, now := newFiles(t, p, doneSet{}, nil)
	os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte(strings.Replace(line, "%d", "1", 1)), 0o644)
	*now = now.Add(time.Hour)
	f.Scan(context.Background())
	if _, err := os.Stat(filepath.Join(dir, "a.jsonl")); err != nil {
		t.Fatal("a file whose processing failed was moved away")
	}
	p.err = nil
	f.Scan(context.Background())
	if _, err := os.Stat(filepath.Join(dir, "processed", "a.jsonl")); err != nil || len(p.units) != 1 {
		t.Fatalf("not retried: %v", err)
	}
}

func TestAlreadyCommittedFileIsNotProcessedAgain(t *testing.T) {
	p := &fakeProc{}
	body := strings.Replace(line, "%d", "1", 1)
	f, dir, now := newFiles(t, p, nil, nil)
	os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte(body), 0o644)
	*now = now.Add(time.Hour)
	// learn the unit id the watcher will compute
	f.Scan(context.Background())
	id := p.units[0].ID
	os.Rename(filepath.Join(dir, "processed", "a.jsonl"), filepath.Join(dir, "a.jsonl"))
	f.done = doneSet{id: true}
	p.units, p.items = nil, nil
	f.Scan(context.Background())
	if len(p.units) != 0 {
		t.Fatal("a committed file was processed again")
	}
	if _, err := os.Stat(filepath.Join(dir, "processed", "a.jsonl")); err != nil {
		t.Fatal("not moved to processed")
	}
}

func TestQuarantine(t *testing.T) {
	p := &fakeProc{}
	f, dir, now := newFiles(t, p, doneSet{}, func(c *FileConfig) { c.MaxFileBytes = 100; c.MaxRecords = 2 })
	os.WriteFile(filepath.Join(dir, "big.jsonl"), []byte(strings.Repeat("x", 500)), 0o644)
	os.WriteFile(filepath.Join(dir, "many.jsonl"), []byte(strings.Repeat("{}\n", 3)), 0o644)
	os.WriteFile(filepath.Join(dir, "cut.ber"), []byte{0xbf, 0x4f, 0x30, 0x83}, 0o644)
	*now = now.Add(time.Hour)
	f.Scan(context.Background())
	for _, n := range []string{"big.jsonl", "many.jsonl", "cut.ber"} {
		if _, err := os.Stat(filepath.Join(dir, "quarantine", n+".error")); err != nil {
			t.Errorf("%s: no error report: %v", n, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "quarantine", n)); err != nil {
			t.Errorf("%s: not quarantined", n)
		}
	}
	if len(p.units) != 0 {
		t.Fatal("a quarantined file reached the pipeline")
	}
}

func TestSameNameDifferentContentKeepsBothInProcessed(t *testing.T) {
	p := &fakeProc{}
	f, dir, now := newFiles(t, p, doneSet{}, nil)
	*now = now.Add(time.Hour)
	for i := 1; i <= 2; i++ {
		os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte(strings.Replace(line, "%d", string(rune('0'+i)), 1)), 0o644)
		f.Scan(context.Background())
		*now = now.Add(time.Second)
	}
	m, _ := filepath.Glob(filepath.Join(dir, "processed", "a.jsonl*"))
	if len(m) != 2 || len(p.units) != 2 || p.units[0].Name == p.units[1].Name {
		t.Fatalf("%v %d", m, len(p.units))
	}
}
