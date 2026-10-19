// Package source reads input for the pipeline: a watched directory of CDR files
// and a Kafka topic of normalized records.
package source

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ber"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/pipeline"
)

// Processor is the pipeline surface the sources need.
type Processor interface {
	Process(ctx context.Context, u ledger.Unit, items []pipeline.Item) (pipeline.Controls, error)
}

// FileDone tells whether a file was committed already.
type FileDone interface {
	FileDone(ctx context.Context, id string) (bool, error)
}

// FileConfig configures the directory watcher.
type FileConfig struct {
	Dir          string        // where input files appear
	Settle       time.Duration // a file must be unchanged this long before it is read (default 5s)
	MaxFileBytes int64         // larger files are quarantined (default 256 MiB)
	MaxRecords   int           // more records than this quarantines the file (default 1,000,000)
	Mapping      cdr.Mapping   // BER mapping
	Logf         func(string, ...any)
}

// Files watches a directory. Layout under Dir:
//
//	*.ber *.dat *.cdr   BER encoded records, back to back
//	*.jsonl *.json     one normalized JSON record per line
//	processed/         files whose output is committed
//	quarantine/        files that cannot be framed, with a <name>.error beside each
type Files struct {
	cfg  FileConfig
	proc Processor
	done FileDone
	now  func() time.Time
}

// NewFiles creates a watcher.
func NewFiles(cfg FileConfig, proc Processor, done FileDone) (*Files, error) {
	if cfg.Dir == "" {
		return nil, errors.New("source: no input directory")
	}
	if cfg.Settle == 0 {
		cfg.Settle = 5 * time.Second
	}
	if cfg.MaxFileBytes == 0 {
		cfg.MaxFileBytes = 256 << 20
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = 1_000_000
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	for _, d := range []string{cfg.Dir, filepath.Join(cfg.Dir, "processed"), filepath.Join(cfg.Dir, "quarantine")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return &Files{cfg: cfg, proc: proc, done: done, now: time.Now}, nil
}

func kindOf(name string) (ber bool, ok bool) {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".ber", ".dat", ".cdr":
		return true, true
	case ".jsonl", ".json":
		return false, true
	}
	return false, false
}

// Run scans every interval until ctx ends.
func (f *Files) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := f.Scan(ctx); err != nil && ctx.Err() == nil {
			f.cfg.Logf("scan: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Scan processes every settled file once. A file that fails for an
// infrastructure reason stays in place and is retried by the next scan.
func (f *Files) Scan(ctx context.Context) error {
	entries, err := os.ReadDir(f.cfg.Dir)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if _, ok := kindOf(e.Name()); !ok {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() || f.now().Sub(info.ModTime()) < f.cfg.Settle {
			continue // still being written, or gone
		}
		if err := f.one(ctx, filepath.Join(f.cfg.Dir, e.Name()), info.Size()); err != nil {
			f.cfg.Logf("%s: %v (will retry)", e.Name(), err)
		}
	}
	return nil
}

func (f *Files) one(ctx context.Context, path string, size int64) error {
	name := filepath.Base(path)
	if size > f.cfg.MaxFileBytes {
		return f.quarantine(path, fmt.Errorf("file is %d bytes, over the limit of %d", size, f.cfg.MaxFileBytes))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // another node took it
			return nil
		}
		return err
	}
	sum := sha256.Sum256(data)
	unit := pipeline.FileUnit(name, hex.EncodeToString(sum[:]))

	if done, err := f.done.FileDone(ctx, unit.ID); err != nil {
		return err
	} else if done {
		return f.move(path, "processed") // already committed: a re-sent or half-moved file
	}

	isBER, _ := kindOf(name)
	var items []pipeline.Item
	if isBER {
		items, err = f.decodeBER(name, data)
	} else {
		items, err = f.decodeJSONL(name, data)
	}
	if err != nil {
		return f.quarantine(path, err)
	}
	c, err := f.proc.Process(ctx, unit, items)
	switch {
	case err == nil:
		f.cfg.Logf("%s: %d in, %d out, %d suspense, %d rejected, %d duplicates, balanced=%v", name, c.In, c.Output, c.Suspense, c.Rejected, c.Duplicates, c.Balanced)
	case errors.Is(err, pipeline.ErrStale):
		// someone else committed this exact file
	default:
		return err
	}
	return f.move(path, "processed")
}

// decodeBER frames the whole file first. A framing error means the stream
// cannot be trusted past that point (BER has no resynchronisation), so the
// whole file is refused rather than half processed.
func (f *Files) decodeBER(name string, data []byte) ([]pipeline.Item, error) {
	rd := ber.NewReader(bytes.NewReader(data))
	var items []pipeline.Item
	for n := 0; ; n++ {
		e, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return items, nil
		}
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", n, err)
		}
		if n >= f.cfg.MaxRecords {
			return nil, fmt.Errorf("more than %d records", f.cfg.MaxRecords)
		}
		it := pipeline.Item{Pos: fmt.Sprintf("%s#%d", name, n)}
		r, derr := cdr.FromBER(&e, &f.cfg.Mapping)
		if derr != nil {
			it.Err = derr
		} else {
			it.Rec = &r
		}
		items = append(items, it)
	}
}

func (f *Files) decodeJSONL(name string, data []byte) ([]pipeline.Item, error) {
	var items []pipeline.Item
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64<<10), cdr.MaxJSONSize+1)
	for n := 0; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if len(items) >= f.cfg.MaxRecords {
			return nil, fmt.Errorf("more than %d records", f.cfg.MaxRecords)
		}
		it := pipeline.Item{Pos: fmt.Sprintf("%s#%d", name, n), Raw: append([]byte(nil), line...)}
		r, err := cdr.FromJSON(line)
		if err != nil {
			it.Err = err
		} else {
			it.Rec = &r
		}
		items = append(items, it)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("line too long or unreadable: %w", err)
	}
	return items, nil
}

func (f *Files) move(path, sub string) error {
	dst := filepath.Join(f.cfg.Dir, sub, filepath.Base(path))
	if _, err := os.Stat(dst); err == nil { // same name, different content seen before
		dst += "." + f.now().UTC().Format("20060102T150405.000000000")
	}
	if err := os.Rename(path, dst); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f *Files) quarantine(path string, cause error) error {
	f.cfg.Logf("%s quarantined: %v", filepath.Base(path), cause)
	if err := f.move(path, "quarantine"); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(f.cfg.Dir, "quarantine", filepath.Base(path)+".error"), []byte(cause.Error()+"\n"), 0o644)
}
