// Package sink writes pipeline output as files with an all-or-nothing publish.
//
// A unit's files are first staged under <root>/.staging/<unit>/, then the
// ledger commit decides whether the unit counts, and only then are the files
// moved into place with rename. A crash at any point is repaired by Recover:
// staged output whose unit was committed is published, anything else is
// deleted. Because a unit's file names are deterministic, publishing twice is
// harmless.
package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
)

// File is one output file: Rel is the path under the sink root.
type File struct {
	Rel  string
	Data []byte
}

// Sink is a directory tree (a local volume, or a mounted object store).
type Sink struct{ root string }

// New creates the root and staging directories.
func New(root string) (*Sink, error) {
	if root == "" {
		return nil, errors.New("sink: no output directory")
	}
	if err := os.MkdirAll(filepath.Join(root, ".staging"), 0o755); err != nil {
		return nil, err
	}
	return &Sink{root: root}, nil
}

// Root is the output directory.
func (s *Sink) Root() string { return s.root }

func (s *Sink) stageDir(name string) string { return filepath.Join(s.root, ".staging", name) }

type meta struct {
	Unit  ledger.Unit `json:"unit"`
	Files []string    `json:"files"`
}

func safeRel(rel string) error {
	if rel == "" || filepath.IsAbs(rel) || strings.Contains(rel, "..") || strings.HasPrefix(rel, ".staging") {
		return fmt.Errorf("sink: unsafe path %q", rel)
	}
	return nil
}

func validName(n string) bool {
	return n != "" && !strings.ContainsAny(n, `/\`) && !strings.HasPrefix(n, ".")
}

// Stage writes the unit's files (durably) into staging. The unit is not
// visible to readers until Publish.
func (s *Sink) Stage(u ledger.Unit, files []File) error {
	if !validName(u.Name) {
		return fmt.Errorf("sink: bad unit name %q", u.Name)
	}
	dir := s.stageDir(u.Name)
	if err := os.RemoveAll(dir); err != nil { // an earlier attempt of the same unit
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	m := meta{Unit: u}
	for _, f := range files {
		if err := safeRel(f.Rel); err != nil {
			return err
		}
		p := filepath.Join(dir, "data", f.Rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := writeSync(p, f.Data); err != nil {
			return err
		}
		m.Files = append(m.Files, f.Rel)
	}
	b, _ := json.Marshal(m)
	// meta.json is written last: a staging directory without it is incomplete
	// and is discarded by Recover.
	return writeSync(filepath.Join(dir, "meta.json"), b)
}

func writeSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Publish moves the staged files into place.
func (s *Sink) Publish(unitName string) error {
	dir := s.stageDir(unitName)
	raw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return err
	}
	var m meta
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	for _, rel := range m.Files {
		if err := safeRel(rel); err != nil {
			return err
		}
		dst := filepath.Join(s.root, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		src := filepath.Join(dir, "data", rel)
		if err := os.Rename(src, dst); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err // a missing source means an earlier publish already moved it
		}
	}
	return os.RemoveAll(dir)
}

// Discard removes a unit's staged output.
func (s *Sink) Discard(unitName string) error { return os.RemoveAll(s.stageDir(unitName)) }

// Recover repairs staging after a crash: committed units are published, the
// rest discarded. It returns how many of each.
func (s *Sink) Recover(ctx context.Context, led *ledger.Ledger) (published, discarded int, err error) {
	entries, err := os.ReadDir(filepath.Join(s.root, ".staging"))
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		name := e.Name()
		raw, rerr := os.ReadFile(filepath.Join(s.stageDir(name), "meta.json"))
		var m meta
		if rerr != nil || json.Unmarshal(raw, &m) != nil {
			if err := s.Discard(name); err != nil {
				return published, discarded, err
			}
			discarded++
			continue
		}
		ok, err := led.Committed(ctx, m.Unit)
		if err != nil {
			return published, discarded, err
		}
		if ok {
			if err := s.Publish(name); err != nil {
				return published, discarded, err
			}
			published++
		} else {
			if err := s.Discard(name); err != nil {
				return published, discarded, err
			}
			discarded++
		}
	}
	return published, discarded, nil
}
