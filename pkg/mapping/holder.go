package mapping

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Holder keeps the mapping in use and replaces it when its file changes. A file
// that does not parse or validate is reported and ignored: the previous mapping
// keeps serving, so a bad edit (by a person or an agent) cannot take translation
// down. It follows symlinks, so a Kubernetes ConfigMap volume works.
type Holder struct {
	path string
	cur  atomic.Pointer[loaded]

	failures atomic.Uint64
	reloads  atomic.Uint64
}

type loaded struct {
	m        *Mapper
	sum      string
	loadedAt time.Time
}

// Event reports the outcome of a reload attempt.
type Event struct {
	Reloaded bool   // a new mapping is now in use
	Name     string // its name
	Checksum string // short SHA-256 of the file
	Err      error  // set when the new file was rejected
}

// Open loads the file, which must be valid: a service that is configured to
// translate must not start with nothing to translate with.
func Open(path string) (*Holder, error) {
	h := &Holder{path: path}
	if ev := h.reload(); ev.Err != nil {
		return nil, ev.Err
	}
	return h, nil
}

func (h *Holder) reload() Event {
	raw, err := os.ReadFile(h.path)
	if err != nil {
		return h.fail(fmt.Errorf("mapping: %w", err))
	}
	sum := sha256.Sum256(raw)
	short := hex.EncodeToString(sum[:])[:12]
	if c := h.cur.Load(); c != nil && c.sum == short {
		return Event{Checksum: short} // unchanged
	}
	spec, err := Parse(raw)
	if err != nil {
		return h.fail(err)
	}
	m, err := Compile(spec)
	if err != nil {
		return h.fail(err)
	}
	h.cur.Store(&loaded{m: m, sum: short, loadedAt: time.Now()})
	h.reloads.Add(1)
	return Event{Reloaded: true, Name: m.name, Checksum: short}
}

func (h *Holder) fail(err error) Event {
	h.failures.Add(1)
	return Event{Err: err}
}

// Current returns the mapping in use.
func (h *Holder) Current() *Mapper { return h.cur.Load().m }

// Checksum identifies the file content in use.
func (h *Holder) Checksum() string { return h.cur.Load().sum }

// Reloads and Failures count successful and rejected reloads (the first load is a reload).
func (h *Holder) Reloads() uint64  { return h.reloads.Load() }
func (h *Holder) Failures() uint64 { return h.failures.Load() }

// Apply translates with the current mapping.
func (h *Holder) Apply(in []byte) ([]byte, string, error) { return h.Current().Apply(in) }

// Watch polls the file until ctx ends and calls on for every reload or rejection.
func (h *Holder) Watch(ctx context.Context, every time.Duration, on func(Event)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ev := h.reload(); (ev.Reloaded || ev.Err != nil) && on != nil {
				on(ev)
			}
		}
	}
}
