// Package unsettled keeps usage that a gateway released without being able to charge it, and charges it
// once the data tier is back.
//
// When Redis is down a terminate (Gy CCR-T, Nchf release) is still acknowledged so the network element can
// free the session, but the usage it reported cannot be settled. Without a record that usage is lost: the
// reservation expires and is refunded and the subscriber is never charged for it. The data tier cannot hold
// the record (it is the thing that is down), so the gateway writes it to a local, fsynced log (the Spool)
// and a Reconciler replays the original terminate, with the same request number, when the data tier answers.
// The charge script makes (session, request number) idempotent, so a replay of a terminate that in fact
// reached Redis (an unknown outcome) is answered from its stored result and charges nothing twice.
//
// The spool is node-local: it survives a process restart on the same volume, not the loss of the volume.
// Records that cannot be settled (the session no longer exists) are kept as dead letters, never dropped.
package unsettled

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

// Unit is the usage of one rating group.
type Unit struct {
	RatingGroup uint32             `json:"rg"`
	ServiceID   uint32             `json:"sid,omitempty"`
	Kind        canonical.UnitKind `json:"kind"`
	Used        int64              `json:"used"`
}

// Record is a terminate that was acknowledged without being settled: everything needed to submit it again.
type Record struct {
	ID     string    `json:"id"` // subscriber|session|request number: a retransmission is the same record
	At     time.Time `json:"at"` // when it was acknowledged
	Source uint8     `json:"src,omitempty"`
	// Type is the canonical request type of what was acknowledged without being settled: a terminate (the
	// default, and what every record written before updates were recorded is) or a refused update.
	Type uint8 `json:"typ,omitempty"`
	// AcceptUnknownSession is the request's flag of that name (Nchf): replayed, it creates the session if it is gone.
	AcceptUnknownSession bool      `json:"acc,omitempty"`
	SubscriberID         string    `json:"sub"`
	SessionID            string    `json:"ses"`
	RequestNumber        uint32    `json:"no"`
	EventTime            time.Time `json:"evt,omitempty"`
	RATType              string    `json:"rat,omitempty"`
	CellID               string    `json:"cell,omitempty"`
	Slice                string    `json:"slice,omitempty"`
	Units                []Unit    `json:"units"`
}

// FromRequest builds the record of a terminate. Only units that reported usage are kept.
func FromRequest(req *canonical.Request, at time.Time) Record {
	r := Record{
		ID:                   fmt.Sprintf("%s|%s|%d", req.SubscriberID, req.SessionID, req.RequestNumber),
		At:                   at,
		Source:               uint8(req.Source),
		Type:                 uint8(req.Type),
		AcceptUnknownSession: req.AcceptUnknownSession,
		SubscriberID:         req.SubscriberID, SessionID: req.SessionID, RequestNumber: req.RequestNumber,
		EventTime: req.EventTime, RATType: req.RATType, CellID: req.CellID, Slice: req.Slice,
	}
	for _, u := range req.Units {
		if u.Used > 0 {
			r.Units = append(r.Units, Unit{u.RatingGroup, u.ServiceID, u.Kind, u.Used})
		}
	}
	return r
}

// IsUpdate reports whether the record is a refused update (otherwise it is a terminate).
func (r Record) IsUpdate() bool { return canonical.RequestType(r.Type) == canonical.TypeUpdate }

// Request rebuilds what was acknowledged: a terminate, or the update that was refused (asking for nothing:
// its usage is settled after the fact and nothing is reserved). The caller releases it with
// canonical.ReleaseRequest.
func (r Record) Request() *canonical.Request {
	q := canonical.AcquireRequest()
	q.Source, q.Type = canonical.Source(r.Source), canonical.TypeTerminate
	if r.IsUpdate() {
		q.Type = canonical.TypeUpdate
	}
	q.SubscriberID, q.SessionID, q.RequestNumber = r.SubscriberID, r.SessionID, r.RequestNumber
	q.EventTime, q.RATType, q.CellID, q.Slice = r.EventTime, r.RATType, r.CellID, r.Slice
	q.AcceptUnknownSession = r.AcceptUnknownSession
	for _, u := range r.Units {
		q.Units = append(q.Units, canonical.UnitRequest{RatingGroup: u.RatingGroup, ServiceID: u.ServiceID, Kind: u.Kind, Used: u.Used})
	}
	return q
}

// Stats are the spool's current numbers.
type Stats struct {
	Pending   int
	Dead      int
	OldestAge time.Duration // of the oldest pending record, 0 when none
	Recorded  uint64        // records accepted since start
	Corrupt   int           // unreadable lines found at start (kept in a side file)
}

type op struct {
	Op  string  `json:"op"` // add | done | dead
	ID  string  `json:"id,omitempty"`
	Rec *Record `json:"rec,omitempty"`
	Why string  `json:"why,omitempty"`
}

type deadRec struct {
	rec Record
	why string
}

const (
	logName     = "unsettled.log"
	maxDeadKept = 1000
)

// Spool is the durable set of pending records. It is safe for concurrent use.
type Spool struct {
	dir string
	now func() time.Time

	mu       sync.Mutex
	f        *os.File
	pending  map[string]Record
	dead     []deadRec
	lines    int // lines in the log file, to decide when to compact
	recorded uint64
	corrupt  int
	closed   bool
}

// Open opens (creating it if needed) the spool in dir and loads what is pending. A torn final line from a
// crash is ignored; unreadable lines elsewhere are counted and the original file is kept beside the
// compacted one.
func Open(dir string) (*Spool, error) { return open(dir, time.Now) }

func open(dir string, now func() time.Time) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("unsettled spool: %w", err)
	}
	s := &Spool{dir: dir, now: now, pending: map[string]Record{}}
	path := filepath.Join(dir, logName)
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("unsettled spool: %w", err)
	}
	s.load(b)
	if s.corrupt > 0 {
		side := fmt.Sprintf("%s.corrupt-%d", path, now().Unix())
		if err := os.WriteFile(side, b, 0o640); err != nil {
			return nil, fmt.Errorf("unsettled spool: %w", err)
		}
	}
	// Start from a clean file holding only what is live, so the log never grows without bound across restarts.
	if err := s.rewrite(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Spool) load(b []byte) {
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	var lines [][]byte
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			lines = append(lines, append([]byte(nil), sc.Bytes()...))
		}
	}
	for i, ln := range lines {
		var o op
		if err := json.Unmarshal(ln, &o); err != nil || !validOp(o) {
			// A torn last line is what a crash during an append leaves behind: expected, not corruption.
			if i == len(lines)-1 && !bytes.HasSuffix(b, []byte("\n")) {
				continue
			}
			s.corrupt++
			continue
		}
		switch o.Op {
		case "add":
			s.pending[o.Rec.ID] = *o.Rec
		case "done":
			delete(s.pending, o.ID)
		case "dead":
			if o.Rec != nil {
				delete(s.pending, o.Rec.ID)
				s.dead = append(s.dead, deadRec{*o.Rec, o.Why})
			}
		}
	}
	s.trimDead()
}

func validOp(o op) bool {
	switch o.Op {
	case "add":
		return o.Rec != nil && o.Rec.ID != ""
	case "done":
		return o.ID != ""
	case "dead":
		return o.Rec != nil && o.Rec.ID != ""
	}
	return false
}

func (s *Spool) trimDead() {
	if len(s.dead) > maxDeadKept {
		s.dead = s.dead[len(s.dead)-maxDeadKept:]
	}
}

// rewrite replaces the log with one holding exactly the live state (atomic rename, directory fsync).
func (s *Spool) rewrite() error {
	path := filepath.Join(s.dir, logName)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	w := bufio.NewWriter(f)
	n := 0
	emit := func(o op) error {
		b, _ := json.Marshal(o)
		n++
		_, err := w.Write(append(b, '\n'))
		return err
	}
	for _, r := range s.sortedPending() {
		r := r
		if err := emit(op{Op: "add", Rec: &r}); err != nil {
			_ = f.Close()
			return err
		}
	}
	for _, d := range s.dead {
		d := d
		if err := emit(op{Op: "dead", Rec: &d.rec, Why: d.why}); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err == nil {
		err = f.Sync()
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	nf, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	if s.f != nil {
		_ = s.f.Close()
	}
	s.f, s.lines = nf, n
	return nil
}

func (s *Spool) sortedPending() []Record {
	out := make([]Record, 0, len(s.pending))
	for _, r := range s.pending {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].At.Equal(out[j].At) {
			return out[i].At.Before(out[j].At)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// append writes one operation and makes it durable before returning.
func (s *Spool) append(o op) error {
	if s.closed {
		return errors.New("unsettled spool: closed")
	}
	b, _ := json.Marshal(o)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("unsettled spool: %w", err)
	}
	s.lines++
	return nil
}

// Record stores a terminate (or a refused update) that was acknowledged without being settled. It returns only after the record is
// on disk; an error means it is NOT safe and the caller must count the usage as lost. A repeat of a record
// already pending (the network element retransmitting the terminate) is accepted without a second entry.
func (s *Spool) Record(req *canonical.Request) error {
	rec := FromRequest(req, s.now())
	if len(rec.Units) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pending[rec.ID]; ok {
		return nil
	}
	if err := s.append(op{Op: "add", Rec: &rec}); err != nil {
		return err
	}
	s.pending[rec.ID] = rec
	s.recorded++
	return nil
}

// Pending returns up to max records, oldest first.
func (s *Spool) Pending(max int) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := s.sortedPending()
	if max > 0 && len(all) > max {
		all = all[:max]
	}
	return all
}

// Done removes a record that has been settled.
func (s *Spool) Done(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pending[id]; !ok {
		return nil
	}
	if err := s.append(op{Op: "done", ID: id}); err != nil {
		return err
	}
	delete(s.pending, id)
	return s.maybeCompact()
}

// Dead moves a record that can never be settled (with the reason) out of the pending set. It stays on disk.
func (s *Spool) Dead(id, why string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.pending[id]
	if !ok {
		return nil
	}
	if err := s.append(op{Op: "dead", Rec: &rec, Why: why}); err != nil {
		return err
	}
	delete(s.pending, id)
	s.dead = append(s.dead, deadRec{rec, why})
	s.trimDead()
	return s.maybeCompact()
}

func (s *Spool) maybeCompact() error {
	live := len(s.pending) + len(s.dead)
	if s.lines > 1000 && s.lines > 4*(live+1) {
		return s.rewrite()
	}
	return nil
}

// DeadLetters returns what could not be settled, with the reasons.
func (s *Spool) DeadLetters() (recs []Record, why []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.dead {
		recs, why = append(recs, d.rec), append(why, d.why)
	}
	return recs, why
}

// Stats returns the current numbers.
func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Pending: len(s.pending), Dead: len(s.dead), Recorded: s.recorded, Corrupt: s.corrupt}
	for _, r := range s.pending {
		if age := s.now().Sub(r.At); age > st.OldestAge {
			st.OldestAge = age
		}
	}
	return st
}

// Close flushes and closes the log.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}
