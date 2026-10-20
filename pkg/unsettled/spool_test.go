package unsettled

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

func term(sub, sid string, no uint32, used int64) *canonical.Request {
	r := canonical.AcquireRequest()
	r.Type, r.Source, r.SubscriberID, r.SessionID, r.RequestNumber = canonical.TypeTerminate, canonical.SourceNchf, sub, sid, no
	r.EventTime = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	r.RATType, r.CellID, r.Slice = "NR", "cell-9", "sst1"
	r.Units = append(r.Units,
		canonical.UnitRequest{RatingGroup: 100, ServiceID: 7, Kind: canonical.UnitBytes, Requested: 5, Used: used},
		canonical.UnitRequest{RatingGroup: 200, Kind: canonical.UnitSeconds, Used: 0}) // no usage: not kept
	return r
}

func TestRecordSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Record(term("u1", "s1", 3, 700)); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(term("u2", "s2", 1, 50)); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := s.Pending(0)
	if len(got) != 2 || got[0].SubscriberID != "u1" || got[1].SubscriberID != "u2" {
		t.Fatalf("pending after restart: %+v", got)
	}
	r := got[0]
	if len(r.Units) != 1 || r.Units[0].Used != 700 || r.Units[0].RatingGroup != 100 || r.Units[0].ServiceID != 7 {
		t.Fatalf("only the unit with usage is kept, in full: %+v", r.Units)
	}
	// The rebuilt request is the one that was acknowledged: same session, same request number, same attributes.
	q := r.Request()
	defer canonical.ReleaseRequest(q)
	if q.Type != canonical.TypeTerminate || q.Source != canonical.SourceNchf || q.SubscriberID != "u1" || q.SessionID != "s1" ||
		q.RequestNumber != 3 || q.RATType != "NR" || q.CellID != "cell-9" || q.Slice != "sst1" || !q.EventTime.Equal(term("", "", 0, 0).EventTime) ||
		len(q.Units) != 1 || q.Units[0].Used != 700 || q.Units[0].Kind != canonical.UnitBytes {
		t.Fatalf("round trip lost something: %+v", q)
	}
}

func TestDoneAndDeadSurviveARestartAndRetransmissionsAreOneRecord(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	for i := 0; i < 3; i++ { // the network element retransmits the terminate during the outage
		if err := s.Record(term("u1", "s1", 3, 700)); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Record(term("u2", "s2", 1, 50))
	_ = s.Record(term("u3", "s3", 1, 60))
	if st := s.Stats(); st.Pending != 3 || st.Recorded != 3 {
		t.Fatalf("a retransmission must not be a second record: %+v", st)
	}
	if err := s.Done("u1|s1|3"); err != nil {
		t.Fatal(err)
	}
	if err := s.Dead("u2|s2|1", "session unknown"); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s, _ = Open(dir)
	defer s.Close()
	p := s.Pending(0)
	if len(p) != 1 || p[0].SubscriberID != "u3" {
		t.Fatalf("pending: %+v", p)
	}
	recs, why := s.DeadLetters()
	if len(recs) != 1 || recs[0].SubscriberID != "u2" || why[0] != "session unknown" {
		t.Fatalf("a dead letter must be kept with its reason: %+v %v", recs, why)
	}
}

func TestATornLastLineFromACrashIsIgnored(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	_ = s.Record(term("u1", "s1", 3, 700))
	_ = s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, logName), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString(`{"op":"add","rec":{"id":"u9|s9|1","at":"2026-10-20T12:00:00Z","sub":"u9","ses":"s9","no":1,"units":[{"rg":1,"ki`) // cut mid-write
	_ = f.Close()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if st := s.Stats(); st.Pending != 1 || st.Corrupt != 0 {
		t.Fatalf("a torn tail is expected after a crash and is not corruption: %+v", st)
	}
	if ents, _ := filepath.Glob(filepath.Join(dir, "*.corrupt-*")); len(ents) != 0 {
		t.Fatalf("no side file expected: %v", ents)
	}
}

func TestUnreadableLinesAreCountedAndTheOriginalKept(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	_ = s.Record(term("u1", "s1", 3, 700))
	_ = s.Close()
	path := filepath.Join(dir, logName)
	b, _ := os.ReadFile(path)
	lines := strings.SplitN(string(b), "\n", 2)
	damaged := "not json at all\n" + lines[0] + "\n" + `{"op":"add"}` + "\n"
	if err := os.WriteFile(path, []byte(damaged), 0o640); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if st := s.Stats(); st.Pending != 1 || st.Corrupt != 2 {
		t.Fatalf("the good record must survive and the bad lines be counted: %+v", st)
	}
	side, _ := filepath.Glob(filepath.Join(dir, "*.corrupt-*"))
	if len(side) != 1 {
		t.Fatalf("the damaged original must be kept for inspection: %v", side)
	}
	if kept, _ := os.ReadFile(side[0]); string(kept) != damaged {
		t.Fatal("the side file is not the original")
	}
}

func TestTheLogIsCompactedAndStaysSmall(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	for i := 0; i < 3000; i++ {
		id := fmt.Sprintf("u%d|s|1", i)
		q := term(fmt.Sprintf("u%d", i), "s", 1, 10)
		if err := s.Record(q); err != nil {
			t.Fatal(err)
		}
		canonical.ReleaseRequest(q)
		if err := s.Done(id); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := os.ReadFile(filepath.Join(dir, logName))
	if n := strings.Count(string(b), "\n"); n > 2200 {
		t.Fatalf("the log kept growing: %d lines for nothing pending", n)
	}
	if s.Stats().Pending != 0 {
		t.Fatal("everything was settled")
	}
	// ... and it still works after compacting.
	if err := s.Record(term("late", "s", 1, 5)); err != nil || s.Stats().Pending != 1 {
		t.Fatalf("record after compaction: %v %+v", err, s.Stats())
	}
}

func TestOldestAgeAndOrder(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	cur := now
	s, err := open(t.TempDir(), func() time.Time { return cur })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.Record(term("b", "s", 1, 5))
	cur = now.Add(30 * time.Second)
	_ = s.Record(term("a", "s", 1, 5))
	cur = now.Add(90 * time.Second)
	if st := s.Stats(); st.OldestAge != 90*time.Second {
		t.Fatalf("oldest age %v", st.OldestAge)
	}
	if p := s.Pending(1); len(p) != 1 || p[0].SubscriberID != "b" {
		t.Fatalf("oldest first, capped: %+v", p)
	}
}

func TestRecordFailsLoudlyWhenTheDiskDoes(t *testing.T) {
	s, _ := Open(t.TempDir())
	_ = s.Close() // a closed log stands in for a failed write
	if err := s.Record(term("u1", "s1", 1, 5)); err == nil {
		t.Fatal("a record that is not safe on disk must be reported as an error")
	}
}

func TestZeroUsageIsNotRecorded(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	q := term("u1", "s1", 1, 0)
	if err := s.Record(q); err != nil || s.Stats().Pending != 0 {
		t.Fatalf("%v %+v", err, s.Stats())
	}
}

func TestConcurrentRecords(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Record(term(fmt.Sprintf("u%d", i), "s", 1, 10)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if s.Stats().Pending != 50 {
		t.Fatalf("%+v", s.Stats())
	}
}

// ---- reconciler ---------------------------------------------------------------------------------

type fakeSettler struct {
	mu     sync.Mutex
	err    error
	result canonical.Result
	calls  []string
}

func (f *fakeSettler) Settle(_ context.Context, req *canonical.Request) (canonical.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("%s/%s/%d/%d", req.SubscriberID, req.SessionID, req.RequestNumber, req.Units[0].Used))
	if f.err != nil {
		return 0, f.err
	}
	return f.result, nil
}

func TestPassSettlesInOrderAndStopsAtTheFirstError(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	_ = s.Record(term("a", "s", 1, 10))
	_ = s.Record(term("b", "s", 1, 20))
	f := &fakeSettler{err: errors.New("redis down")}
	r := &Reconciler{Spool: s, Settler: f}
	n, err := r.Pass(context.Background())
	if err == nil || n != 0 || s.Stats().Pending != 2 || len(f.calls) != 1 {
		t.Fatalf("an unavailable data tier must leave everything pending and stop at the first record: n=%d err=%v calls=%v", n, err, f.calls)
	}
	f.err, f.calls = nil, nil
	n, err = r.Pass(context.Background())
	if err != nil || n != 2 || s.Stats().Pending != 0 || fmt.Sprint(f.calls) != "[a/s/1/10 b/s/1/20]" {
		t.Fatalf("n=%d err=%v calls=%v", n, err, f.calls)
	}
	if st := r.Stats(); st.Settled != 2 || st.Deferred != 1 {
		t.Fatalf("%+v", st)
	}
}

func TestPassMakesGoneSessionsDeadLettersNotRetries(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	_ = s.Record(term("a", "s", 1, 10))
	f := &fakeSettler{result: canonical.ResultUnknownSession}
	var logged []string
	r := &Reconciler{Spool: s, Settler: f, Logf: func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }}
	if _, err := r.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Pending != 0 || st.Dead != 1 || r.Stats().Dead != 1 || len(logged) != 1 {
		t.Fatalf("%+v %+v %v", st, r.Stats(), logged)
	}
	if _, err := r.Pass(context.Background()); err != nil || len(f.calls) != 1 {
		t.Fatal("a dead letter must not be tried again")
	}
}

func TestRunDrainsOnceTheDataTierReturns(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	_ = s.Record(term("a", "s", 1, 10))
	f := &fakeSettler{err: errors.New("down")}
	r := &Reconciler{Spool: s, Settler: f, Interval: 5 * time.Millisecond, Backoff: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	time.Sleep(60 * time.Millisecond)
	if s.Stats().Pending != 1 {
		t.Fatal("must stay pending while the data tier is down")
	}
	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for s.Stats().Pending != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if s.Stats().Pending != 0 {
		t.Fatal("not drained after the data tier came back")
	}
}

func TestAnUpdateRecordKeepsItsType(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	u := term("u1", "s1", 4, 900)
	u.Type = canonical.TypeUpdate
	u.Units[0].Requested = 123456 // an update asked for more; the late settlement reserves nothing
	if err := s.Record(u); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s, _ = Open(dir)
	defer s.Close()
	rec := s.Pending(0)[0]
	if !rec.IsUpdate() {
		t.Fatalf("%+v", rec)
	}
	q := rec.Request()
	defer canonical.ReleaseRequest(q)
	if q.Type != canonical.TypeUpdate || q.Units[0].Used != 900 || q.Units[0].Requested != 0 || q.RequestNumber != 4 {
		t.Fatalf("a refused update is replayed asking for nothing: %+v", q)
	}
}

func TestRecordsWrittenBeforeUpdatesWereRecordedAreTerminates(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"op":"add","rec":{"id":"u1|s1|3","at":"2026-10-20T12:00:00Z","sub":"u1","ses":"s1","no":3,"units":[{"rg":100,"kind":0,"used":700}]}}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, logName), []byte(legacy), 0o640); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec := s.Pending(0)[0]
	q := rec.Request()
	defer canonical.ReleaseRequest(q)
	if rec.IsUpdate() || q.Type != canonical.TypeTerminate {
		t.Fatalf("a record without a type is a terminate: %+v", q)
	}
}
