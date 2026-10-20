package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const bundle = int64(10) << 30

// fakeSystem stands in for the gateway and provisioning: a bundle per subscriber, charged by reserving
// on create, settling on update and release. mode injects the faults the verification must catch.
type fakeSystem struct {
	mode  string // "", "nodebit", "doubledebit", "hang", "slow"
	mu    sync.Mutex
	bal   map[string]int64
	imsi  map[string]string
	held  map[string]*fakeSession
	reqs  atomic.Int64
	creat atomic.Int64
	n     atomic.Int64
}

type fakeSession struct {
	key  string
	held int64
}

func newFake(mode string) *fakeSystem {
	return &fakeSystem{mode: mode, bal: map[string]int64{}, imsi: map[string]string{}, held: map[string]*fakeSession{}}
}

func (f *fakeSystem) prov() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/subscribers", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			SubscriberKey string `json:"subscriberKey"`
			Identifiers   []struct {
				Value string `json:"value"`
			} `json:"identifiers"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		f.mu.Lock()
		f.imsi[in.Identifiers[0].Value] = in.SubscriberKey
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"commandId":%q,"state":"accepted"}`, r.Header.Get("Idempotency-Key"))
	})
	mux.HandleFunc("POST /v1/subscribers/{key}/offering", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.bal[r.PathValue("key")] = bundle
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"commandId":%q}`, r.Header.Get("Idempotency-Key"))
	})
	mux.HandleFunc("GET /v1/commands/{id}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"state":"applied"}`)
	})
	mux.HandleFunc("GET /v1/subscribers/{key}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		b, ok := f.bal[r.PathValue("key")]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, `{"buckets":[{"id":"data10g","balance":%d}]}`, b)
	})
	return mux
}

type fakeReq struct {
	Sub string `json:"subscriberIdentifier"`
	TS  string `json:"invocationTimeStamp"`
	PDU struct {
		Info struct {
			ID uint32 `json:"pduSessionID"`
		} `json:"pduSessionInformation"`
	} `json:"pDUSessionChargingInformation"`
	MUU []struct {
		Req *struct {
			Total int64 `json:"totalVolume"`
		} `json:"requestedUnit"`
		Used []struct {
			Total int64 `json:"totalVolume"`
		} `json:"usedUnitContainer"`
	} `json:"multipleUnitUsage"`
}

func (f *fakeSystem) nchf() http.Handler {
	answer := func(w http.ResponseWriter, status int, grant int64) {
		code := "SUCCESS"
		if grant == 0 {
			code = "QUOTA_LIMIT_REACHED"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if grant > 0 {
			fmt.Fprintf(w, `{"multipleUnitInformation":[{"ratingGroup":100,"resultCode":%q,"grantedUnit":{"totalVolume":%d}}]}`, code, grant)
		} else {
			fmt.Fprintf(w, `{"multipleUnitInformation":[{"ratingGroup":100,"resultCode":%q}]}`, code)
		}
	}
	parse := func(r *http.Request) (fakeReq, int64, int64) {
		var q fakeReq
		_ = json.NewDecoder(r.Body).Decode(&q)
		var want, used int64
		if len(q.MUU) > 0 {
			if q.MUU[0].Req != nil {
				want = q.MUU[0].Req.Total
			}
			for _, u := range q.MUU[0].Used {
				used += u.Total
			}
		}
		return q, want, used
	}
	reserve := func(key string, want int64) int64 { // caller holds f.mu
		g := min(want, f.bal[key])
		if f.mode != "nodebit" {
			f.bal[key] -= g
		}
		return g
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nchf-convergedcharging/v3/chargingdata", func(w http.ResponseWriter, r *http.Request) {
		f.reqs.Add(1)
		q, want, _ := parse(r)
		if f.mode == "slow" {
			time.Sleep(150 * time.Millisecond)
		}
		hang := f.mode == "hang" && f.creat.Add(1)%3 == 0
		f.mu.Lock()
		key := f.imsi[strings.TrimPrefix(q.Sub, "imsi-")]
		g := reserve(key, want)
		// Like the gateway, derive the reference from the create request: an identical request is the same
		// session. ("samesession" breaks that on purpose: every create of a subscriber maps to one reference.)
		id := q.Sub + "|" + q.TS + "|" + fmt.Sprint(q.PDU.Info.ID)
		if f.mode == "samesession" {
			id = q.Sub
		}
		sum := sha256.Sum256([]byte(id))
		ref := "r1." + hex.EncodeToString(sum[:8])
		if old, ok := f.held[ref]; ok { // a retransmission: replay, reserve nothing
			if f.mode != "nodebit" {
				f.bal[key] += g // give back what the duplicate reserved
			}
			g = old.held
		} else {
			f.held[ref] = &fakeSession{key, g}
		}
		f.mu.Unlock()
		if hang {
			time.Sleep(300 * time.Millisecond) // longer than the client's timeout
		}
		w.Header().Set("Location", "http://"+r.Host+"/nchf-convergedcharging/v3/chargingdata/"+ref)
		answer(w, http.StatusCreated, g)
	})
	mux.HandleFunc("POST /nchf-convergedcharging/v3/chargingdata/{ref}/update", func(w http.ResponseWriter, r *http.Request) {
		f.reqs.Add(1)
		_, want, used := parse(r)
		f.mu.Lock()
		s := f.held[r.PathValue("ref")]
		f.bal[s.key] += s.held - used
		s.held = reserve(s.key, want)
		g := s.held
		f.mu.Unlock()
		answer(w, http.StatusOK, g)
	})
	mux.HandleFunc("POST /nchf-convergedcharging/v3/chargingdata/{ref}/release", func(w http.ResponseWriter, r *http.Request) {
		f.reqs.Add(1)
		_, _, used := parse(r)
		f.mu.Lock()
		s := f.held[r.PathValue("ref")]
		f.bal[s.key] += s.held - used
		if f.mode == "doubledebit" {
			f.bal[s.key] -= used
		}
		s.held = 0
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	return h2c.NewHandler(mux, &http2.Server{})
}

func (f *fakeSystem) admin() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "# HELP x\nocs_engine_requests_total %d\n", f.reqs.Load())
	})
}

func testConfig(nchf, prov, admin string) config {
	return config{
		Nchf: nchf, Prov: prov, GatewayAdmin: admin,
		Subscribers: 20, SeedConcurrency: 8, Offering: "gold-offer", Bucket: "data10g", RatingGroup: 100,
		Rate: 400, Duration: time.Second, Warmup: 200 * time.Millisecond, Conns: 2, MaxInflight: 5000,
		Timeout: 2 * time.Second, RequestSize: 1_000_000, UsedPercent: 80, Updates: 1, Seed: 7,
	}
}

func exercise(t *testing.T, f *fakeSystem, edit func(*config)) (*Report, string) {
	t.Helper()
	n := httptest.NewServer(f.nchf())
	p := httptest.NewServer(f.prov())
	a := httptest.NewServer(f.admin())
	t.Cleanup(func() { n.Close(); p.Close(); a.Close() })
	c := testConfig(n.URL, p.URL, a.URL)
	if edit != nil {
		edit(&c)
	}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	rp, err := execute(context.Background(), c, &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	return rp, out.String()
}

func TestRunConservesMoneyAndReportsHonestly(t *testing.T) {
	rp, out := exercise(t, newFake(""), nil)
	if !rp.Pass {
		t.Fatalf("a correct system must pass:\n%s", out)
	}
	if rp.Sessions["completed"] < 200 || rp.Sessions["failed"] != 0 || rp.Sessions["dropped"] != 0 {
		t.Fatalf("unexpected sessions %v", rp.Sessions)
	}
	if rp.Verification.Verdicts[verdictExact] != 20 {
		t.Fatalf("every balance should reconcile exactly: %v", rp.Verification.Verdicts)
	}
	if rp.Latency["create"].Count == 0 || rp.Latency["update"].Count == 0 || rp.Latency["release"].Count == 0 {
		t.Fatalf("latencies were not recorded: %+v", rp.Latency)
	}
	if rp.GatewayCounted == nil || uint64(*rp.GatewayCounted) != rp.ClientSent {
		t.Fatalf("the gateway counted %v requests, the client sent %d", rp.GatewayCounted, rp.ClientSent)
	}
	// The achieved rate must be close to the offered rate when nothing was dropped.
	if rp.AchievedSessionsPS < 300 || rp.AchievedSessionsPS > 500 {
		t.Fatalf("achieved %.0f sessions/s for 400 offered", rp.AchievedSessionsPS)
	}
}

// The mutation checks: a system that breaks conservation must fail the run.
func TestVerificationCatchesValueCreatedFromNothing(t *testing.T) {
	rp, out := exercise(t, newFake("nodebit"), nil)
	if rp.Pass || rp.Verification.Verdicts[verdictCreated] == 0 {
		t.Fatalf("grants that are not debited must be reported:\n%s", out)
	}
}

func TestVerificationCatchesDoubleDebit(t *testing.T) {
	rp, out := exercise(t, newFake("doubledebit"), nil)
	if rp.Pass || rp.Verification.Verdicts[verdictOverspend] == 0 {
		t.Fatalf("a double debit must be reported:\n%s", out)
	}
}

func TestUnknownOutcomesWidenTheBoundsButStillHold(t *testing.T) {
	rp, out := exercise(t, newFake("hang"), func(c *config) { c.Timeout = 80 * time.Millisecond })
	if rp.Sessions["timeouts"] == 0 {
		t.Fatalf("the fault did not fire:\n%s", out)
	}
	if !rp.Verification.OK || rp.Verification.Verdicts[verdictWithin] == 0 {
		t.Fatalf("a timed-out create may have reserved quota: balances must be accepted inside the bounds:\n%s", out)
	}
	if rp.Pass != true {
		t.Fatalf("no objective was set, so the run passes:\n%s", out)
	}
}

func TestSpentBundleStaysNonNegative(t *testing.T) {
	rp, out := exercise(t, newFake(""), func(c *config) {
		c.Subscribers, c.RequestSize, c.Rate, c.UsedPercent = 1, 4<<30, 100, 100
	})
	if !rp.Verification.OK {
		t.Fatalf("contention on one balance must not break conservation:\n%s", out)
	}
	if rp.Sessions["denied"] == 0 || rp.Verification.Overflowed != 1 {
		t.Fatalf("a spent bundle must produce denials and be reported as spent:\n%s", out)
	}
}

func TestFallingBehindIsDroppedAndFailsTheObjective(t *testing.T) {
	rp, out := exercise(t, newFake("slow"), func(c *config) {
		c.MaxInflight, c.Rate, c.SLOOK, c.Duration = 2, 100, 0.99, 800*time.Millisecond
	})
	if rp.Sessions["dropped"] == 0 || rp.Pass || len(rp.SLO) == 0 {
		t.Fatalf("dropped arrivals must be reported and fail the objective:\n%s", out)
	}
	if !strings.Contains(out, "OBJECTIVE MISSED") || !strings.Contains(out, "RESULT: FAIL") {
		t.Fatalf("the report must say so:\n%s", out)
	}
}

func TestLatencyObjective(t *testing.T) {
	rp, _ := exercise(t, newFake("slow"), func(c *config) { c.SLOP99 = 10 * time.Millisecond; c.Rate = 20 })
	if rp.Pass {
		t.Fatal("150 ms answers must miss a 10 ms P99 objective")
	}
}

func TestJSONOutput(t *testing.T) {
	path := t.TempDir() + "/r.json"
	_, _ = exercise(t, newFake(""), func(c *config) { c.JSON = path })
	var back Report
	b, err := io.ReadAll(mustOpen(t, path))
	if err != nil || json.Unmarshal(b, &back) != nil || back.Requests == 0 || !back.Pass {
		t.Fatalf("json result unreadable: %v %s", err, b)
	}
}

func mustOpen(t *testing.T, p string) io.Reader {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(b)
}

func TestFlagValidation(t *testing.T) {
	for _, args := range [][]string{{"-subscribers", "0"}, {"-rate", "0"}, {"-used-percent", "101"}, {"-updates", "99"}, {"-slo-completed", "2"}, {"-conns", "0"}} {
		if _, err := parseFlags(args, io.Discard); err == nil {
			t.Errorf("%v should be refused", args)
		}
	}
	if _, err := parseFlags(nil, io.Discard); err != nil {
		t.Errorf("the defaults must be valid: %v", err)
	}
}

func TestConcurrentSessionsOfOneSubscriberAreDistinct(t *testing.T) {
	// Many overlapping sessions on one subscriber, in bursts that share a millisecond: each must still be its
	// own session at the gateway, or the books would not balance.
	rp, out := exercise(t, newFake("slow"), func(c *config) {
		c.Subscribers, c.Rate, c.Duration, c.Warmup = 1, 1000, time.Second, 100*time.Millisecond
		c.UsedPercent, c.RequestSize = 100, 1_000
	})
	if rp.RefCollisions != 0 || !rp.Pass {
		t.Fatalf("look-alike sessions:\n%s", out)
	}
}

func TestLookAlikeSessionsAreDetectedAndInvalidateTheRun(t *testing.T) {
	rp, out := exercise(t, newFake("samesession"), func(c *config) { c.Subscribers = 1 })
	if rp.RefCollisions == 0 || rp.Pass || !strings.Contains(out, "look-alike") {
		t.Fatalf("a gateway that maps different sessions to one reference must invalidate the run:\n%s", out)
	}
}
