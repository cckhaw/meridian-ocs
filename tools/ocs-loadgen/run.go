package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Nchf, Prov, GatewayAdmin string
	NchfTLS, ProvTLS         tlsFiles

	Subscribers     int
	SeedConcurrency int
	Offering        string
	Bucket          string
	RatingGroup     uint32

	Rate        float64 // charging sessions started per second
	Duration    time.Duration
	Warmup      time.Duration
	Conns       int
	MaxInflight int
	Timeout     time.Duration
	RequestSize int64 // bytes asked for in each create and update
	UsedPercent int64 // share of every grant the subscriber reports as used
	Updates     int   // update rounds between create and release
	Seed        int64

	JSON         string
	SLOP99       time.Duration // 0 = not checked
	SLOOK        float64       // minimum share of sessions that completed, 0 = not checked
	NoVerify     bool
	Audit        bool
	Timeline     bool
	DrainTimeout time.Duration
	Hold         time.Duration // how long a session lasts: spread evenly over its updates and its release
}

func (c config) validate() error {
	switch {
	case c.Subscribers < 1 || c.Subscribers > 99999:
		return errors.New("--subscribers must be between 1 and 99999")
	case c.Rate <= 0:
		return errors.New("--rate must be positive")
	case c.Duration <= 0:
		return errors.New("--duration must be positive")
	case c.Warmup < 0:
		return errors.New("--warmup must not be negative")
	case c.Conns < 1:
		return errors.New("--conns must be at least 1")
	case c.MaxInflight < 1:
		return errors.New("--max-inflight must be at least 1")
	case c.RequestSize < 1:
		return errors.New("--request-bytes must be positive")
	case c.UsedPercent < 0 || c.UsedPercent > 100:
		return errors.New("--used-percent must be 0 to 100")
	case c.Updates < 0 || c.Updates > 20:
		return errors.New("--updates must be 0 to 20")
	case c.Hold < 0:
		return errors.New("--hold must not be negative")
	case c.SLOOK < 0 || c.SLOOK > 1:
		return errors.New("--slo-completed must be between 0 and 1")
	case c.Nchf == "" || c.Prov == "":
		return errors.New("--nchf and --prov are required")
	}
	return nil
}

// ---- results ---------------------------------------------------------------------------------

type counters struct {
	started   atomic.Uint64 // sessions that began
	completed atomic.Uint64 // create → … → release all answered as expected
	denied    atomic.Uint64 // the system refused or granted nothing (a valid answer when the balance is spent)
	failed    atomic.Uint64 // an unexpected status, a timeout or a transport error
	dropped   atomic.Uint64 // not started: too many sessions in flight, i.e. the client or system fell behind
	requests  atomic.Uint64 // charging requests sent
	granted   atomic.Int64  // bytes granted
	timeouts  atomic.Uint64

	mu       sync.Mutex
	statuses map[string]uint64
}

func (c *counters) status(s string) {
	c.mu.Lock()
	if c.statuses == nil {
		c.statuses = map[string]uint64{}
	}
	c.statuses[s]++
	c.mu.Unlock()
}

// second is one second of the measured window.
type second struct {
	started, ok, failed atomic.Uint64
	maxCreateUs         atomic.Uint64
}

func (r *runner) slot(due time.Time) *second {
	if len(r.tl) == 0 {
		return nil
	}
	i := int(due.Sub(r.start) / time.Second)
	if i < 0 {
		i = 0
	}
	if i >= len(r.tl) {
		i = len(r.tl) - 1
	}
	return &r.tl[i]
}

type latencies struct {
	create, update, release Hist
	createCorrected         Hist // from the moment the create was due, not sent: includes queueing delay
}

// runner executes the load.
type runner struct {
	c    config
	pool *nchfPool
	subs []*subscriber

	refs       sync.Map      // charging references handed out in this run
	collisions atomic.Uint64 // creates that returned a reference another session already holds
	measuring  atomic.Bool
	start      time.Time // when the measured window began (set before measuring turns on)
	tl         []second  // outcomes per second of the measured window, by when each session was due
	ct         counters  // the whole run
	mt         counters  // the measured window
	lat        latencies
}

func (r *runner) both(f func(*counters)) {
	f(&r.ct)
	if r.measuring.Load() {
		f(&r.mt)
	}
}

type chargeBody struct {
	SubscriberIdentifier string `json:"subscriberIdentifier"`
	NFConsumer           any    `json:"nfConsumerIdentification"`
	InvocationTimeStamp  string `json:"invocationTimeStamp"`
	Sequence             uint32 `json:"invocationSequenceNumber"`
	MultipleUnitUsage    []any  `json:"multipleUnitUsage"`
	PDU                  any    `json:"pDUSessionChargingInformation,omitempty"`
}

func (r *runner) body(imsi string, pdu uint32, seq uint32, request int64, used int64) []byte {
	mu := map[string]any{"ratingGroup": r.c.RatingGroup}
	if request > 0 {
		mu["requestedUnit"] = map[string]any{"totalVolume": request}
	}
	if used > 0 {
		mu["usedUnitContainer"] = []any{map[string]any{"localSequenceNumber": seq - 1, "totalVolume": used}}
	}
	b := chargeBody{
		SubscriberIdentifier: "imsi-" + imsi,
		NFConsumer:           map[string]any{"nodeFunctionality": "SMF", "nFName": "ocs-loadgen"},
		InvocationTimeStamp:  time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Sequence:             seq,
		MultipleUnitUsage:    []any{mu},
	}
	if seq == 1 {
		b.PDU = map[string]any{"pduSessionInformation": map[string]any{"pduSessionID": pdu, "ratType": "NR"}}
	}
	out, _ := json.Marshal(b)
	return out
}

// grantOf extracts the granted total volume of the one rating group, and whether the answer says SUCCESS.
func grantOf(body []byte) (int64, bool) {
	g, ok, _ := grantResult(body)
	return g, ok
}

// resultDenied is what the gateway answers for a unit it refused because the subscriber is barred or the
// data tier is unavailable. A barred subscriber's usage is still settled; with the data tier down it is not.
const resultDenied = "END_USER_SERVICE_DENIED"

func grantResult(body []byte) (granted int64, success bool, code string) {
	var a struct {
		Units []struct {
			Result  string `json:"resultCode"`
			Granted *struct {
				Total int64 `json:"totalVolume"`
			} `json:"grantedUnit"`
		} `json:"multipleUnitInformation"`
	}
	if json.Unmarshal(body, &a) != nil || len(a.Units) == 0 {
		return 0, false, ""
	}
	u := a.Units[0]
	if u.Granted == nil {
		return 0, u.Result == "SUCCESS", u.Result
	}
	return u.Granted.Total, u.Result == "SUCCESS", u.Result
}

func refPath(location string) (string, bool) {
	u, err := url.Parse(location)
	if err != nil || u.Path == "" || !strings.Contains(u.Path, "/chargingdata/") {
		return "", false
	}
	return u.Path, true
}

// session runs one charging session: create, c.Updates updates, release. due is when it was meant to start.
func (r *runner) session(ctx context.Context, due time.Time, s *subscriber) {
	c := r.c
	rec := r.measuring.Load() // a session belongs to the window it started in, however long it takes
	both := func(f func(*counters)) {
		f(&r.ct)
		if rec {
			f(&r.mt)
		}
	}
	both(func(k *counters) { k.started.Add(1) })
	var sec *second
	if rec {
		if sec = r.slot(due); sec != nil {
			sec.started.Add(1)
		}
	}
	tick := func(h *Hist, from time.Time) {
		if rec {
			h.Record(time.Since(from))
		}
	}
	fail := func(what string) {
		both(func(k *counters) { k.failed.Add(1); k.status(what) })
		if sec != nil {
			sec.failed.Add(1)
		}
	}
	call := func(h *Hist, path string, body []byte) reply {
		both(func(k *counters) { k.requests.Add(1) })
		t0 := time.Now()
		rep := r.pool.post(ctx, path, body, c.Timeout)
		if rep.err == nil {
			tick(h, t0)
		}
		return rep
	}

	// Each session gets its own PDU session identifier. The gateway derives the charging reference from the
	// create request, so two sessions of one subscriber that send identical requests are, by design, one
	// session (a retransmission): that must never happen here by accident.
	pdu := 1 + s.pdu.Add(1)%255
	rep := call(&r.lat.create, "/nchf-convergedcharging/v3/chargingdata", r.body(s.imsi, pdu, 1, c.RequestSize, 0))
	if rec && rep.err == nil {
		r.lat.createCorrected.Record(time.Since(due))
		if sec != nil {
			if us := uint64(time.Since(due) / time.Microsecond); us > sec.maxCreateUs.Load() {
				sec.maxCreateUs.Store(us) // approximate under races: a display value
			}
		}
	}
	if rep.err != nil {
		s.note(c.Audit, "create: no answer (%v)", rep.err)
	} else {
		g, _ := grantOf(rep.body)
		s.note(c.Audit, "create: %d granted %d %s", rep.status, g, rep.location)
	}
	if rep.err != nil {
		// Unknown outcome: the reservation may have been made. It is bounded by what was asked.
		s.mu.Lock()
		s.unsure += c.RequestSize
		s.mu.Unlock()
		both(func(k *counters) {
			if errors.Is(rep.err, context.DeadlineExceeded) {
				k.timeouts.Add(1)
			}
		})
		fail("transport")
		return
	}
	both(func(k *counters) { k.status(strconv.Itoa(rep.status)) })
	if rep.status != http.StatusCreated {
		if rep.status >= 500 {
			s.mu.Lock()
			s.unsure += c.RequestSize
			s.mu.Unlock()
		}
		fail(strconv.Itoa(rep.status))
		return
	}
	ref, ok := refPath(rep.location)
	if !ok {
		fail("no_location")
		return
	}
	if _, dup := r.refs.LoadOrStore(ref, struct{}{}); dup {
		// Two of our sessions were treated as one: the run's accounting is no longer valid.
		r.collisions.Add(1)
		fail("duplicate_ref")
		return
	}
	grant, success := grantOf(rep.body)
	both(func(k *counters) { k.granted.Add(grant) })

	pause := func() {
		if c.Hold > 0 {
			time.Sleep(c.Hold / time.Duration(c.Updates+1))
		}
	}
	seq := uint32(1)
	for i := 0; i < c.Updates && grant > 0; i++ {
		pause()
		used := grant * c.UsedPercent / 100
		seq++
		up := call(&r.lat.update, ref+"/update", r.body(s.imsi, pdu, seq, c.RequestSize, used))
		s.note(c.Audit, "update %s seq %d used %d: %d err=%v", ref[strings.LastIndex(ref, "/")+1:], seq, used, up.status, up.err)
		if up.err != nil || up.status != http.StatusOK {
			// The update may or may not have settled the usage and reserved more.
			s.mu.Lock()
			s.held += grant
			s.unsure += c.RequestSize + used
			s.mu.Unlock()
			if up.err != nil {
				fail("transport")
			} else {
				fail(strconv.Itoa(up.status))
			}
			return
		}
		prev := grant
		var code string
		grant, _, code = grantResult(up.body)
		s.mu.Lock()
		if code == resultDenied {
			// Refused: with the data tier down nothing was settled (the previous reservation stays until it
			// expires); for a barred subscriber the usage was settled. The answer does not say which.
			s.unsure += used
			s.held += prev
		} else {
			s.used += used
		}
		s.mu.Unlock()
		both(func(k *counters) { k.granted.Add(grant) })
	}

	pause()
	used := grant * c.UsedPercent / 100
	seq++
	rel := call(&r.lat.release, ref+"/release", r.body(s.imsi, pdu, seq, 0, used))
	s.note(c.Audit, "release %s seq %d used %d: %d err=%v", ref[strings.LastIndex(ref, "/")+1:], seq, used, rel.status, rel.err)
	if rel.err != nil || rel.status != http.StatusNoContent {
		s.mu.Lock()
		s.held += grant
		s.unsure += used
		s.mu.Unlock()
		if rel.err != nil {
			fail("transport")
		} else {
			fail(strconv.Itoa(rel.status))
		}
		return
	}
	s.mu.Lock()
	s.used += used
	s.sessions++
	s.mu.Unlock()
	if sec != nil {
		sec.ok.Add(1)
	}
	if success && grant > 0 {
		both(func(k *counters) { k.completed.Add(1) })
	} else {
		both(func(k *counters) { k.denied.Add(1) })
	}
}

// drive starts sessions at a constant rate (open loop): a slow answer does not delay the next arrival.
func (r *runner) drive(ctx context.Context) (measured time.Duration) {
	rng := rand.New(rand.NewSource(r.c.Seed))
	interval := time.Duration(float64(time.Second) / r.c.Rate)
	if interval < 1 {
		interval = 1
	}
	start := time.Now()
	warmEnd := start.Add(r.c.Warmup)
	end := warmEnd.Add(r.c.Duration)
	var inflight atomic.Int64
	var wg sync.WaitGroup
	next := start
	var measureFrom time.Time
	for {
		now := time.Now()
		if !r.measuring.Load() && !now.Before(warmEnd) {
			measureFrom = now
			r.start = now
			r.tl = make([]second, int(r.c.Duration/time.Second)+2)
			r.measuring.Store(true)
		}
		if !now.Before(end) || ctx.Err() != nil {
			break
		}
		for !next.After(now) && next.Before(end) {
			due := next
			next = next.Add(interval)
			if inflight.Load() >= int64(r.c.MaxInflight) {
				r.both(func(k *counters) { k.dropped.Add(1) })
				continue
			}
			s := r.subs[rng.Intn(len(r.subs))]
			inflight.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer inflight.Add(-1)
				r.session(ctx, due, s)
			}()
		}
		if d := time.Until(next); d > 0 {
			if d > 2*time.Millisecond {
				d = 2 * time.Millisecond
			}
			time.Sleep(d)
		}
	}
	measured = time.Since(measureFrom)
	r.measuring.Store(false) // sessions still running at the end are not measured
	wg.Wait()
	return measured
}

// engineRequests reads the gateway's own request counter, to cross-check what the client believes it sent.
func engineRequests(ctx context.Context, admin string) (float64, error) {
	return scrape(ctx, admin, "ocs_engine_requests_total")
}

// scrapeAll reads every unlabelled sample of a service's /metrics.
func scrapeAll(ctx context.Context, admin string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(admin, "/")+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	out := map[string]float64{}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || line[0] == '#' || strings.ContainsAny(line, "{") {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil {
			out[name] = f
		}
	}
	return out, nil
}

// scrape reads one unlabelled counter from a service's /metrics.
func scrape(ctx context.Context, admin, name string) (float64, error) {
	m, err := scrapeAll(ctx, admin)
	if err != nil {
		return 0, err
	}
	v, ok := m[name]
	if !ok {
		return 0, errors.New(name + " not found")
	}
	return v, nil
}

// gwDelta is how much a counter rose during the run; a gateway that restarted began again at zero.
func gwDelta(before, after map[string]float64, name string) float64 {
	d := after[name] - before[name]
	if d < 0 {
		d = after[name]
	}
	return d
}
