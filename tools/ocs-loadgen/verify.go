package main

import (
	"context"
	"fmt"
	"sync"
)

// Verdicts of the conservation check, per subscriber.
const (
	verdictExact     = "exact"      // the balance is exactly the starting balance minus the confirmed usage
	verdictWithin    = "within"     // some requests had an unknown outcome; the balance is inside the bounds they allow
	verdictOverspend = "overspend"  // the balance is lower than any sequence of outcomes can explain: money was taken twice
	verdictCreated   = "created"    // the balance is higher than allowed: value appeared from nowhere
	verdictNegative  = "negative"   // the balance is below zero
	verdictUnsettled = "unsettled"  // off the bounds by no more than the unsettled terminates the gateway counted allow
	verdictUnread    = "unreadable" // the balance could not be read
)

// Verification is the result of comparing every subscriber's balance with what the run did to it.
type Verification struct {
	Checked    int                 `json:"checked"`
	Verdicts   map[string]int      `json:"verdicts"`
	Violations []string            `json:"violations,omitempty"` // the first few, for reading
	Trails     map[string][]string `json:"trails,omitempty"`     // with --audit: the history of each violating subscriber
	Overflowed int                 `json:"subscribersThatSpentTheirBundle"`
	// UnsettledTerminates is how many terminates the gateway itself counted as acknowledged without settling
	// (data tier down). A 204 carries no body, so the client cannot tell which; each one may leave up to one
	// request's worth of usage uncharged or one reservation held. Budget is that, in bytes, and Explained the
	// part of it the observed deviations used.
	UnsettledTerminates float64 `json:"unsettledTerminates"`
	Budget              int64   `json:"unsettledBudget"`
	Explained           int64   `json:"unsettledExplained"`
	OK                  bool    `json:"ok"`
}

// bounds returns the balance interval the run allows for s. The confirmed usage is certain; reservations
// that were granted but not confirmed released, and requests whose outcome is unknown, widen the lower
// edge. The upper edge is exact: nothing can return more than was taken.
func bounds(s *subscriber) (low, high int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	high = s.initial - s.used
	low = high - s.held - s.unsure
	if high < 0 {
		high = 0 // the bundle is spent; the rest of the usage is charged to other buckets
	}
	if low < 0 {
		low = 0
	}
	return low, high
}

func verify(ctx context.Context, p *provClient, c config, subs []*subscriber, unsettled float64) Verification {
	v := Verification{Verdicts: map[string]int{}, OK: true, UnsettledTerminates: unsettled, Budget: int64(unsettled) * c.RequestSize}
	type result struct {
		verdict, note string
		spent         bool
		dev           int64 // how far outside the bounds
		key           string
		trail         []string
	}
	results := make([]result, len(subs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i, s := range subs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			bal, err := p.bucketBalance(ctx, s.key, c.Bucket)
			low, high := bounds(s)
			s.mu.Lock()
			exactWanted := s.held == 0 && s.unsure == 0
			s.mu.Unlock()
			defer func() { results[i].key, results[i].trail = s.key, s.trail }()
			switch {
			case err != nil:
				results[i] = result{verdict: verdictUnread, note: fmt.Sprintf("%s: %v", s.key, err), spent: false}
			case bal < 0:
				results[i] = result{verdict: verdictNegative, note: fmt.Sprintf("%s: balance %d is negative", s.key, bal), spent: false}
			case bal < low:
				results[i] = result{verdict: verdictOverspend, dev: low - bal, note: fmt.Sprintf("%s: balance %d is below the lowest explainable %d (started %d)", s.key, bal, low, s.initial), spent: false}
			case bal > high:
				results[i] = result{verdict: verdictCreated, dev: bal - high, note: fmt.Sprintf("%s: balance %d is above the highest explainable %d (started %d)", s.key, bal, high, s.initial), spent: false}
			case exactWanted && low == high:
				results[i] = result{verdict: verdictExact, spent: bal == 0}
			default:
				results[i] = result{verdict: verdictWithin, spent: bal == 0}
			}
		}()
	}
	wg.Wait()
	for i := range results {
		// Deviations the gateway's own counter of unsettled terminates can account for are not violations;
		// anything beyond that budget is.
		r := &results[i]
		if (r.verdict == verdictOverspend || r.verdict == verdictCreated) && v.Explained+r.dev <= v.Budget {
			v.Explained += r.dev
			r.note = fmt.Sprintf("%s (accounted for by %.0f unsettled terminates)", r.note, unsettled)
			r.verdict = verdictUnsettled
		}
	}
	for _, r := range results {
		v.Checked++
		v.Verdicts[r.verdict]++
		if r.spent {
			v.Overflowed++
		}
		switch r.verdict {
		case verdictExact, verdictWithin, verdictUnsettled:
		default:
			v.OK = false
			if len(v.Violations) < 10 {
				v.Violations = append(v.Violations, r.note)
				if c.Audit {
					if v.Trails == nil {
						v.Trails = map[string][]string{}
					}
					v.Trails[r.key] = r.trail
				}
			}
		}
	}
	return v
}
