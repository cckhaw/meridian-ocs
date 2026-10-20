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
	verdictUnread    = "unreadable" // the balance could not be read
)

// Verification is the result of comparing every subscriber's balance with what the run did to it.
type Verification struct {
	Checked    int                 `json:"checked"`
	Verdicts   map[string]int      `json:"verdicts"`
	Violations []string            `json:"violations,omitempty"` // the first few, for reading
	Trails     map[string][]string `json:"trails,omitempty"`     // with --audit: the history of each violating subscriber
	Overflowed int                 `json:"subscribersThatSpentTheirBundle"`
	OK         bool                `json:"ok"`
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

func verify(ctx context.Context, p *provClient, c config, subs []*subscriber) Verification {
	v := Verification{Verdicts: map[string]int{}, OK: true}
	type result struct {
		verdict, note string
		spent         bool
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
				results[i] = result{verdict: verdictOverspend, note: fmt.Sprintf("%s: balance %d is below the lowest explainable %d (started %d)", s.key, bal, low, s.initial), spent: false}
			case bal > high:
				results[i] = result{verdict: verdictCreated, note: fmt.Sprintf("%s: balance %d is above the highest explainable %d (started %d)", s.key, bal, high, s.initial), spent: false}
			case exactWanted && low == high:
				results[i] = result{verdict: verdictExact, spent: bal == 0}
			default:
				results[i] = result{verdict: verdictWithin, spent: bal == 0}
			}
		}()
	}
	wg.Wait()
	for _, r := range results {
		v.Checked++
		v.Verdicts[r.verdict]++
		if r.spent {
			v.Overflowed++
		}
		switch r.verdict {
		case verdictExact, verdictWithin:
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
