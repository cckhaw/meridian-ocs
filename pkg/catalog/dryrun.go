package catalog

import (
	"errors"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

// MaxDryRunCases bounds one dry-run call.
const MaxDryRunCases = 1000

// Case is one synthetic rating check.
type Case struct {
	Name   string `json:"name,omitempty"`
	PlanID string `json:"planId"`
	// At is the instant to price at (it selects the live version and the
	// time-of-day variables). Zero means now.
	At          time.Time `json:"at,omitempty"`
	RAT         string    `json:"rat,omitempty"`
	Slice       string    `json:"slice,omitempty"`
	Cell        string    `json:"cell,omitempty"`
	RatingGroup uint32    `json:"ratingGroup,omitempty"`
	ServiceID   uint32    `json:"serviceId,omitempty"`
	Unit        string    `json:"unit,omitempty"` // "bytes" (default), "seconds", "events"
}

// RateView is a rate in API form.
type RateView struct {
	Class string `json:"class"`
	Num   int64  `json:"num"`
	Den   int64  `json:"den"`
}

// Outcome is what one catalog version does with a case.
type Outcome struct {
	Version  int64      `json:"version,omitempty"`
	Rates    []RateView `json:"rates,omitempty"`
	RuleID   string     `json:"ruleId,omitempty"`
	FilterID string     `json:"filterId,omitempty"`
	// Error is "" when priced, else "barred", "no_rule", "unknown_plan" or
	// "no_version".
	Error string `json:"error,omitempty"`
}

// CaseResult compares the version in force with the proposed one.
type CaseResult struct {
	Case     Case    `json:"case"`
	Live     Outcome `json:"live"`
	Proposed Outcome `json:"proposed"`
	Changed  bool    `json:"changed"`
}

// DryRun prices every case under the version of live in effect at the case's
// time (nil live means none) and under proposed, without publishing anything.
func DryRun(live *Store, proposed *Snapshot, cases []Case, now time.Time) ([]CaseResult, error) {
	if len(cases) > MaxDryRunCases {
		return nil, errors.New("catalog: too many dry-run cases")
	}
	out := make([]CaseResult, len(cases))
	for i, c := range cases {
		at := c.At
		if at.IsZero() {
			at = now
		}
		req := canonical.Request{RATType: c.RAT, Slice: c.Slice, CellID: c.Cell}
		kind, ok := unitKind(c.Unit)
		if c.Unit == "" {
			kind, ok = canonical.UnitBytes, true
		}
		r := CaseResult{Case: c}
		if !ok {
			r.Live = Outcome{Error: "invalid_unit"}
			r.Proposed = r.Live
			out[i] = r
			continue
		}
		u := canonical.UnitRequest{RatingGroup: c.RatingGroup, ServiceID: c.ServiceID, Kind: kind}
		var base *Snapshot
		if live != nil {
			base = live.At(at)
		}
		r.Live = evaluate(base, c.PlanID, at, &req, &u)
		r.Proposed = evaluate(proposed, c.PlanID, at, &req, &u)
		r.Changed = !sameOutcome(r.Live, r.Proposed)
		out[i] = r
	}
	return out, nil
}

func evaluate(sn *Snapshot, plan string, at time.Time, req *canonical.Request, u *canonical.UnitRequest) Outcome {
	if sn == nil {
		return Outcome{Error: "no_version"}
	}
	rates, m, err := sn.Rates(plan, at, req, u, nil)
	o := Outcome{Version: sn.Version, RuleID: m.RuleID, FilterID: m.FilterID}
	switch {
	case errors.Is(err, ErrBarred):
		o.Error = "barred"
	case errors.Is(err, ErrNoRule):
		o.Error = "no_rule"
	case errors.Is(err, ErrUnknownPlan):
		o.Error = "unknown_plan"
	}
	for _, rt := range rates {
		o.Rates = append(o.Rates, RateView{Class: string(rt.Class), Num: rt.Num, Den: rt.Den})
	}
	return o
}

// sameOutcome ignores the version number: only the pricing matters.
func sameOutcome(a, b Outcome) bool {
	if a.Error != b.Error || len(a.Rates) != len(b.Rates) {
		return false
	}
	for i := range a.Rates {
		if a.Rates[i] != b.Rates[i] {
			return false
		}
	}
	return true
}
