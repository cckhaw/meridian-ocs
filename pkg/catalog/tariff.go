package catalog

import (
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

// PlanResolver says which price plan a subscriber is on.
type PlanResolver interface {
	PlanFor(subscriberID string) (planID string, ok bool)
}

// MapResolver is a fixed subscriber -> plan table, for tests and tools.
type MapResolver map[string]string

func (m MapResolver) PlanFor(id string) (string, bool) { p, ok := m[id]; return p, ok }

// Tariff adapts a Store to the rating engine's Tariff interface. It prices at
// the time the request is processed, using the catalog version then in effect;
// it never blocks on a publish.
type Tariff struct {
	Store *Store
	// Resolver maps a subscriber to a plan. When nil or when it has no answer,
	// DefaultPlan is used. If that is empty too the usage is refused.
	Resolver    PlanResolver
	DefaultPlan string
	// Now supplies the processing time; nil means time.Now.
	Now func() time.Time
}

var _ engine.Tariff = (*Tariff)(nil)

// Rates implements engine.Tariff.
func (t *Tariff) Rates(req *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, error) {
	now := time.Now
	if t.Now != nil {
		now = t.Now
	}
	at := now()
	sn := t.Store.At(at)
	if sn == nil {
		return dst, fmt.Errorf("%w: %w", engine.ErrNoTariff, ErrNoVersion)
	}
	plan := t.DefaultPlan
	if t.Resolver != nil {
		if p, ok := t.Resolver.PlanFor(req.SubscriberID); ok {
			plan = p
		}
	}
	out, _, err := sn.Rates(plan, at, req, u, dst)
	if err != nil {
		return dst, fmt.Errorf("%w: %w", engine.ErrNoTariff, err)
	}
	return out, nil
}

// RatesAt prices as of a past or future instant (re-rating of offline usage,
// "what will this cost on the 1st"). It returns ErrNoVersion when t precedes
// every retained version.
func (t *Tariff) RatesAt(at time.Time, planID string, req *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, Match, error) {
	sn := t.Store.At(at)
	if sn == nil {
		return dst, Match{}, ErrNoVersion
	}
	return sn.Rates(planID, at, req, u, dst)
}
