package catalog

import (
	"errors"
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
)

// ErrNoPlan means the subscriber has no plan of its own: the default plan applies.
var ErrNoPlan = errors.New("catalog: subscriber has no plan assigned")

// PlanResolver says which price plan a subscriber is on at a given time.
// ErrNoPlan selects the default plan; any other error means the answer is
// unknown and the usage must not be priced on a guess.
type PlanResolver interface {
	PlanFor(subscriberID string, at time.Time) (planID string, err error)
}

// MapResolver is a fixed subscriber -> plan table, for tests and tools.
type MapResolver map[string]string

// PlanFor implements PlanResolver.
func (m MapResolver) PlanFor(id string, _ time.Time) (string, error) {
	if p, ok := m[id]; ok {
		return p, nil
	}
	return "", ErrNoPlan
}

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
	plan, err := t.planFor(req.SubscriberID, at)
	if err != nil {
		return dst, fmt.Errorf("%w: %w", engine.ErrNoTariff, err)
	}
	out, _, err := sn.Rates(plan, at, req, u, dst)
	if err != nil {
		return dst, fmt.Errorf("%w: %w", engine.ErrNoTariff, err)
	}
	return out, nil
}

// planFor picks the subscriber's plan, or the default when it has none.
func (t *Tariff) planFor(subscriber string, at time.Time) (string, error) {
	if t.Resolver != nil {
		p, err := t.Resolver.PlanFor(subscriber, at)
		switch {
		case err == nil:
			return p, nil
		case !errors.Is(err, ErrNoPlan):
			return "", err
		}
	}
	return t.DefaultPlan, nil
}

// PlanAt reports the plan a subscriber is on at a time (the default if none).
func (t *Tariff) PlanAt(subscriber string, at time.Time) (string, error) {
	return t.planFor(subscriber, at)
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
