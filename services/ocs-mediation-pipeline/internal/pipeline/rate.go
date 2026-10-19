package pipeline

import (
	"errors"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
)

// CatalogRater prices each container at the cash rate of the catalog version
// that was in effect when the record was opened (time travel), so re-processing
// a late file after a price change still uses the price that applied. The plan
// is the one the subscriber's *current* offering points to (the Tariff's
// resolver), or the Tariff's default plan: the offering the subscriber was on
// when the record was opened is not recorded anywhere yet.
//
// The result is a list-price value: allowances and bundles are not applied,
// because online charging already debited them. It is meant for reconciliation
// and for post-paid billing input, not as the amount taken from a balance.
type CatalogRater struct {
	Tariff *catalog.Tariff
}

const maxRatedUnits = 1 << 40 // the exact-arithmetic ceiling used by the charge script

// Rate implements Rater and reports whether any container could not be priced.
func (c CatalogRater) Rate(r *cdr.Record) (unrated bool) {
	if len(r.Containers) == 0 {
		return false
	}
	req := canonical.Request{RATType: r.RATType, SubscriberID: r.SubscriberKey, EventTime: r.OpenTime}
	plan, err := c.Tariff.PlanAt(r.SubscriberKey, r.OpenTime)
	if err != nil || plan == "" {
		for i := range r.Containers {
			r.Containers[i].RatedWith = "plan_unknown"
		}
		return true
	}
	var total int64
	var rates [4]datalayer.Rate
	for i := range r.Containers {
		ct := &r.Containers[i]
		units := ct.Uplink + ct.Downlink
		u := canonical.UnitRequest{RatingGroup: ct.RatingGroup, Kind: canonical.UnitBytes}
		if units > maxRatedUnits {
			ct.RatedWith = "volume_too_large"
			unrated = true
			continue
		}
		rs, m, err := c.Tariff.RatesAt(r.OpenTime, plan, &req, &u, rates[:0])
		if err != nil {
			ct.RatedWith = reason(err)
			unrated = true
			continue
		}
		var cash *datalayer.Rate
		for j := range rs {
			if rs[j].Class == datalayer.ClassCash {
				cash = &rs[j]
			}
		}
		if cash == nil {
			ct.RatedWith = "no_cash_rate"
			unrated = true
			continue
		}
		cost := (units*cash.Num + cash.Den - 1) / cash.Den // ceil, as the charge script does
		ct.Charge, ct.RatedWith = &cost, m.RuleID
		total += cost
	}
	if unrated {
		return true
	}
	r.Charge, r.PlanID = &total, plan
	if sn := c.Tariff.Store.At(r.OpenTime); sn != nil {
		r.CatalogVer = sn.Version
	}
	return false
}

func reason(err error) string {
	switch {
	case errors.Is(err, catalog.ErrBarred):
		return "barred"
	case errors.Is(err, catalog.ErrNoRule):
		return "no_rule"
	case errors.Is(err, catalog.ErrUnknownPlan):
		return "unknown_plan"
	case errors.Is(err, catalog.ErrNoVersion):
		return "no_catalog_version"
	}
	return "error"
}
