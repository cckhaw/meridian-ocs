// Package catalog is the dynamic tariff catalog: product offerings, price
// plans, charge rules and policy filters, published as immutable versions that
// can be dated in the past, present or future, and compiled into a form the
// rating engine evaluates without allocating.
//
// A Document is the editable, JSON-serialisable source. Compile validates a
// Document (reporting every problem at once, for a UI) and produces a
// Snapshot. A Store holds the timeline of snapshots behind an atomic pointer,
// so publishing never blocks or restarts a rating node.
package catalog

import "time"

// Document is the source form of one catalog version.
type Document struct {
	// Version is assigned by the Store at publish time (previous + 1). On input
	// it is ignored for validation and dry-run, and must equal the expected
	// next version on publish if set.
	Version int64 `json:"version,omitempty"`
	// EffectiveFrom is when this version starts to apply. It must be later than
	// the previous version's, so history is append-only.
	EffectiveFrom time.Time `json:"effectiveFrom"`
	Note          string    `json:"note,omitempty"`

	Plans     []PricePlan       `json:"plans"`
	Offerings []ProductOffering `json:"offerings"`
}

// PricePlan decides what a unit of usage costs.
type PricePlan struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	// Currency is informational (ISO 4217); amounts are in minor units.
	Currency string `json:"currency,omitempty"`
	// TimeZone (IANA name) is used for the hour/minute/weekday variables.
	// Empty means UTC.
	TimeZone string `json:"timeZone,omitempty"`
	// Filters are evaluated first; a matching filter bars the usage.
	Filters []PolicyFilter `json:"filters,omitempty"`
	// Rules are evaluated by descending Priority, then in listed order; the
	// first rule whose condition holds prices the usage.
	Rules []ChargeRule `json:"rules"`
}

// PolicyFilter bars usage that matches its condition (for example roaming on
// a domestic plan). It prices nothing; a barred unit gets no rates.
type PolicyFilter struct {
	ID     string `json:"id"`
	When   string `json:"when"`
	Action string `json:"action"` // "deny"
}

// ChargeRule prices usage that matches When.
type ChargeRule struct {
	ID       string `json:"id"`
	Priority int    `json:"priority,omitempty"`
	// When is an expression (see package expr). Empty always matches.
	When string `json:"when,omitempty"`
	// Classes lists which bucket classes may pay: "promo", "bundle", "cash".
	// Empty means all three. Promo and bundle buckets are debited 1:1.
	Classes []string `json:"classes,omitempty"`
	// Cash is the price when paying from cash: Num/Den minor units per unit.
	// It is required when "cash" is among the classes.
	Cash *Price `json:"cash,omitempty"`
}

// Price is a rational price, Num/Den minor currency units per unit.
type Price struct {
	Num int64 `json:"num"`
	Den int64 `json:"den"`
}

// ProductOffering is what a subscriber buys: a price plan plus the allowances
// that come with it.
type ProductOffering struct {
	ID         string            `json:"id"`
	Name       string            `json:"name,omitempty"`
	PlanID     string            `json:"planId"`
	Allowances []AllowanceBucket `json:"allowances,omitempty"`
}

// AllowanceBucket is a bucket created when the offering is attached.
type AllowanceBucket struct {
	ID    string `json:"id"`
	Class string `json:"class"` // "promo" or "bundle"
	Unit  string `json:"unit"`  // "bytes", "seconds", "events"
	// Amount is in units of Unit.
	Amount int64 `json:"amount"`
	// ValidityDays is how long the bucket lives after attachment; 0 = forever.
	ValidityDays int `json:"validityDays,omitempty"`
	// Order is the consumption order within the class (lower first).
	Order float64 `json:"order,omitempty"`
}

// Limits keep a catalog small enough to reason about and cheap to compile.
const (
	MaxPlans         = 10_000
	MaxRulesPerPlan  = 500
	MaxFiltersInPlan = 100
	MaxOfferings     = 100_000
	MaxIDLength      = 64
)
