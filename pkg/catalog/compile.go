package catalog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog/expr"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
)

var (
	// ErrUnknownPlan: the plan id is not in the snapshot.
	ErrUnknownPlan = errors.New("catalog: unknown price plan")
	// ErrBarred: a policy filter denies the usage.
	ErrBarred = errors.New("catalog: usage barred by policy filter")
	// ErrNoRule: no charge rule matches the usage.
	ErrNoRule = errors.New("catalog: no charge rule matches")
)

// Problem is one validation finding. Path points into the Document, for
// example "plans[2].rules[0].when".
type Problem struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (p Problem) String() string { return p.Path + ": " + p.Message }

// ValidationError carries every error found, not just the first, so a UI can
// show them all.
type ValidationError struct{ Problems []Problem }

func (e *ValidationError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "catalog: %d validation error(s)", len(e.Problems))
	for i, p := range e.Problems {
		if i == 5 {
			sb.WriteString("; ...")
			break
		}
		sb.WriteString("; " + p.String())
	}
	return sb.String()
}

type classMask uint8

const (
	maskPromo classMask = 1 << iota
	maskBundle
	maskCash
	maskAll = maskPromo | maskBundle | maskCash
)

type compiledRule struct {
	id      string
	when    *expr.Program
	mask    classMask
	cash    datalayer.Rate
	hasCash bool
}

type compiledFilter struct {
	id   string
	when *expr.Program
}

type compiledPlan struct {
	loc     *time.Location
	filters []compiledFilter
	rules   []compiledRule
}

// Snapshot is one compiled, immutable catalog version. All methods are safe
// for concurrent use.
type Snapshot struct {
	Version       int64
	EffectiveFrom time.Time

	doc       Document
	plans     map[string]*compiledPlan
	offerings map[string]*ProductOffering
}

// Document returns a copy of the source this snapshot was compiled from.
func (s *Snapshot) Document() Document {
	d := s.doc
	d.Plans = append([]PricePlan(nil), s.doc.Plans...)
	d.Offerings = append([]ProductOffering(nil), s.doc.Offerings...)
	return d
}

// Offering returns an offering by id.
func (s *Snapshot) Offering(id string) (*ProductOffering, bool) {
	o, ok := s.offerings[id]
	return o, ok
}

// HasPlan reports whether the plan exists.
func (s *Snapshot) HasPlan(id string) bool { _, ok := s.plans[id]; return ok }

func validID(s string) bool {
	if s == "" || len(s) > MaxIDLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

func unitKind(s string) (canonical.UnitKind, bool) {
	switch s {
	case "bytes":
		return canonical.UnitBytes, true
	case "seconds":
		return canonical.UnitSeconds, true
	case "events":
		return canonical.UnitEvents, true
	}
	return 0, false
}

type collector struct{ probs []Problem }

func (c *collector) add(path, format string, a ...any) {
	c.probs = append(c.probs, Problem{Path: path, Message: fmt.Sprintf(format, a...)})
}

// Compile validates doc and compiles it. It returns a *ValidationError with
// every problem when the document is not valid. Warnings (for example an
// unreachable rule) do not prevent compilation.
func Compile(doc Document) (*Snapshot, []Problem, error) {
	c := &collector{}
	var warns []Problem
	s := &Snapshot{
		Version:       doc.Version,
		EffectiveFrom: doc.EffectiveFrom,
		plans:         make(map[string]*compiledPlan, len(doc.Plans)),
		offerings:     make(map[string]*ProductOffering, len(doc.Offerings)),
	}
	if doc.EffectiveFrom.IsZero() {
		c.add("effectiveFrom", "is required")
	}
	if len(doc.Plans) == 0 {
		c.add("plans", "at least one price plan is required")
	}
	if len(doc.Plans) > MaxPlans {
		c.add("plans", "more than %d plans", MaxPlans)
	}
	if len(doc.Offerings) > MaxOfferings {
		c.add("offerings", "more than %d offerings", MaxOfferings)
	}

	for i := range doc.Plans {
		p := &doc.Plans[i]
		path := fmt.Sprintf("plans[%d]", i)
		cp := compilePlan(c, &warns, path, p)
		if !validID(p.ID) {
			c.add(path+".id", "must be 1-%d characters of [A-Za-z0-9_.-]", MaxIDLength)
			continue
		}
		if _, dup := s.plans[p.ID]; dup {
			c.add(path+".id", "duplicate plan id %q", p.ID)
			continue
		}
		s.plans[p.ID] = cp
	}

	for i := range doc.Offerings {
		o := &doc.Offerings[i]
		path := fmt.Sprintf("offerings[%d]", i)
		if !validID(o.ID) {
			c.add(path+".id", "must be 1-%d characters of [A-Za-z0-9_.-]", MaxIDLength)
			continue
		}
		if _, dup := s.offerings[o.ID]; dup {
			c.add(path+".id", "duplicate offering id %q", o.ID)
			continue
		}
		if _, ok := s.plans[o.PlanID]; !ok {
			c.add(path+".planId", "unknown price plan %q", o.PlanID)
		}
		seen := map[string]bool{}
		for j, a := range o.Allowances {
			ap := fmt.Sprintf("%s.allowances[%d]", path, j)
			switch {
			case !validID(a.ID):
				c.add(ap+".id", "must be 1-%d characters of [A-Za-z0-9_.-]", MaxIDLength)
			case seen[a.ID]:
				c.add(ap+".id", "duplicate allowance id %q", a.ID)
			}
			seen[a.ID] = true
			if a.Class != "promo" && a.Class != "bundle" {
				c.add(ap+".class", `must be "promo" or "bundle"`)
			}
			if _, ok := unitKind(a.Unit); !ok {
				c.add(ap+".unit", `must be "bytes", "seconds" or "events"`)
			}
			if a.Amount <= 0 || a.Amount > 1<<40 {
				c.add(ap+".amount", "must be between 1 and 2^40")
			}
			if a.ValidityDays < 0 || a.ValidityDays > 3660 {
				c.add(ap+".validityDays", "must be between 0 and 3660")
			}
		}
		s.offerings[o.ID] = o
	}

	if len(c.probs) > 0 {
		sort.SliceStable(c.probs, func(i, j int) bool { return c.probs[i].Path < c.probs[j].Path })
		return nil, warns, &ValidationError{Problems: c.probs}
	}
	// Own the source: later edits to the caller's slices must not change a
	// published version.
	s.doc = cloneDoc(doc)
	for i := range s.doc.Offerings {
		s.offerings[s.doc.Offerings[i].ID] = &s.doc.Offerings[i]
	}
	return s, warns, nil
}

func cloneDoc(d Document) Document {
	out := d
	out.Plans = make([]PricePlan, len(d.Plans))
	for i, p := range d.Plans {
		p.Filters = append([]PolicyFilter(nil), p.Filters...)
		p.Rules = append([]ChargeRule(nil), p.Rules...)
		for j := range p.Rules {
			p.Rules[j].Classes = append([]string(nil), p.Rules[j].Classes...)
			if p.Rules[j].Cash != nil {
				cp := *p.Rules[j].Cash
				p.Rules[j].Cash = &cp
			}
		}
		out.Plans[i] = p
	}
	out.Offerings = make([]ProductOffering, len(d.Offerings))
	for i, o := range d.Offerings {
		o.Allowances = append([]AllowanceBucket(nil), o.Allowances...)
		out.Offerings[i] = o
	}
	return out
}

func compilePlan(c *collector, warns *[]Problem, path string, p *PricePlan) *compiledPlan {
	cp := &compiledPlan{loc: time.UTC}
	if p.TimeZone != "" {
		loc, err := time.LoadLocation(p.TimeZone)
		if err != nil {
			c.add(path+".timeZone", "unknown time zone %q", p.TimeZone)
		} else {
			cp.loc = loc
		}
	}
	if len(p.Filters) > MaxFiltersInPlan {
		c.add(path+".filters", "more than %d filters", MaxFiltersInPlan)
	}
	fids := map[string]bool{}
	for i, f := range p.Filters {
		fp := fmt.Sprintf("%s.filters[%d]", path, i)
		if !validID(f.ID) {
			c.add(fp+".id", "must be 1-%d characters of [A-Za-z0-9_.-]", MaxIDLength)
		} else if fids[f.ID] {
			c.add(fp+".id", "duplicate filter id %q", f.ID)
		}
		fids[f.ID] = true
		if f.Action != "deny" {
			c.add(fp+".action", `must be "deny"`)
		}
		if strings.TrimSpace(f.When) == "" {
			c.add(fp+".when", "a filter needs a condition")
			continue
		}
		prog, err := expr.Compile(f.When)
		if err != nil {
			c.add(fp+".when", "%v", err)
			continue
		}
		cp.filters = append(cp.filters, compiledFilter{id: f.ID, when: prog})
	}

	if len(p.Rules) == 0 {
		c.add(path+".rules", "at least one charge rule is required")
	}
	if len(p.Rules) > MaxRulesPerPlan {
		c.add(path+".rules", "more than %d rules", MaxRulesPerPlan)
	}
	type indexed struct {
		r    compiledRule
		prio int
		idx  int
	}
	var rules []indexed
	rids := map[string]bool{}
	for i, r := range p.Rules {
		rp := fmt.Sprintf("%s.rules[%d]", path, i)
		if !validID(r.ID) {
			c.add(rp+".id", "must be 1-%d characters of [A-Za-z0-9_.-]", MaxIDLength)
		} else if rids[r.ID] {
			c.add(rp+".id", "duplicate rule id %q", r.ID)
		}
		rids[r.ID] = true
		cr := compiledRule{id: r.ID, mask: maskAll}
		if len(r.Classes) > 0 {
			cr.mask = 0
			for _, cl := range r.Classes {
				var m classMask
				switch cl {
				case "promo":
					m = maskPromo
				case "bundle":
					m = maskBundle
				case "cash":
					m = maskCash
				default:
					c.add(rp+".classes", `unknown class %q (use "promo", "bundle" or "cash")`, cl)
					continue
				}
				if cr.mask&m != 0 {
					c.add(rp+".classes", "duplicate class %q", cl)
				}
				cr.mask |= m
			}
		}
		switch {
		case cr.mask&maskCash != 0 && r.Cash == nil:
			c.add(rp+".cash", `a price is required when "cash" may pay`)
		case cr.mask&maskCash == 0 && r.Cash != nil:
			c.add(rp+".cash", `a price is set but "cash" is not among the classes`)
		case r.Cash != nil:
			if r.Cash.Num < 1 || r.Cash.Num > datalayer.MaxNum || r.Cash.Den < 1 || r.Cash.Den > datalayer.MaxDen {
				c.add(rp+".cash", "num must be 1..%d and den 1..%d", datalayer.MaxNum, datalayer.MaxDen)
			} else {
				cr.hasCash = true
				cr.cash = datalayer.Rate{Class: datalayer.ClassCash, Num: r.Cash.Num, Den: r.Cash.Den}
			}
		}
		if cr.mask == 0 {
			c.add(rp+".classes", "no class can pay")
		}
		prog, err := expr.Compile(r.When)
		if err != nil {
			c.add(rp+".when", "%v", err)
		}
		cr.when = prog
		rules = append(rules, indexed{cr, r.Priority, i})
	}
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].prio > rules[j].prio })
	cp.rules = make([]compiledRule, len(rules))
	catchAll := ""
	for i, r := range rules {
		cp.rules[i] = r.r
		if catchAll != "" {
			*warns = append(*warns, Problem{Path: fmt.Sprintf("%s.rules[%d]", path, r.idx),
				Message: fmt.Sprintf("unreachable: rule %q above it matches everything", catchAll)})
		} else if r.r.when == nil {
			catchAll = r.r.id
		}
	}
	if catchAll == "" && len(rules) > 0 {
		*warns = append(*warns, Problem{Path: path + ".rules",
			Message: "no catch-all rule: usage matching no rule will be refused"})
	}
	return cp
}

var envPool = sync.Pool{New: func() any { return new(expr.Env) }}

// Match identifies what priced (or barred) a unit.
type Match struct {
	RuleID   string // the charge rule that priced the unit
	FilterID string // the filter that barred it (with ErrBarred)
}

// Rates prices one unit request under planID and appends the per-class rates
// to dst. It does not allocate beyond growing dst. now is the instant used for
// the time-of-day variables.
func (s *Snapshot) Rates(planID string, now time.Time, req *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, Match, error) {
	p := s.plans[planID]
	if p == nil {
		return dst, Match{}, ErrUnknownPlan
	}
	t := now.In(p.loc)
	// The compiled closures are called indirectly, so a stack Env would escape
	// to the heap on every request; pool it instead.
	env := envPool.Get().(*expr.Env)
	defer envPool.Put(env)
	*env = expr.Env{
		RAT: req.RATType, Slice: req.Slice, Cell: req.CellID, Unit: u.Kind.String(),
		RatingGroup: u.RatingGroup, ServiceID: u.ServiceID,
		Hour: t.Hour(), Minute: t.Minute(), Weekday: int(t.Weekday()),
	}
	for i := range p.filters {
		if p.filters[i].when.Eval(env) {
			return dst, Match{FilterID: p.filters[i].id}, ErrBarred
		}
	}
	for i := range p.rules {
		r := &p.rules[i]
		if !r.when.Eval(env) {
			continue
		}
		if r.mask&maskPromo != 0 {
			dst = append(dst, datalayer.Rate{Class: datalayer.ClassPromo, Num: 1, Den: 1})
		}
		if r.mask&maskBundle != 0 {
			dst = append(dst, datalayer.Rate{Class: datalayer.ClassBundle, Num: 1, Den: 1})
		}
		if r.hasCash {
			dst = append(dst, r.cash)
		}
		return dst, Match{RuleID: r.id}, nil
	}
	return dst, Match{}, ErrNoRule
}
