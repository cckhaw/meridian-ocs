// Package mapping is a declarative JSON-to-JSON translator: a YAML or JSON
// document describes how a vendor or partner record becomes an OCS record, and
// the rules can be replaced at run time without restarting anything.
//
// It exists so that an operator, or an AI Ops agent working under review, can
// add support for a new record layout by writing data rather than code. The
// language is intentionally small and total:
//
//   - paths are `$.a.b[0].c`: fields and array indexes only (no wildcards, no
//     filters, no recursion), so evaluation cost is bounded by the document;
//   - values are a constant, a path, or a concatenation, followed by a fixed
//     list of named modifiers (as, lower, divide, lookup, ...);
//   - there are no loops except `each`, which maps one array into one array;
//   - nothing can call out, read files or run code.
//
// A document is compiled and fully validated before it replaces the one in
// use; a bad document is rejected and the previous mapping keeps serving.
package mapping

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Limits keep documents reviewable and evaluation cheap.
const (
	MaxRules       = 100
	MaxFields      = 64
	MaxDepth       = 4
	MaxConcatParts = 8
	MaxLookup      = 1000
	MaxSpecBytes   = 1 << 20
	MaxInputBytes  = 1 << 20
)

// Spec is the document.
type Spec struct {
	Version     int    `yaml:"version" json:"version"` // must be 1
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Rules       []Rule `yaml:"rules" json:"rules"`
}

// Rule translates the records that match it.
type Rule struct {
	ID    string           `yaml:"id" json:"id"`
	Match []Condition      `yaml:"match,omitempty" json:"match,omitempty"` // all must hold; none = matches everything
	Out   map[string]Value `yaml:"output" json:"output"`
}

// Condition tests one path: exactly one of Equals, In, Exists.
type Condition struct {
	Path   string   `yaml:"path" json:"path"`
	Equals *string  `yaml:"equals,omitempty" json:"equals,omitempty"`
	In     []string `yaml:"in,omitempty" json:"in,omitempty"`
	Exists *bool    `yaml:"exists,omitempty" json:"exists,omitempty"`
}

// Scalar is a constant string, number, boolean or null written in a document.
type Scalar struct{ V any }

// UnmarshalYAML accepts scalars only: a constant object or list would let a
// document smuggle structure past the field-by-field validation.
func (s *Scalar) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return errors.New("only a string, number or boolean is allowed here")
	}
	return n.Decode(&s.V)
}

// Value produces one output field.
type Value struct {
	// Source: exactly one of Const, Path, Concat, Each.
	Const  *Scalar `yaml:"const,omitempty" json:"const,omitempty"`
	Path   string  `yaml:"path,omitempty" json:"path,omitempty"`
	Concat []Value `yaml:"concat,omitempty" json:"concat,omitempty"`
	// Each maps the array at this path to an array of objects built from Item.
	Each string           `yaml:"each,omitempty" json:"each,omitempty"`
	Item map[string]Value `yaml:"item,omitempty" json:"item,omitempty"`

	// Modifiers, applied in this order.
	Strip    string            `yaml:"strip,omitempty" json:"strip,omitempty"`       // trim these characters from both ends
	Trim     bool              `yaml:"trim,omitempty" json:"trim,omitempty"`         // trim white space
	Lower    bool              `yaml:"lower,omitempty" json:"lower,omitempty"`       //
	Upper    bool              `yaml:"upper,omitempty" json:"upper,omitempty"`       //
	Lookup   map[string]string `yaml:"lookup,omitempty" json:"lookup,omitempty"`     // exact-match table; a miss is an error unless Default is set
	Divide   *float64          `yaml:"divide,omitempty" json:"divide,omitempty"`     // numeric
	Multiply *float64          `yaml:"multiply,omitempty" json:"multiply,omitempty"` // numeric
	As       string            `yaml:"as,omitempty" json:"as,omitempty"`             // string | number | integer | bool | time_rfc3339 (normalise an RFC 3339 string to UTC) | time_unix_s | time_unix_ms (Unix time to an RFC 3339 UTC string)
	Default  *Scalar           `yaml:"default,omitempty" json:"default,omitempty"`   // used when the source is absent or null
	Required bool              `yaml:"required,omitempty" json:"required,omitempty"` // absent source is an error (otherwise the field is omitted)
}

// Errors.
var (
	ErrNoMatch = errors.New("mapping: no rule matches the record")
	ErrInput   = errors.New("mapping: the record cannot be translated")
)

// ValidationError lists every problem found in a Spec.
type ValidationError struct{ Problems []string }

func (e *ValidationError) Error() string {
	return fmt.Sprintf("mapping: %d problem(s): %s", len(e.Problems), strings.Join(e.Problems, "; "))
}

type step struct {
	field string
	index int // >= 0 for an array index; -1 for a field
}

type cpath []step

var (
	idRe    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)
	fieldRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
	segRe   = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_-]*)((?:\[[0-9]{1,4}\])*)$`)
)

func parsePath(p string) (cpath, error) {
	if p == "$" {
		return cpath{}, nil
	}
	if !strings.HasPrefix(p, "$.") {
		return nil, fmt.Errorf("path %q must start with $.", p)
	}
	var out cpath
	for _, seg := range strings.Split(p[2:], ".") {
		m := segRe.FindStringSubmatch(seg)
		if m == nil {
			return nil, fmt.Errorf("bad path segment %q in %q (fields and [n] indexes only)", seg, p)
		}
		out = append(out, step{field: m[1], index: -1})
		for _, ix := range regexp.MustCompile(`\[([0-9]+)\]`).FindAllStringSubmatch(m[2], -1) {
			n, _ := strconv.Atoi(ix[1])
			out = append(out, step{index: n})
		}
		if len(out) > 16 {
			return nil, fmt.Errorf("path %q is deeper than 16 steps", p)
		}
	}
	return out, nil
}

func (p cpath) eval(root any) (any, bool) {
	cur := root
	for _, s := range p {
		if s.index >= 0 {
			a, ok := cur.([]any)
			if !ok || s.index >= len(a) {
				return nil, false
			}
			cur = a[s.index]
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[s.field]; !ok {
			return nil, false
		}
	}
	return cur, cur != nil
}

type cvalue struct {
	constant any
	hasConst bool
	path     cpath
	hasPath  bool
	concat   []*cvalue
	each     cpath
	hasEach  bool
	item     map[string]*cvalue

	strip, trimSet     string
	trim, lower, upper bool
	lookup             map[string]string
	divide, multiply   *float64
	as                 string
	def                any
	hasDef             bool
	required           bool
	where              string
}

type crule struct {
	id    string
	match []ccond
	out   map[string]*cvalue
}

type ccond struct {
	path   cpath
	equals *string
	in     map[string]bool
	exists *bool
}

// Mapper is a compiled, immutable Spec.
type Mapper struct {
	name  string
	rules []crule
}

// Name returns the spec's name.
func (m *Mapper) Name() string { return m.name }

type problems struct{ list []string }

func (p *problems) add(format string, a ...any) { p.list = append(p.list, fmt.Sprintf(format, a...)) }

var validAs = map[string]bool{"": true, "string": true, "number": true, "integer": true, "bool": true,
	"time_rfc3339": true, "time_unix_s": true, "time_unix_ms": true}

// Parse reads a YAML or JSON document strictly (unknown keys are errors).
func Parse(doc []byte) (*Spec, error) {
	if len(doc) > MaxSpecBytes {
		return nil, fmt.Errorf("mapping: document larger than %d bytes", MaxSpecBytes)
	}
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	dec.KnownFields(true)
	var s Spec
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("mapping: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return nil, errors.New("mapping: the file holds more than one document")
	}
	return &s, nil
}

// Compile validates a Spec and prepares it for use.
func Compile(s *Spec) (*Mapper, error) {
	var pr problems
	if s.Version != 1 {
		pr.add("version must be 1")
	}
	if !idRe.MatchString(s.Name) {
		pr.add("name must be 1-64 characters of [A-Za-z0-9_.-] starting with a letter")
	}
	if len(s.Rules) == 0 || len(s.Rules) > MaxRules {
		pr.add("1 to %d rules are required", MaxRules)
	}
	m := &Mapper{name: s.Name}
	seen := map[string]bool{}
	for i, r := range s.Rules {
		at := fmt.Sprintf("rules[%d]", i)
		if !idRe.MatchString(r.ID) || seen[r.ID] {
			pr.add("%s.id %q must be unique, 1-64 characters of [A-Za-z0-9_.-]", at, r.ID)
		}
		seen[r.ID] = true
		cr := crule{id: r.ID}
		for j, c := range r.Match {
			cc, err := compileCond(c)
			if err != nil {
				pr.add("%s.match[%d]: %v", at, j, err)
				continue
			}
			cr.match = append(cr.match, cc)
		}
		if len(r.Out) == 0 || len(r.Out) > MaxFields {
			pr.add("%s.output needs 1 to %d fields", at, MaxFields)
		}
		cr.out = compileFields(&pr, at+".output", r.Out, 0)
		m.rules = append(m.rules, cr)
	}
	if len(pr.list) > 0 {
		return nil, &ValidationError{pr.list}
	}
	return m, nil
}

func compileCond(c Condition) (ccond, error) {
	p, err := parsePath(c.Path)
	if err != nil {
		return ccond{}, err
	}
	n := 0
	cc := ccond{path: p}
	if c.Equals != nil {
		n++
		cc.equals = c.Equals
	}
	if len(c.In) > 0 {
		n++
		if len(c.In) > MaxLookup {
			return cc, fmt.Errorf("`in` has more than %d values", MaxLookup)
		}
		cc.in = map[string]bool{}
		for _, v := range c.In {
			cc.in[v] = true
		}
	}
	if c.Exists != nil {
		n++
		cc.exists = c.Exists
	}
	if n != 1 {
		return cc, errors.New("exactly one of equals, in or exists is required")
	}
	return cc, nil
}

func compileFields(pr *problems, at string, in map[string]Value, depth int) map[string]*cvalue {
	out := make(map[string]*cvalue, len(in))
	for name, v := range in {
		fat := at + "." + name
		if !fieldRe.MatchString(name) {
			pr.add("%s: bad field name", fat)
			continue
		}
		out[name] = compileValue(pr, fat, v, depth)
	}
	return out
}

func compileValue(pr *problems, at string, v Value, depth int) *cvalue {
	cv := &cvalue{where: at, strip: v.Strip, trim: v.Trim, lower: v.Lower, upper: v.Upper, divide: v.Divide, multiply: v.Multiply,
		as: v.As, required: v.Required}
	if depth > MaxDepth {
		pr.add("%s: nested deeper than %d", at, MaxDepth)
		return cv
	}
	sources := 0
	if v.Const != nil {
		sources++
		cv.constant, cv.hasConst = v.Const.V, true
	}
	if v.Path != "" {
		sources++
		p, err := parsePath(v.Path)
		if err != nil {
			pr.add("%s.path: %v", at, err)
		}
		cv.path, cv.hasPath = p, true
	}
	if len(v.Concat) > 0 {
		sources++
		if len(v.Concat) > MaxConcatParts {
			pr.add("%s.concat: more than %d parts", at, MaxConcatParts)
		}
		for i, part := range v.Concat {
			cv.concat = append(cv.concat, compileValue(pr, fmt.Sprintf("%s.concat[%d]", at, i), part, depth+1))
		}
	}
	if v.Each != "" {
		sources++
		p, err := parsePath(v.Each)
		if err != nil {
			pr.add("%s.each: %v", at, err)
		}
		cv.each, cv.hasEach = p, true
		if len(v.Item) == 0 || len(v.Item) > MaxFields {
			pr.add("%s.item needs 1 to %d fields", at, MaxFields)
		}
		cv.item = compileFields(pr, at+".item", v.Item, depth+1)
	} else if len(v.Item) > 0 {
		pr.add("%s: item is only valid with each", at)
	}
	if sources != 1 {
		pr.add("%s: exactly one of const, path, concat or each is required", at)
	}
	if !validAs[v.As] {
		pr.add("%s.as: unknown conversion %q", at, v.As)
	}
	if v.Lookup != nil {
		if len(v.Lookup) > MaxLookup {
			pr.add("%s.lookup: more than %d entries", at, MaxLookup)
		}
		cv.lookup = v.Lookup
	}
	if v.Divide != nil && (*v.Divide == 0 || math.IsNaN(*v.Divide) || math.IsInf(*v.Divide, 0)) {
		pr.add("%s.divide: must be a finite non-zero number", at)
	}
	if v.Multiply != nil && (math.IsNaN(*v.Multiply) || math.IsInf(*v.Multiply, 0)) {
		pr.add("%s.multiply: must be finite", at)
	}
	if v.Default != nil {
		cv.def, cv.hasDef = v.Default.V, true
	}
	if v.Required && v.Default != nil {
		pr.add("%s: required and default are contradictory", at)
	}
	if v.Each != "" && (v.Strip != "" || v.Trim || v.Lower || v.Upper || v.Lookup != nil || v.Divide != nil || v.Multiply != nil || v.As != "") {
		pr.add("%s: modifiers do not apply to each", at)
	}
	return cv
}

func (c *ccond) holds(root any) bool {
	v, ok := c.path.eval(root)
	switch {
	case c.exists != nil:
		return ok == *c.exists
	case !ok:
		return false
	case c.equals != nil:
		return scalarString(v) == *c.equals
	}
	return c.in[scalarString(v)]
}

func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	}
	return ""
}

// Apply translates one JSON record with the first rule that matches it. It
// returns the output JSON and the id of the rule used.
func (m *Mapper) Apply(in []byte) ([]byte, string, error) {
	if len(in) > MaxInputBytes {
		return nil, "", fmt.Errorf("%w: larger than %d bytes", ErrInput, MaxInputBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrInput, err)
	}
	if _, ok := root.(map[string]any); !ok {
		return nil, "", fmt.Errorf("%w: not a JSON object", ErrInput)
	}
	for i := range m.rules {
		r := &m.rules[i]
		matched := true
		for j := range r.match {
			if !r.match[j].holds(root) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		out, err := evalFields(r.out, root)
		if err != nil {
			return nil, r.id, fmt.Errorf("%w: rule %s: %v", ErrInput, r.id, err)
		}
		b, err := json.Marshal(out)
		return b, r.id, err
	}
	return nil, "", ErrNoMatch
}

func evalFields(fields map[string]*cvalue, root any) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for name, cv := range fields {
		v, present, err := cv.eval(root)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", cv.where, err)
		}
		if present {
			out[name] = v
		}
	}
	return out, nil
}

func (cv *cvalue) eval(root any) (any, bool, error) {
	var v any
	var ok bool
	switch {
	case cv.hasConst:
		v, ok = cv.constant, cv.constant != nil
	case cv.hasPath:
		v, ok = cv.path.eval(root)
	case len(cv.concat) > 0:
		var sb strings.Builder
		ok = true
		for _, part := range cv.concat {
			pv, pok, err := part.eval(root)
			if err != nil {
				return nil, false, err
			}
			if !pok {
				ok = false
				break
			}
			sb.WriteString(fmt.Sprint(scalarOrString(pv)))
		}
		v = sb.String()
	case cv.hasEach:
		arr, aok := cv.each.eval(root)
		if !aok {
			ok = false
			break
		}
		elems, isArr := arr.([]any)
		if !isArr {
			return nil, false, errors.New("each: the path is not an array")
		}
		if len(elems) > 4096 {
			return nil, false, errors.New("each: more than 4096 elements")
		}
		list := make([]any, 0, len(elems))
		for _, e := range elems {
			o, err := evalFields(cv.item, e)
			if err != nil {
				return nil, false, err
			}
			list = append(list, o)
		}
		return list, true, nil
	}
	if !ok {
		switch {
		case cv.hasDef:
			v, ok = cv.def, true
		case cv.required:
			return nil, false, errors.New("a required value is missing")
		default:
			return nil, false, nil
		}
	}
	v, err := cv.modify(v)
	return v, true, err
}

func scalarOrString(v any) any {
	if n, ok := v.(json.Number); ok {
		return n.String()
	}
	return v
}

func toFloat(v any) (float64, error) {
	switch t := v.(type) {
	case json.Number:
		return t.Float64()
	case float64:
		return t, nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(t), 64)
	}
	return 0, fmt.Errorf("%v is not a number", v)
}

func (cv *cvalue) modify(v any) (any, error) {
	if s, ok := v.(string); ok {
		if cv.strip != "" {
			s = strings.Trim(s, cv.strip)
		}
		if cv.trim {
			s = strings.TrimSpace(s)
		}
		if cv.lower {
			s = strings.ToLower(s)
		}
		if cv.upper {
			s = strings.ToUpper(s)
		}
		v = s
	}
	if cv.lookup != nil {
		key := scalarString(v)
		out, ok := cv.lookup[key]
		switch {
		case ok:
			v = out
		case cv.hasDef:
			v = cv.def
		default:
			return nil, fmt.Errorf("%q is not in the lookup table", key)
		}
	}
	if cv.divide != nil || cv.multiply != nil {
		f, err := toFloat(v)
		if err != nil {
			return nil, err
		}
		if cv.divide != nil {
			f /= *cv.divide
		}
		if cv.multiply != nil {
			f *= *cv.multiply
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errors.New("the result is not finite")
		}
		v = f
	}
	return convert(v, cv.as)
}

func convert(v any, as string) (any, error) {
	switch as {
	case "":
		if n, ok := v.(json.Number); ok {
			return n, nil
		}
		return v, nil
	case "string":
		return fmt.Sprint(scalarOrString(v)), nil
	case "number":
		f, err := toFloat(v)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("%v is not a number", v)
		}
		return f, nil
	case "integer":
		f, err := toFloat(v)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f > 1<<53 || f < -(1<<53) {
			return nil, fmt.Errorf("%v is not an integer in range", v)
		}
		return int64(math.Round(f)), nil
	case "bool":
		switch t := v.(type) {
		case bool:
			return t, nil
		case string:
			switch strings.ToLower(t) {
			case "true", "1", "yes":
				return true, nil
			case "false", "0", "no":
				return false, nil
			}
		}
		return nil, fmt.Errorf("%v is not a boolean", v)
	case "time_rfc3339":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%v is not a time string", v)
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, err
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case "time_unix_s", "time_unix_ms":
		f, err := toFloat(v)
		if err != nil || f < 0 || f > 1e14 {
			return nil, fmt.Errorf("%v is not a Unix time", v)
		}
		if as == "time_unix_s" {
			return time.Unix(int64(f), 0).UTC().Format(time.RFC3339Nano), nil
		}
		return time.UnixMilli(int64(f)).UTC().Format(time.RFC3339Nano), nil
	}
	return v, nil
}
