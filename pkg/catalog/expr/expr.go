// Package expr is a small typed expression language for tariff conditions,
// for example:
//
//	rat in ["NR","LTE"] && (hour >= 22 || hour < 6) && !startsWith(cell, "262-01")
//
// An expression is parsed and type-checked once, at publish time, and compiled
// into a tree of closures. Evaluation reads an Env, allocates nothing and has
// no side effects, so it is safe on the rating hot path and for concurrent use.
//
// Variables (all fields of Env):
//
//	rat, slice, cell, unit   string
//	rg, service              number (rating group, service id)
//	hour, minute, weekday    number (weekday: 0 = Sunday), in the plan's time zone
//
// Operators, loosest to tightest: ||, &&, !, comparison (== != < <= > >=, in),
// parentheses. Functions: startsWith(s, prefix), endsWith(s, suffix),
// contains(s, part). Literals: "strings", numbers, true, false, [list, ...].
//
// Deliberately absent: loops, assignment, user functions, regular expressions
// (unbounded cost) and any access outside Env.
package expr

import (
	"fmt"
	"strconv"
	"strings"
)

// Env is the input of an evaluation. Callers fill it per request.
type Env struct {
	RAT, Slice, Cell, Unit string
	RatingGroup, ServiceID uint32
	Hour, Minute, Weekday  int
}

// Program is a compiled boolean expression.
type Program struct {
	src  string
	eval func(*Env) bool
}

// Eval evaluates the program. A nil Program is true, so "no condition" needs
// no special case at call sites.
func (p *Program) Eval(e *Env) bool {
	if p == nil {
		return true
	}
	return p.eval(e)
}

// Source returns the text the program was compiled from.
func (p *Program) Source() string {
	if p == nil {
		return ""
	}
	return p.src
}

// MaxLength bounds expression text so a hostile or careless catalog cannot
// make compilation expensive.
const MaxLength = 2000

// Compile parses and type-checks src. An empty or blank src compiles to nil
// (always true).
func Compile(src string) (*Program, error) {
	if strings.TrimSpace(src) == "" {
		return nil, nil
	}
	if len(src) > MaxLength {
		return nil, fmt.Errorf("expr: expression longer than %d bytes", MaxLength)
	}
	p := &parser{src: src}
	if err := p.lex(); err != nil {
		return nil, err
	}
	n, err := p.parseOr(0)
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.toks) {
		return nil, p.errf("unexpected %q", p.toks[p.pos].text)
	}
	if n.typ != tBool {
		return nil, fmt.Errorf("expr: condition must be true/false, got %s", n.typ)
	}
	return &Program{src: src, eval: n.b}, nil
}

// MustCompile is Compile for tests and constants.
func MustCompile(src string) *Program {
	p, err := Compile(src)
	if err != nil {
		panic(err)
	}
	return p
}

type typ uint8

const (
	tBool typ = iota
	tString
	tNumber
	tStrList
	tNumList
)

func (t typ) String() string {
	return [...]string{"bool", "string", "number", "string list", "number list"}[t]
}

// node is a typed, compiled sub-expression; only the closure matching typ is set.
type node struct {
	typ typ
	b   func(*Env) bool
	s   func(*Env) string
	n   func(*Env) float64
	sl  []string
	nl  []float64
}

type tokKind uint8

const (
	kEOF tokKind = iota
	kIdent
	kString
	kNumber
	kOp // operators and punctuation
)

type token struct {
	kind tokKind
	text string
	pos  int
}

type parser struct {
	src  string
	toks []token
	pos  int
}

func (p *parser) errf(format string, a ...any) error {
	at := len(p.src)
	if p.pos < len(p.toks) {
		at = p.toks[p.pos].pos
	}
	return fmt.Errorf("expr: %s (at offset %d)", fmt.Sprintf(format, a...), at)
}

func (p *parser) lex() error {
	s := p.src
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '"':
			j := i + 1
			var sb strings.Builder
			for ; j < len(s) && s[j] != '"'; j++ {
				if s[j] == '\\' && j+1 < len(s) {
					j++
				}
				sb.WriteByte(s[j])
			}
			if j >= len(s) {
				return fmt.Errorf("expr: unterminated string (at offset %d)", i)
			}
			p.toks = append(p.toks, token{kString, sb.String(), i})
			i = j + 1
		case c >= '0' && c <= '9':
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
				j++
			}
			p.toks = append(p.toks, token{kNumber, s[i:j], i})
			i = j
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i
			for j < len(s) && (s[j] == '_' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] >= '0' && s[j] <= '9') {
				j++
			}
			p.toks = append(p.toks, token{kIdent, s[i:j], i})
			i = j
		default:
			two := ""
			if i+1 < len(s) {
				two = s[i : i+2]
			}
			switch two {
			case "&&", "||", "==", "!=", "<=", ">=":
				p.toks = append(p.toks, token{kOp, two, i})
				i += 2
				continue
			}
			if strings.IndexByte("!<>()[],", c) < 0 {
				return fmt.Errorf("expr: unexpected character %q (at offset %d)", c, i)
			}
			p.toks = append(p.toks, token{kOp, string(c), i})
			i++
		}
	}
	return nil
}

func (p *parser) peek() token {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return token{kind: kEOF}
}

func (p *parser) isOp(op string) bool {
	t := p.peek()
	return t.kind == kOp && t.text == op
}

func (p *parser) expectOp(op string) error {
	if !p.isOp(op) {
		return p.errf("expected %q", op)
	}
	p.pos++
	return nil
}

// maxDepth bounds nesting so a crafted expression cannot exhaust the stack.
const maxDepth = 32

func (p *parser) parseOr(depth int) (node, error) {
	if depth > maxDepth {
		return node{}, p.errf("expression nested deeper than %d", maxDepth)
	}
	l, err := p.parseAnd(depth)
	if err != nil {
		return l, err
	}
	for p.isOp("||") {
		p.pos++
		r, err := p.parseAnd(depth)
		if err != nil {
			return r, err
		}
		if l.typ != tBool || r.typ != tBool {
			return l, p.errf("|| needs true/false operands")
		}
		lb, rb := l.b, r.b
		l = node{typ: tBool, b: func(e *Env) bool { return lb(e) || rb(e) }}
	}
	return l, nil
}

func (p *parser) parseAnd(depth int) (node, error) {
	l, err := p.parseNot(depth)
	if err != nil {
		return l, err
	}
	for p.isOp("&&") {
		p.pos++
		r, err := p.parseNot(depth)
		if err != nil {
			return r, err
		}
		if l.typ != tBool || r.typ != tBool {
			return l, p.errf("&& needs true/false operands")
		}
		lb, rb := l.b, r.b
		l = node{typ: tBool, b: func(e *Env) bool { return lb(e) && rb(e) }}
	}
	return l, nil
}

func (p *parser) parseNot(depth int) (node, error) {
	if depth > maxDepth {
		return node{}, p.errf("expression nested deeper than %d", maxDepth)
	}
	if p.isOp("!") {
		p.pos++
		x, err := p.parseNot(depth + 1)
		if err != nil {
			return x, err
		}
		if x.typ != tBool {
			return x, p.errf("! needs a true/false operand")
		}
		xb := x.b
		return node{typ: tBool, b: func(e *Env) bool { return !xb(e) }}, nil
	}
	return p.parseCmp(depth)
}

func (p *parser) parseCmp(depth int) (node, error) {
	l, err := p.parsePrimary(depth)
	if err != nil {
		return l, err
	}
	t := p.peek()
	if t.kind == kIdent && t.text == "in" {
		p.pos++
		r, err := p.parsePrimary(depth)
		if err != nil {
			return r, err
		}
		return inNode(p, l, r)
	}
	if t.kind != kOp {
		return l, nil
	}
	switch t.text {
	case "==", "!=", "<", "<=", ">", ">=":
	default:
		return l, nil
	}
	p.pos++
	r, err := p.parsePrimary(depth)
	if err != nil {
		return r, err
	}
	if l.typ != r.typ || (l.typ != tString && l.typ != tNumber && l.typ != tBool) {
		return l, p.errf("cannot compare %s with %s", l.typ, r.typ)
	}
	ordered := t.text != "==" && t.text != "!="
	if ordered && l.typ != tNumber {
		return l, p.errf("%s needs numbers", t.text)
	}
	return cmpNode(t.text, l, r), nil
}

func cmpNode(op string, l, r node) node {
	switch l.typ {
	case tString:
		ls, rs := l.s, r.s
		if op == "==" {
			return node{typ: tBool, b: func(e *Env) bool { return ls(e) == rs(e) }}
		}
		return node{typ: tBool, b: func(e *Env) bool { return ls(e) != rs(e) }}
	case tBool:
		lb, rb := l.b, r.b
		if op == "==" {
			return node{typ: tBool, b: func(e *Env) bool { return lb(e) == rb(e) }}
		}
		return node{typ: tBool, b: func(e *Env) bool { return lb(e) != rb(e) }}
	}
	ln, rn := l.n, r.n
	var f func(*Env) bool
	switch op {
	case "==":
		f = func(e *Env) bool { return ln(e) == rn(e) }
	case "!=":
		f = func(e *Env) bool { return ln(e) != rn(e) }
	case "<":
		f = func(e *Env) bool { return ln(e) < rn(e) }
	case "<=":
		f = func(e *Env) bool { return ln(e) <= rn(e) }
	case ">":
		f = func(e *Env) bool { return ln(e) > rn(e) }
	default:
		f = func(e *Env) bool { return ln(e) >= rn(e) }
	}
	return node{typ: tBool, b: f}
}

func inNode(p *parser, l, r node) (node, error) {
	switch {
	case l.typ == tString && r.typ == tStrList:
		ls, set := l.s, r.sl
		return node{typ: tBool, b: func(e *Env) bool {
			v := ls(e)
			for _, x := range set {
				if x == v {
					return true
				}
			}
			return false
		}}, nil
	case l.typ == tNumber && r.typ == tNumList:
		ln, set := l.n, r.nl
		return node{typ: tBool, b: func(e *Env) bool {
			v := ln(e)
			for _, x := range set {
				if x == v {
					return true
				}
			}
			return false
		}}, nil
	}
	return l, p.errf("cannot test %s in %s", l.typ, r.typ)
}

func (p *parser) parsePrimary(depth int) (node, error) {
	t := p.peek()
	switch t.kind {
	case kString:
		p.pos++
		v := t.text
		return node{typ: tString, s: func(*Env) string { return v }}, nil
	case kNumber:
		p.pos++
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return node{}, p.errf("bad number %q", t.text)
		}
		return node{typ: tNumber, n: func(*Env) float64 { return f }}, nil
	case kIdent:
		p.pos++
		return p.identOrCall(t, depth)
	case kOp:
		switch t.text {
		case "(":
			p.pos++
			x, err := p.parseOr(depth + 1)
			if err != nil {
				return x, err
			}
			return x, p.expectOp(")")
		case "[":
			p.pos++
			return p.parseList()
		}
	}
	if t.kind == kEOF {
		return node{}, p.errf("unexpected end of expression")
	}
	return node{}, p.errf("unexpected %q", t.text)
}

func (p *parser) parseList() (node, error) {
	var strs []string
	var nums []float64
	for first := true; !p.isOp("]"); first = false {
		if !first {
			if err := p.expectOp(","); err != nil {
				return node{}, err
			}
		}
		t := p.peek()
		switch t.kind {
		case kString:
			strs = append(strs, t.text)
		case kNumber:
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return node{}, p.errf("bad number %q", t.text)
			}
			nums = append(nums, f)
		default:
			return node{}, p.errf("list items must be string or number literals")
		}
		p.pos++
		if len(strs) > 0 && len(nums) > 0 {
			return node{}, p.errf("a list cannot mix strings and numbers")
		}
		if len(strs)+len(nums) > 256 {
			return node{}, p.errf("list longer than 256 items")
		}
	}
	p.pos++ // ]
	if len(nums) > 0 {
		return node{typ: tNumList, nl: nums}, nil
	}
	return node{typ: tStrList, sl: strs}, nil
}

func (p *parser) identOrCall(t token, depth int) (node, error) {
	if p.isOp("(") {
		return p.call(t.text, depth)
	}
	switch t.text {
	case "true":
		return node{typ: tBool, b: func(*Env) bool { return true }}, nil
	case "false":
		return node{typ: tBool, b: func(*Env) bool { return false }}, nil
	case "rat":
		return strVar(func(e *Env) string { return e.RAT }), nil
	case "slice":
		return strVar(func(e *Env) string { return e.Slice }), nil
	case "cell":
		return strVar(func(e *Env) string { return e.Cell }), nil
	case "unit":
		return strVar(func(e *Env) string { return e.Unit }), nil
	case "rg":
		return numVar(func(e *Env) float64 { return float64(e.RatingGroup) }), nil
	case "service":
		return numVar(func(e *Env) float64 { return float64(e.ServiceID) }), nil
	case "hour":
		return numVar(func(e *Env) float64 { return float64(e.Hour) }), nil
	case "minute":
		return numVar(func(e *Env) float64 { return float64(e.Minute) }), nil
	case "weekday":
		return numVar(func(e *Env) float64 { return float64(e.Weekday) }), nil
	}
	return node{}, fmt.Errorf("expr: unknown variable %q (at offset %d)", t.text, t.pos)
}

func strVar(f func(*Env) string) node  { return node{typ: tString, s: f} }
func numVar(f func(*Env) float64) node { return node{typ: tNumber, n: f} }

func (p *parser) call(name string, depth int) (node, error) {
	var fn func(a, b string) bool
	switch name {
	case "startsWith":
		fn = strings.HasPrefix
	case "endsWith":
		fn = strings.HasSuffix
	case "contains":
		fn = strings.Contains
	default:
		return node{}, p.errf("unknown function %q", name)
	}
	p.pos++ // (
	a, err := p.parseOr(depth + 1)
	if err != nil {
		return a, err
	}
	if err := p.expectOp(","); err != nil {
		return a, err
	}
	b, err := p.parseOr(depth + 1)
	if err != nil {
		return b, err
	}
	if err := p.expectOp(")"); err != nil {
		return b, err
	}
	if a.typ != tString || b.typ != tString {
		return a, p.errf("%s needs two strings", name)
	}
	as, bs := a.s, b.s
	return node{typ: tBool, b: func(e *Env) bool { return fn(as(e), bs(e)) }}, nil
}
