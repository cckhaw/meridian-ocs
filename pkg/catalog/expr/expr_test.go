package expr

import (
	"strings"
	"testing"
)

var env = Env{RAT: "NR", Slice: "embb", Cell: "262-01-4711", Unit: "bytes", RatingGroup: 100, ServiceID: 7, Hour: 23, Minute: 30, Weekday: 6}

func TestEvaluation(t *testing.T) {
	cases := []struct {
		src  string
		want bool
	}{
		{``, true},
		{`   `, true},
		{`true`, true},
		{`false`, false},
		{`rat == "NR"`, true},
		{`rat != "NR"`, false},
		{`rat in ["NR","LTE"]`, true},
		{`rat in ["LTE","UTRAN"]`, false},
		{`rg in [100, 200]`, true},
		{`rg in [1, 2]`, false},
		{`hour >= 22 || hour < 6`, true},
		{`hour >= 6 && hour < 22`, false},
		{`!(hour >= 6 && hour < 22)`, true},
		{`!!true`, true},
		{`weekday == 0 || weekday == 6`, true},
		{`startsWith(cell, "262-01")`, true},
		{`startsWith(cell, "310")`, false},
		{`endsWith(cell, "4711")`, true},
		{`contains(cell, "-01-")`, true},
		{`unit == "bytes" && service != 8`, true},
		{`(rat == "NR" || rat == "LTE") && slice == "embb"`, true},
		{`true == true`, true},
		{`rat == "LTE" || rat == "NR" && hour == 23`, true}, // && binds tighter than ||
		{`rat == "a\"b"`, false},
	}
	for _, c := range cases {
		p, err := Compile(c.src)
		if err != nil {
			t.Errorf("%q: %v", c.src, err)
			continue
		}
		if got := p.Eval(&env); got != c.want {
			t.Errorf("%q = %v, want %v", c.src, got, c.want)
		}
	}
}

func TestNilProgramIsTrue(t *testing.T) {
	var p *Program
	if !p.Eval(&env) || p.Source() != "" {
		t.Fatal("a nil program must be an always-true condition")
	}
}

func TestCompileErrors(t *testing.T) {
	cases := map[string]string{
		`rat`:                   "true/false",
		`rat == 5`:              "cannot compare",
		`hour == "x"`:           "cannot compare",
		`rat < "x"`:             "needs numbers",
		`hour in ["a"]`:         "cannot test",
		`rat in [1, 2]`:         "cannot test",
		`rat in [1, "a"]`:       "mix",
		`bogus == 1`:            "unknown variable",
		`nope(rat, "a")`:        "unknown function",
		`startsWith(rat)`:       `expected ","`,
		`startsWith(hour, "a")`: "two strings",
		`rat == "NR" &&`:        "unexpected end",
		`(rat == "NR"`:          `expected ")"`,
		`rat == "NR" )`:         "unexpected",
		`rat == "unterminated`:  "unterminated",
		`rat = "NR"`:            "unexpected character",
		`hour == 1 == 2`:        "unexpected",
		`"a" && true`:           "true/false",
		`!rat`:                  "true/false",
		`rg in []`:              "cannot test",
		`1.2.3 == 1`:            "bad number",
		`rat == "NR"; drop`:     "unexpected character",
	}
	for src, want := range cases {
		_, err := Compile(src)
		if err == nil {
			t.Errorf("%q compiled, want error containing %q", src, want)
		} else if !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", src, err, want)
		}
	}
}

func TestResourceLimits(t *testing.T) {
	if _, err := Compile(strings.Repeat("true && ", 400) + "true"); err == nil {
		t.Fatal("an expression over MaxLength must be refused")
	}
	deep := strings.Repeat("(", 100) + "true" + strings.Repeat(")", 100)
	if _, err := Compile(deep); err == nil || !strings.Contains(err.Error(), "nested") {
		t.Fatalf("deep nesting: %v", err)
	}
	nots := strings.Repeat("!", 100) + "true"
	if _, err := Compile(nots); err == nil {
		t.Fatal("deep negation must be refused")
	}
	var items []string
	for i := 0; i < 300; i++ {
		items = append(items, `"x"`)
	}
	if _, err := Compile(`rat in [` + strings.Join(items, ",") + `]`); err == nil {
		t.Fatal("a 300 item list must be refused")
	}
}

func TestEvaluationDoesNotAllocate(t *testing.T) {
	p := MustCompile(`rat in ["NR","LTE"] && (hour >= 22 || hour < 6) && startsWith(cell, "262") && !(rg == 5)`)
	if n := testing.AllocsPerRun(1000, func() { _ = p.Eval(&env) }); n != 0 {
		t.Fatalf("%v allocations per evaluation", n)
	}
}

func BenchmarkEval(b *testing.B) {
	p := MustCompile(`rat in ["NR","LTE"] && (hour >= 22 || hour < 6) && startsWith(cell, "262") && !(rg == 5)`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Eval(&env)
	}
}

// Random byte strings must never panic the compiler.
func FuzzCompile(f *testing.F) {
	for _, s := range []string{`rat == "NR"`, `rg in [1,2]`, `((`, `"`, `!`, `startsWith(`, `1.`, `[`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if p, err := Compile(s); err == nil {
			p.Eval(&env)
		}
	})
}
