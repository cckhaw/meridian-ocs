package mapping

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const acme = `
version: 1
name: acme-pgw
description: Acme packet gateway JSON export to the normalized CDR
rules:
  - id: acme-pgw
    match:
      - path: $.recordType
        in: ["pgw", "PGW"]
      - path: $.subscriber.imsi
        exists: true
    output:
      type: {const: PGW}
      nodeId: {path: $.node.name, required: true}
      chargingId: {path: $.chargingId, as: integer}
      sequence: {path: $.seq, as: integer, default: 0}
      imsi: {path: $.subscriber.imsi}
      msisdn: {path: $.subscriber.msisdn, strip: "+"}
      apn: {path: $.apn, lower: true}
      ratType: {path: $.rat, lookup: {"6": NR, "1": UTRAN, "6e": LTE}, default: unknown}
      openTime: {path: $.openedAtMs, as: time_unix_ms}
      durationSeconds: {path: $.durationMs, divide: 1000, as: integer}
      label: {concat: [{const: "acme/"}, {path: $.node.name}, {const: "#"}, {path: $.seq}]}
      containers:
        each: $.usage
        item:
          ratingGroup: {path: $.rg, as: integer}
          uplink: {path: $.ul, as: integer}
          downlink: {path: $.dl, as: integer}
  - id: fallback-sms
    match:
      - path: $.recordType
        equals: sms
    output:
      type: {const: SMS}
      nodeId: {path: $.node.name}
`

const acmeRecord = `{"recordType":"pgw","node":{"name":"pgw-7"},"chargingId":4242,"seq":3,
 "subscriber":{"imsi":"262011234567890","msisdn":"+491701234567"},"apn":"Internet.Example","rat":"6",
 "openedAtMs":1893499200000,"durationMs":61500,
 "usage":[{"rg":100,"ul":1000,"dl":5000},{"rg":200,"ul":1,"dl":2}]}`

func compile(t *testing.T, doc string) *Mapper {
	t.Helper()
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	m, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func apply(t *testing.T, m *Mapper, in string) (map[string]any, string) {
	t.Helper()
	b, id, err := m.Apply([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out, id
}

func TestTranslation(t *testing.T) {
	out, id := apply(t, compile(t, acme), acmeRecord)
	if id != "acme-pgw" {
		t.Fatal(id)
	}
	want := map[string]any{
		"type": "PGW", "nodeId": "pgw-7", "chargingId": float64(4242), "sequence": float64(3), "imsi": "262011234567890",
		"msisdn": "491701234567", "apn": "internet.example", "ratType": "NR", "openTime": "2030-01-01T12:00:00Z",
		"durationSeconds": float64(62), "label": "acme/pgw-7#3",
	}
	for k, v := range want {
		if out[k] != v {
			t.Errorf("%s = %v (%T), want %v", k, out[k], out[k], v)
		}
	}
	cs, _ := out["containers"].([]any)
	if len(cs) != 2 || cs[0].(map[string]any)["downlink"] != float64(5000) || cs[1].(map[string]any)["ratingGroup"] != float64(200) {
		t.Fatalf("%v", out["containers"])
	}
}

func TestRulesAreTriedInOrderAndDefaultsApply(t *testing.T) {
	m := compile(t, acme)
	out, id := apply(t, m, `{"recordType":"sms","node":{"name":"smsc-1"}}`)
	if id != "fallback-sms" || out["type"] != "SMS" {
		t.Fatalf("%s %v", id, out)
	}
	// Missing optional values are omitted, a default fills `sequence`, an unknown lookup uses its default.
	out, _ = apply(t, m, `{"recordType":"PGW","node":{"name":"n"},"subscriber":{"imsi":"1"},"rat":"99","openedAtMs":1000,"durationMs":0}`)
	if out["sequence"] != float64(0) || out["ratType"] != "unknown" {
		t.Fatalf("%v", out)
	}
	if _, has := out["msisdn"]; has {
		t.Fatalf("an absent optional value must be omitted: %v", out)
	}
	if _, has := out["label"]; has {
		t.Fatalf("a concat with a missing part must be omitted: %v", out)
	}
}

func TestFailures(t *testing.T) {
	m := compile(t, acme)
	cases := map[string]struct {
		in   string
		want error
	}{
		"no rule matches":        {`{"recordType":"voice"}`, ErrNoMatch},
		"match needs imsi":       {`{"recordType":"pgw","node":{"name":"n"}}`, ErrNoMatch},
		"required value missing": {`{"recordType":"pgw","subscriber":{"imsi":"1"}}`, ErrInput},
		"not a number":           {`{"recordType":"pgw","node":{"name":"n"},"subscriber":{"imsi":"1"},"chargingId":"abc"}`, ErrInput},
		"each on a non-array":    {`{"recordType":"pgw","node":{"name":"n"},"subscriber":{"imsi":"1"},"usage":{"rg":1}}`, ErrInput},
		"not json":               {`nope`, ErrInput},
		"not an object":          {`[1,2]`, ErrInput},
		"bad time":               {`{"recordType":"pgw","node":{"name":"n"},"subscriber":{"imsi":"1"},"openedAtMs":-5}`, ErrInput},
	}
	for name, c := range cases {
		if _, _, err := m.Apply([]byte(c.in)); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	if _, _, err := m.Apply(make([]byte, MaxInputBytes+1)); !errors.Is(err, ErrInput) {
		t.Errorf("oversized: %v", err)
	}
	// A lookup miss with no default is an error, not a silent pass-through.
	strict := compile(t, "version: 1\nname: s\nrules:\n - id: r\n   output:\n    k: {path: $.k, lookup: {a: b}}\n")
	if _, _, err := strict.Apply([]byte(`{"k":"zzz"}`)); !errors.Is(err, ErrInput) {
		t.Errorf("lookup miss: %v", err)
	}
}

func TestValidationReportsEverything(t *testing.T) {
	doc := `
version: 2
name: "bad name!"
rules:
  - id: r1
    match:
      - {path: "$.a[*]", equals: x}
      - {path: "$.b"}
      - {path: "$.c", equals: x, exists: true}
    output:
      "bad field": {path: $.x}
      both: {const: 1, path: $.y}
      none: {}
      conv: {path: $.z, as: telepathy}
      div0: {path: $.z, divide: 0}
      contradictory: {path: $.z, required: true, default: 1}
      itemWithoutEach: {path: $.z, item: {a: {path: $.q}}}
      eachModifiers: {each: $.arr, item: {a: {path: $.q}}, lower: true}
  - id: r1
    output: {}
`
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Compile(spec)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("%v", err)
	}
	all := strings.Join(ve.Problems, "\n")
	for _, want := range []string{"version must be 1", "name must be", "bad path segment", "exactly one of equals, in or exists", "bad field name",
		"exactly one of const, path, concat or each", "unknown conversion", "finite non-zero", "contradictory", "item is only valid with each",
		"modifiers do not apply to each", "must be unique", "needs 1 to 64 fields"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in:\n%s", want, all)
		}
	}
}

func TestParseIsStrict(t *testing.T) {
	for name, doc := range map[string]string{
		"unknown key":   "version: 1\nname: a\nrules:\n - id: r\n   outptu: {}\n",
		"two documents": "version: 1\nname: a\nrules: []\n---\nversion: 1\n",
		"not yaml":      "version: [1\n",
		"too large":     strings.Repeat("#", MaxSpecBytes+1),
		"modifier typo": "version: 1\nname: a\nrules:\n - id: r\n   output:\n    k: {path: $.k, lowercase: true}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// JSON is accepted too.
	if _, err := Parse([]byte(`{"version":1,"name":"j","rules":[{"id":"r","output":{"k":{"const":"v"}}}]}`)); err != nil {
		t.Fatal(err)
	}
}

func TestPathsAreBounded(t *testing.T) {
	for _, p := range []string{"a.b", "$.", "$..a", "$.a[]", "$.a[x]", "$.a[1", "$.a b", "$.a[99999]", "$.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r"} {
		if _, err := parsePath(p); err == nil {
			t.Errorf("%q accepted", p)
		}
	}
	for _, p := range []string{"$", "$.a", "$.a.b[0]", "$.a[1][2].c", "$.field-name"} {
		if _, err := parsePath(p); err != nil {
			t.Errorf("%q: %v", p, err)
		}
	}
}

func TestRandomInputNeverPanics(t *testing.T) {
	m := compile(t, acme)
	rng := rand.New(rand.NewSource(7))
	pieces := []string{`{`, `}`, `"usage"`, `:`, `[`, `]`, `,`, `null`, `1e999`, `"x"`, `{"recordType":"pgw"`, `"subscriber":{"imsi":1}`, `-0`, `true`}
	for i := 0; i < 20000; i++ {
		var sb strings.Builder
		for j := rng.Intn(12); j >= 0; j-- {
			sb.WriteString(pieces[rng.Intn(len(pieces))])
		}
		m.Apply([]byte(sb.String()))
	}
}

func FuzzApply(f *testing.F) {
	f.Add([]byte(acmeRecord))
	f.Add([]byte(`{"recordType":"sms"}`))
	spec, err := Parse([]byte(acme))
	if err != nil {
		f.Fatal(err)
	}
	m, err := Compile(spec)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, in []byte) { m.Apply(in) })
}

func TestHolderHotReloadKeepsTheLastGoodMapping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "map.yaml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(acme)
	h, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.Current().Name() != "acme-pgw" || h.Reloads() != 1 {
		t.Fatal("initial load")
	}
	first := h.Checksum()

	var mu sync.Mutex
	var events []Event
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.Watch(ctx, 5*time.Millisecond, func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() })
		close(done)
	}()
	defer func() { cancel(); <-done }()
	wait := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); !cond(); {
			if time.Now().After(deadline) {
				t.Fatalf("timed out: %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// A good edit takes effect with no restart.
	write(strings.Replace(acme, "name: acme-pgw", "name: acme-pgw-v2", 1))
	wait("reload", func() bool { return h.Current().Name() == "acme-pgw-v2" })
	if h.Checksum() == first {
		t.Fatal("checksum did not change")
	}

	// A broken edit is rejected; translation keeps working with the previous mapping.
	write("version: 1\nname: broken\nrules:\n - id: r\n   output:\n    k: {path: nonsense}\n")
	wait("rejection", func() bool { return h.Failures() >= 1 })
	if h.Current().Name() != "acme-pgw-v2" {
		t.Fatal("a rejected file replaced the mapping")
	}
	if _, _, err := h.Apply([]byte(acmeRecord)); err != nil {
		t.Fatalf("translation stopped: %v", err)
	}
	// Repairing the file recovers.
	write(acme)
	wait("recovery", func() bool { return h.Current().Name() == "acme-pgw" })

	mu.Lock()
	defer mu.Unlock()
	var good, bad int
	for _, e := range events {
		if e.Reloaded {
			good++
		}
		if e.Err != nil {
			bad++
		}
	}
	if good != 2 || bad < 1 {
		t.Fatalf("events: %d reloads, %d rejections", good, bad)
	}
}

func TestOpenRefusesAnInvalidOrMissingFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(filepath.Join(dir, "nope.yaml")); err == nil {
		t.Fatal("missing file")
	}
	bad := filepath.Join(dir, "bad.yaml")
	os.WriteFile(bad, []byte("version: 3\n"), 0o644)
	if _, err := Open(bad); err == nil {
		t.Fatal("invalid file")
	}
}

func BenchmarkApply(b *testing.B) {
	spec, _ := Parse([]byte(acme))
	m, _ := Compile(spec)
	in := []byte(acmeRecord)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := m.Apply(in); err != nil {
			b.Fatal(err)
		}
	}
}
