package mapping

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestExampleCompilesAndTranslates(t *testing.T) {
	spec, err := Parse(ExampleAcmePGW)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	out, id := apply(t, m, `{"recordType":"PGW","node":{"name":"pgw-1"},"chargingId":5,"subscriber":{"imsi":"262011234567890","msisdn":"+49170"},
	  "rat":"6e","openedAtMs":1893499200000,"durationMs":2500,"usage":[{"ratingGroup":100,"uplinkBytes":7,"downlinkBytes":9}]}`)
	if id != "acme-pgw" || out["ratType"] != "LTE" || out["durationSeconds"] != float64(3) || out["msisdn"] != "49170" {
		t.Fatalf("%v", out)
	}
}

// The schema is hand-written; this keeps it from drifting away from the Go types.
func TestSchemaListsExactlyTheFieldsTheParserKnows(t *testing.T) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Defs       map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(SchemaJSON, &schema); err != nil {
		t.Fatalf("the schema is not valid JSON: %v", err)
	}
	keys := func(m map[string]json.RawMessage) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	tags := func(v any) []string {
		var out []string
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			name := strings.Split(rt.Field(i).Tag.Get("yaml"), ",")[0]
			if name != "" && name != "-" {
				out = append(out, name)
			}
		}
		sort.Strings(out)
		return out
	}
	for name, c := range map[string]struct{ got, want []string }{
		"spec":      {keys(schema.Properties), tags(Spec{})},
		"rule":      {keys(schema.Defs["rule"].Properties), tags(Rule{})},
		"condition": {keys(schema.Defs["condition"].Properties), tags(Condition{})},
		"value":     {keys(schema.Defs["value"].Properties), tags(Value{})},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s: schema has %v, parser knows %v", name, c.got, c.want)
		}
	}
}
