package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigRefusals(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "m.json")
	os.WriteFile(bad, []byte(`{"types":{"79":"PGW"},"fields":[],"surprise":1}`), 0o644)
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no output", map[string]string{"OCS_MED_IN_DIR": "/in"}, "OCS_MED_OUT_DIR"},
		{"no input", map[string]string{"OCS_MED_OUT_DIR": "/out"}, "nothing to read"},
		{"bad rating", map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_MED_IN_DIR": "/i", "OCS_MED_RATING": "yes"}, "OCS_MED_RATING"},
		{"catalog without plan", map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_MED_IN_DIR": "/i", "OCS_MED_RATING": "catalog"}, "OCS_DEFAULT_PLAN"},
		{"bad duration", map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_MED_IN_DIR": "/i", "OCS_MED_SETTLE": "soon"}, "OCS_MED_SETTLE"},
		{"bad mapping file", map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_MED_IN_DIR": "/i", "OCS_CDR_MAPPING": bad}, "OCS_CDR_MAPPING"},
		{"missing mapping file", map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_MED_IN_DIR": "/i", "OCS_CDR_MAPPING": "/nope.json"}, "OCS_CDR_MAPPING"},
	}
	for _, c := range cases {
		_, err := LoadConfig(env(c.env))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	c, err := LoadConfig(env(map[string]string{"OCS_MED_OUT_DIR": "/o", "OCS_KAFKA_BROKERS": "b:9092"}))
	if err != nil || c.Topic != "ocs.mediation.cdr" || c.Mapping.Types[79] != "PGW" {
		t.Fatalf("%+v %v", c, err)
	}
}
