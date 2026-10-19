package main

import (
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigRefusals(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"auth unset", map[string]string{}, `must be set to "mtls" or "off"`},
		{"auth typo", map[string]string{"OCS_CATALOG_AUTH": "mutual"}, `must be set to "mtls" or "off"`},
		{"mtls without certs", map[string]string{"OCS_CATALOG_AUTH": "mtls", "OCS_CATALOG_PUBLISHERS": "ed"}, "needs OCS_CATALOG_TLS_CERT"},
		{"mtls without publishers", map[string]string{"OCS_CATALOG_AUTH": "mtls", "OCS_CATALOG_TLS_CERT": "a", "OCS_CATALOG_TLS_KEY": "b", "OCS_CATALOG_CLIENT_CA": "c"}, "OCS_CATALOG_PUBLISHERS"},
		{"off with publishers", map[string]string{"OCS_CATALOG_AUTH": "off", "OCS_CATALOG_PUBLISHERS": "ed"}, "looks restricted but is not"},
		{"bad retain", map[string]string{"OCS_CATALOG_AUTH": "off", "OCS_CATALOG_RETAIN": "0"}, "RETAIN"},
		{"bad resync", map[string]string{"OCS_CATALOG_AUTH": "off", "OCS_CATALOG_RESYNC": "soon"}, "OCS_CATALOG_RESYNC"},
	}
	for _, c := range cases {
		_, err := LoadConfig(env(c.env))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	c, err := LoadConfig(env(map[string]string{"OCS_CATALOG_AUTH": "off"}))
	if err != nil || c.Addr != ":8444" || c.Retain != 64 {
		t.Fatalf("%+v %v", c, err)
	}
}
