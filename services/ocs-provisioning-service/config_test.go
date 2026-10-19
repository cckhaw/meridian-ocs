package main

import (
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
)

func env(m map[string]string) svcenv.Getenv { return func(k string) string { return m[k] } }

func valid() map[string]string {
	return map[string]string{
		"OCS_KAFKA_BROKERS": "k1:9092,k2:9092", "OCS_CURRENCY": "EUR", "OCS_PROVISIONING_AUTH": "mtls",
		"OCS_PROV_TLS_CERT": "/c.pem", "OCS_PROV_TLS_KEY": "/k.pem", "OCS_PROV_CLIENT_CA": "/ca.pem",
	}
}

func TestValidConfigAndDefaults(t *testing.T) {
	c, err := LoadConfig(env(valid()))
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != "all" || c.RedisAddr != "127.0.0.1:6379" || c.IdentityPrefix != "id:" || c.Workers != 16 ||
		c.RESTAddr != ":8443" || c.GRPCAddr != ":9443" || len(c.Bus.Brokers) != 2 || c.Bus.CommandsTopic != "ocs.provisioning.commands" ||
		c.Bus.Group != "ocs-provisioning" || c.Limits.Currency != "EUR" || c.Limits.MaxTopUp != 10_000_000 || c.Limits.MaxAdjust != 1_000_000 {
		t.Fatalf("%+v", c)
	}
	if !c.runsAPI() || !c.runsWorker() {
		t.Fatal("mode all runs both")
	}
	m := valid()
	m["OCS_PROV_MODE"] = "api"
	if c, _ = LoadConfig(env(m)); c.runsWorker() || !c.runsAPI() {
		t.Fatal("api mode")
	}
	m["OCS_PROV_MODE"] = "worker"
	if c, _ = LoadConfig(env(m)); !c.runsWorker() || c.runsAPI() {
		t.Fatal("worker mode")
	}
}

func TestUnsafeOrIncompleteConfigurationsAreRefused(t *testing.T) {
	tests := []struct {
		name string
		mod  func(map[string]string)
		want string
	}{
		{"no brokers", func(m map[string]string) { delete(m, "OCS_KAFKA_BROKERS") }, "broker"},
		{"no currency", func(m map[string]string) { delete(m, "OCS_CURRENCY") }, "OCS_CURRENCY"},
		{"lower-case currency", func(m map[string]string) { m["OCS_CURRENCY"] = "eur" }, "OCS_CURRENCY"},
		{"long currency", func(m map[string]string) { m["OCS_CURRENCY"] = "EURO" }, "OCS_CURRENCY"},
		{"authentication left unset", func(m map[string]string) { delete(m, "OCS_PROVISIONING_AUTH") }, "OCS_PROVISIONING_AUTH"},
		{"authentication value invalid", func(m map[string]string) { m["OCS_PROVISIONING_AUTH"] = "basic" }, "OCS_PROVISIONING_AUTH"},
		{"mtls without a certificate", func(m map[string]string) { delete(m, "OCS_PROV_TLS_CERT") }, "mtls needs"},
		{"mtls without a client CA", func(m map[string]string) { delete(m, "OCS_PROV_CLIENT_CA") }, "mtls needs"},
		{"client lists with authentication off", func(m map[string]string) {
			m["OCS_PROVISIONING_AUTH"] = "off"
			m["OCS_PROV_PRIVILEGED"] = "care"
		}, "no effect"},
		{"bad mode", func(m map[string]string) { m["OCS_PROV_MODE"] = "both" }, "OCS_PROV_MODE"},
		{"zero workers", func(m map[string]string) { m["OCS_PROV_WORKERS"] = "0" }, "WORKERS"},
		{"zero top-up limit", func(m map[string]string) { m["OCS_PROV_MAX_TOPUP"] = "0" }, "positive"},
		{"non-numeric limit", func(m map[string]string) { m["OCS_PROV_MAX_ADJUST"] = "lots" }, "OCS_PROV_MAX_ADJUST"},
	}
	for _, tc := range tests {
		m := valid()
		tc.mod(m)
		_, err := LoadConfig(env(m))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v (want something about %q)", tc.name, err, tc.want)
		}
	}
}

func TestAuthenticationCanBeSwitchedOffExplicitly(t *testing.T) {
	m := valid()
	m["OCS_PROVISIONING_AUTH"] = "off"
	for _, k := range []string{"OCS_PROV_TLS_CERT", "OCS_PROV_TLS_KEY", "OCS_PROV_CLIENT_CA"} {
		delete(m, k)
	}
	if c, err := LoadConfig(env(m)); err != nil || c.Auth != "off" {
		t.Fatalf("%+v %v", c, err)
	}
}
