package main

import (
	"context"
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

const secret = "S3cret-redis-pw-9d1"

// The catalog service loads its catalog from a Redis that needs a user, a
// password and TLS, and refuses to start when it cannot.
func TestCatalogServiceUsesAProtectedRedis(t *testing.T) {
	pki := tlstest.New(t)
	addr, _ := redistest.StartSecure(t, redistest.Secure{DisableDefaultUser: true, User: "ocs", UserPassword: secret,
		TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	start := func(extra map[string]string) error {
		e := map[string]string{"OCS_REDIS_ADDR": addr, "OCS_CATALOG_ADDR": "127.0.0.1:0", "OCS_CATALOG_AUTH": "off"}
		for k, v := range extra {
			e[k] = v
		}
		g := func(k string) string { return e[k] }
		cfg, err := LoadConfig(g)
		if err != nil {
			return err
		}
		app, err := NewApp(context.Background(), cfg, g, t.Logf)
		if err != nil {
			return err
		}
		app.Close()
		return nil
	}
	good := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_USERNAME": "ocs", "OCS_REDIS_PASSWORD": secret}
	if err := start(good); err != nil {
		t.Fatalf("with the right user, password and TLS: %v", err)
	}
	for name, mutate := range map[string]func(map[string]string){
		"wrong password": func(m map[string]string) { m["OCS_REDIS_PASSWORD"] = "nope" },
		"wrong user":     func(m map[string]string) { m["OCS_REDIS_USERNAME"] = "someone" },
		"no credentials": func(m map[string]string) { delete(m, "OCS_REDIS_USERNAME"); delete(m, "OCS_REDIS_PASSWORD") },
		"no TLS":         func(m map[string]string) { delete(m, "OCS_REDIS_TLS"); delete(m, "OCS_REDIS_TLS_CA") },
	} {
		env := map[string]string{}
		for k, v := range good {
			env[k] = v
		}
		mutate(env)
		err := start(env)
		if err == nil {
			t.Errorf("%s: started anyway", name)
		} else if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error leaks the password: %v", name, err)
		}
	}
}
