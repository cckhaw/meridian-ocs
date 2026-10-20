package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

const secret = "S3cret-redis-pw-9d1"

// The pipeline reaches a Redis that needs a password and TLS, and says so when
// it cannot (it must not run, or lose its ledger, against a Redis it cannot use).
func TestMediationUsesAProtectedRedis(t *testing.T) {
	pki := tlstest.New(t)
	addr, _ := redistest.StartSecure(t, redistest.Secure{Password: secret,
		TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	start := func(extra map[string]string) error {
		e := map[string]string{"OCS_MED_OUT_DIR": t.TempDir(), "OCS_MED_IN_DIR": t.TempDir(), "OCS_REDIS_ADDR": addr, "OCS_REDIS_REPLICAS": "0"}
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
		defer app.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		return app.Run(ctx)
	}
	tlsOn := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile}
	with := func(pw string) map[string]string {
		m := map[string]string{"OCS_REDIS_PASSWORD": pw}
		for k, v := range tlsOn {
			m[k] = v
		}
		return m
	}
	if err := start(with(secret)); err != nil {
		t.Fatalf("with the right password and TLS: %v", err)
	}
	for name, env := range map[string]map[string]string{"wrong password": with("nope"), "no password": tlsOn, "no TLS": {"OCS_REDIS_PASSWORD": secret}} {
		err := start(env)
		if err == nil {
			t.Errorf("%s: started anyway", name)
			continue
		}
		if strings.Contains(err.Error(), secret) && strings.Contains(err.Error(), "OCS_REDIS_PASSWORD=") {
			t.Errorf("%s: the error leaks a secret: %v", name, err)
		}
	}
}
