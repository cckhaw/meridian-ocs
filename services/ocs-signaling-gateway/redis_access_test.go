package main

import (
	"context"
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

const secret = "S3cret-redis-pw-9d1"

// The gateway's data tier is a Redis that needs a user, a password and TLS, and
// the gateway finds out at start-up when its credentials do not work.
func TestGatewayUsesAProtectedRedis(t *testing.T) {
	pki := tlstest.New(t)
	addr, _ := redistest.StartSecure(t, redistest.Secure{DisableDefaultUser: true, User: "ocs", UserPassword: secret,
		TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	try := func(env map[string]string) error {
		for _, k := range []string{"OCS_REDIS_USERNAME", "OCS_REDIS_PASSWORD", "OCS_REDIS_TLS", "OCS_REDIS_TLS_CA"} {
			t.Setenv(k, env[k]) // svcenv.OS reads the process environment
		}
		rdb, err := newRedis(envOf(map[string]string{"OCS_REDIS_ADDR": addr}))
		if err != nil {
			return err
		}
		defer rdb.Close()
		if err := svcenv.CheckRedisAccess(context.Background(), rdb); err != nil {
			return err
		}
		return rdb.Ping(context.Background()).Err()
	}
	good := map[string]string{"OCS_REDIS_USERNAME": "ocs", "OCS_REDIS_PASSWORD": secret, "OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile}
	if err := try(good); err != nil {
		t.Fatalf("with the right user, password and TLS: %v", err)
	}
	for name, drop := range map[string][]string{
		"no user":     {"OCS_REDIS_USERNAME"},
		"no password": {"OCS_REDIS_USERNAME", "OCS_REDIS_PASSWORD"},
		"no TLS":      {"OCS_REDIS_TLS", "OCS_REDIS_TLS_CA"},
	} {
		env := map[string]string{}
		for k, v := range good {
			env[k] = v
		}
		for _, k := range drop {
			delete(env, k)
		}
		err := try(env)
		if err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error leaks the password: %v", name, err)
		}
	}
}
