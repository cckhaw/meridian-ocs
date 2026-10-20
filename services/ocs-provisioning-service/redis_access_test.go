package main

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
)

const secret = "S3cret-redis-pw-9d1"

// Provisioning reaches a Redis that needs a password and TLS, and refuses to
// start (instead of failing every request later) when it cannot.
func TestProvisioningUsesAProtectedRedis(t *testing.T) {
	pki := tlstest.New(t)
	addr, _ := redistest.StartSecure(t, redistest.Secure{Password: secret,
		TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	bc := bus.Config{}.WithDefaults()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, bc.CommandsTopic, bc.EventsTopic, bc.DLQTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	start := func(extra map[string]string) error {
		e := map[string]string{"OCS_KAFKA_BROKERS": cluster.ListenAddrs()[0], "OCS_REDIS_ADDR": addr, "OCS_REDIS_REPLICAS": "0",
			"OCS_CURRENCY": "EUR", "OCS_PROVISIONING_AUTH": "off", "OCS_PROV_REST_ADDR": "127.0.0.1:0", "OCS_PROV_GRPC_ADDR": "off"}
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
	good := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_PASSWORD": secret}
	if err := start(good); err != nil {
		t.Fatalf("with the right password and TLS: %v", err)
	}
	for name, env := range map[string]map[string]string{
		"wrong password": {"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_PASSWORD": "nope"},
		"no password":    {"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile},
		"no TLS":         {"OCS_REDIS_PASSWORD": secret},
		"untrusted CA":   {"OCS_REDIS_TLS": "on", "OCS_REDIS_PASSWORD": secret},
	} {
		err := start(env)
		if err == nil {
			t.Errorf("%s: started anyway", name)
		} else if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error leaks the password: %v", name, err)
		}
	}
}
