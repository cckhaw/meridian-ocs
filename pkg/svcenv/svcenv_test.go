package svcenv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
)

func env(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func TestHelpers(t *testing.T) {
	g := env(map[string]string{"S": "x", "I": "7", "NEG": "-1", "BAD": "x", "D": "250ms", "L": " a, b ,,c "})
	if g.String("S", "d") != "x" || g.String("MISSING", "d") != "d" {
		t.Error("String")
	}
	if n, err := g.Int("I", 1); err != nil || n != 7 {
		t.Error("Int")
	}
	if n, _ := g.Int("MISSING", 3); n != 3 {
		t.Error("Int default")
	}
	for _, k := range []string{"NEG", "BAD"} {
		if _, err := g.Int(k, 1); err == nil {
			t.Errorf("Int %s", k)
		}
		if _, err := g.Int64(k, 1); err == nil {
			t.Errorf("Int64 %s", k)
		}
	}
	if d, err := g.Duration("D", time.Second); err != nil || d != 250*time.Millisecond {
		t.Error("Duration")
	}
	if _, err := g.Duration("BAD", time.Second); err == nil {
		t.Error("Duration bad")
	}
	if got := g.List("L"); strings.Join(got, "|") != "a|b|c" || len(g.List("MISSING")) != 0 {
		t.Errorf("List %v", got)
	}
}

func TestConfigureDurability(t *testing.T) {
	primary := redistest.Start(t)
	var logs []string
	logf := func(f string, a ...any) { logs = append(logs, f) }
	ctx := context.Background()

	// A replica is required by default and the primary has none.
	if err := ConfigureDurability(ctx, env(nil), datalayer.New(primary), logf); err == nil || !strings.Contains(err.Error(), "0 connected replica") {
		t.Fatalf("%v", err)
	}
	// The development override starts anyway, with a warning.
	logs = nil
	if err := ConfigureDurability(ctx, env(map[string]string{"OCS_ALLOW_NO_REPLICA": "true"}), datalayer.New(primary), logf); err != nil || len(logs) != 1 || !strings.Contains(logs[0], "WARNING") {
		t.Fatalf("%v %v", err, logs)
	}
	// Waiting can be switched off, with a warning.
	logs = nil
	if err := ConfigureDurability(ctx, env(map[string]string{"OCS_REDIS_REPLICAS": "0"}), datalayer.New(primary), logf); err != nil || len(logs) != 1 {
		t.Fatalf("%v %v", err, logs)
	}
	// Bad settings are refused.
	if err := ConfigureDurability(ctx, env(map[string]string{"OCS_REDIS_WAIT_TIMEOUT": "soon"}), datalayer.New(primary), logf); err == nil {
		t.Fatal("bad timeout")
	}
	// With a replica it starts and the client waits.
	redistest.StartReplica(t, primary)
	store := datalayer.New(primary)
	if err := ConfigureDurability(ctx, env(nil), store, logf); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WithDurability(datalayer.Durability{Replicas: 1}); err != nil {
		t.Fatal(err)
	}
}
