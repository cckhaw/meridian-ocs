// Package svcenv holds the environment-variable helpers and the shared
// start-up checks of the OCS services.
package svcenv

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
)

// Getenv is os.Getenv; tests substitute their own.
type Getenv func(string) string

// String returns the variable or def.
func (g Getenv) String(key, def string) string {
	if v := g(key); v != "" {
		return v
	}
	return def
}

// Int returns a non-negative integer variable or def.
func (g Getenv) Int(key string, def int) (int, error) {
	v := g(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: want a non-negative integer, got %q", key, v)
	}
	return n, nil
}

// Int64 returns a non-negative int64 variable or def.
func (g Getenv) Int64(key string, def int64) (int64, error) {
	v := g(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: want a non-negative integer, got %q", key, v)
	}
	return n, nil
}

// Duration returns a positive duration variable or def.
func (g Getenv) Duration(key string, def time.Duration) (time.Duration, error) {
	v := g(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: want a positive duration, got %q", key, v)
	}
	return d, nil
}

// List splits a comma separated variable, dropping blanks.
func (g Getenv) List(key string) []string {
	var out []string
	for _, p := range strings.Split(g(key), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// OS reads the real environment.
var OS Getenv = os.Getenv

// ConfigureDurability makes committing writes wait for replica acknowledgement
// (docs/architecture/07-durability.md):
//
//	OCS_REDIS_REPLICAS       replicas that must acknowledge a commit (default 1; 0 = off)
//	OCS_REDIS_WAIT_TIMEOUT   how long to wait for them               (default 25ms)
//	OCS_ALLOW_NO_REPLICA     "true" starts with fewer replicas than required (development only)
//
// It refuses to proceed if fewer replicas are connected than required, unless
// the override is set.
func ConfigureDurability(ctx context.Context, g Getenv, store *datalayer.Client, logf func(string, ...any)) error {
	replicas, err := g.Int("OCS_REDIS_REPLICAS", 1)
	if err != nil {
		return err
	}
	timeout, err := g.Duration("OCS_REDIS_WAIT_TIMEOUT", 25*time.Millisecond)
	if err != nil {
		return err
	}
	if replicas == 0 {
		logf("WARNING commits do not wait for replicas (OCS_REDIS_REPLICAS=0)")
		return nil
	}
	if _, err := store.WithDurability(datalayer.Durability{Replicas: replicas, Timeout: timeout}); err != nil {
		return err
	}
	have, err := store.ConnectedReplicas(ctx)
	if err != nil {
		return fmt.Errorf("cannot check the Redis replicas: %w", err)
	}
	if have < replicas {
		if g("OCS_ALLOW_NO_REPLICA") == "true" {
			logf("WARNING %d replica(s) connected, %d required: commits are not durable", have, replicas)
			return nil
		}
		return fmt.Errorf("Redis has %d connected replica(s) but OCS_REDIS_REPLICAS=%d; connect them or set OCS_ALLOW_NO_REPLICA=true for development", have, replicas)
	}
	logf("commits wait for %d replica(s), timeout %s", replicas, timeout)
	return nil
}
