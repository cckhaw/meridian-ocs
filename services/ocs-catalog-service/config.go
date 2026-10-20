package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
)

// Config is read from the environment.
//
//	OCS_REDIS_ADDR            catalog system of record          (default 127.0.0.1:6379)
//	OCS_REDIS_USERNAME, OCS_REDIS_PASSWORD or OCS_REDIS_PASSWORD_FILE, OCS_REDIS_TLS=on and
//	OCS_REDIS_TLS_CA / _CERT / _KEY / _SERVER_NAME: access control for Redis (svcenv.ApplyRedisAccess)
//	OCS_CATALOG_ADDR          REST listener                      (default :8444)
//	OCS_CATALOG_AUTH          required: "mtls" or "off"
//	OCS_CATALOG_TLS_CERT, OCS_CATALOG_TLS_KEY, OCS_CATALOG_CLIENT_CA
//	OCS_CATALOG_PUBLISHERS    client certificate common names allowed to publish (required with mtls)
//	OCS_CATALOG_RETAIN        past versions kept for time travel (default 64)
//	OCS_CATALOG_RESYNC        periodic resync interval           (default 30s)
type Config struct {
	RedisAddr  string
	Addr       string
	Auth       string
	TLSCert    string
	TLSKey     string
	ClientCA   string
	Publishers []string
	Retain     int
	Resync     time.Duration
}

// LoadConfig reads and validates the configuration.
func LoadConfig(g svcenv.Getenv) (Config, error) {
	c := Config{
		RedisAddr:  g.String("OCS_REDIS_ADDR", "127.0.0.1:6379"),
		Addr:       g.String("OCS_CATALOG_ADDR", ":8444"),
		Auth:       g("OCS_CATALOG_AUTH"),
		TLSCert:    g("OCS_CATALOG_TLS_CERT"),
		TLSKey:     g("OCS_CATALOG_TLS_KEY"),
		ClientCA:   g("OCS_CATALOG_CLIENT_CA"),
		Publishers: g.List("OCS_CATALOG_PUBLISHERS"),
	}
	var err error
	if c.Retain, err = g.Int("OCS_CATALOG_RETAIN", 64); err != nil {
		return c, err
	}
	if c.Retain < 1 {
		return c, errors.New("OCS_CATALOG_RETAIN must be positive")
	}
	if c.Resync, err = g.Duration("OCS_CATALOG_RESYNC", 30*time.Second); err != nil {
		return c, err
	}
	if c.Resync <= 0 {
		return c, errors.New("OCS_CATALOG_RESYNC must be positive")
	}
	switch c.Auth {
	case "mtls":
		if c.TLSCert == "" || c.TLSKey == "" || c.ClientCA == "" {
			return c, errors.New("OCS_CATALOG_AUTH=mtls needs OCS_CATALOG_TLS_CERT, OCS_CATALOG_TLS_KEY and OCS_CATALOG_CLIENT_CA")
		}
		if len(c.Publishers) == 0 {
			return c, errors.New("OCS_CATALOG_AUTH=mtls needs OCS_CATALOG_PUBLISHERS: at least one client allowed to publish")
		}
	case "off":
		if len(c.Publishers) > 0 {
			return c, errors.New("OCS_CATALOG_PUBLISHERS has no effect with OCS_CATALOG_AUTH=off: refusing a configuration that looks restricted but is not")
		}
	default:
		return c, fmt.Errorf(`OCS_CATALOG_AUTH must be set to "mtls" or "off" (got %q); "off" is for local development only`, c.Auth)
	}
	return c, nil
}
