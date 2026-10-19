package main

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

// Config is the service configuration, read from the environment:
//
//	OCS_PROV_MODE            all (default) | api | worker
//	OCS_REDIS_ADDR           hot store                         (default 127.0.0.1:6379)
//	OCS_REDIS_REPLICAS, OCS_REDIS_WAIT_TIMEOUT, OCS_ALLOW_NO_REPLICA   see docs/architecture/07-durability.md
//	OCS_IDENTITY_PREFIX      identity directory key prefix     (default id:; must match the gateway)
//	OCS_KAFKA_BROKERS        required, comma separated
//	OCS_KAFKA_COMMANDS_TOPIC, OCS_KAFKA_EVENTS_TOPIC, OCS_KAFKA_DLQ_TOPIC, OCS_KAFKA_GROUP
//	OCS_CURRENCY             required: the deployment currency, e.g. EUR
//	OCS_PROV_MAX_TOPUP       largest single top-up, minor units  (default 10000000)
//	OCS_PROV_MAX_ADJUST      largest single adjustment           (default 1000000)
//	OCS_PROV_WORKERS         parallel command workers            (default 16)
//	OCS_PROV_REST_ADDR       REST listener, "off" = none         (default :8443)
//	OCS_PROV_GRPC_ADDR       gRPC listener, "off" = none         (default :9443)
//	OCS_PROVISIONING_AUTH    required: "mtls" or "off"
//	OCS_PROV_TLS_CERT, OCS_PROV_TLS_KEY, OCS_PROV_CLIENT_CA   server certificate and the CA of client certificates
//	OCS_PROV_CLIENTS         common names of standard callers (empty = any client of the CA)
//	OCS_PROV_PRIVILEGED      common names allowed to adjust, remove and change lifecycle state
type Config struct {
	Mode           string
	RedisAddr      string
	IdentityPrefix string
	Bus            bus.Config
	Limits         command.Limits
	Workers        int
	RESTAddr       string
	GRPCAddr       string
	Auth           string
	TLSCert        string
	TLSKey         string
	ClientCA       string
	Standard       []string
	Privileged     []string
}

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// LoadConfig reads and validates the configuration.
func LoadConfig(g svcenv.Getenv) (Config, error) {
	c := Config{
		Mode:           g.String("OCS_PROV_MODE", "all"),
		RedisAddr:      g.String("OCS_REDIS_ADDR", "127.0.0.1:6379"),
		IdentityPrefix: g.String("OCS_IDENTITY_PREFIX", "id:"),
		Bus: bus.Config{
			Brokers:       g.List("OCS_KAFKA_BROKERS"),
			CommandsTopic: g.String("OCS_KAFKA_COMMANDS_TOPIC", ""),
			EventsTopic:   g.String("OCS_KAFKA_EVENTS_TOPIC", ""),
			DLQTopic:      g.String("OCS_KAFKA_DLQ_TOPIC", ""),
			Group:         g.String("OCS_KAFKA_GROUP", ""),
		}.WithDefaults(),
		RESTAddr:   g.String("OCS_PROV_REST_ADDR", ":8443"),
		GRPCAddr:   g.String("OCS_PROV_GRPC_ADDR", ":9443"),
		Auth:       g("OCS_PROVISIONING_AUTH"),
		TLSCert:    g("OCS_PROV_TLS_CERT"),
		TLSKey:     g("OCS_PROV_TLS_KEY"),
		ClientCA:   g("OCS_PROV_CLIENT_CA"),
		Standard:   g.List("OCS_PROV_CLIENTS"),
		Privileged: g.List("OCS_PROV_PRIVILEGED"),
	}
	switch c.Mode {
	case "all", "api", "worker":
	default:
		return c, fmt.Errorf("OCS_PROV_MODE: want all, api or worker, got %q", c.Mode)
	}
	if err := c.Bus.Validate(); err != nil {
		return c, fmt.Errorf("OCS_KAFKA_BROKERS and topics: %w", err)
	}

	cur := g("OCS_CURRENCY")
	if !currencyRe.MatchString(cur) {
		return c, errors.New("OCS_CURRENCY is required and must be a three-letter currency code such as EUR")
	}
	c.Limits = command.Limits{Currency: cur}
	var err error
	if c.Limits.MaxTopUp, err = g.Int64("OCS_PROV_MAX_TOPUP", 10_000_000); err != nil {
		return c, err
	}
	if c.Limits.MaxAdjust, err = g.Int64("OCS_PROV_MAX_ADJUST", 1_000_000); err != nil {
		return c, err
	}
	if c.Limits.MaxTopUp == 0 || c.Limits.MaxAdjust == 0 {
		return c, errors.New("OCS_PROV_MAX_TOPUP and OCS_PROV_MAX_ADJUST must be positive")
	}
	if c.Workers, err = g.Int("OCS_PROV_WORKERS", 16); err != nil {
		return c, err
	}
	if c.Workers == 0 {
		return c, errors.New("OCS_PROV_WORKERS must be positive")
	}

	switch c.Auth {
	case "mtls":
		if c.TLSCert == "" || c.TLSKey == "" || c.ClientCA == "" {
			return c, errors.New("OCS_PROVISIONING_AUTH=mtls needs OCS_PROV_TLS_CERT, OCS_PROV_TLS_KEY and OCS_PROV_CLIENT_CA")
		}
	case "off":
		if len(c.Standard) > 0 || len(c.Privileged) > 0 {
			return c, errors.New("OCS_PROV_CLIENTS / OCS_PROV_PRIVILEGED have no effect with OCS_PROVISIONING_AUTH=off: refusing a configuration that looks restricted but is not")
		}
	default:
		return c, fmt.Errorf("OCS_PROVISIONING_AUTH must be set to \"mtls\" or \"off\", got %q", c.Auth)
	}
	return c, nil
}

func (c Config) runsAPI() bool    { return c.Mode == "all" || c.Mode == "api" }
func (c Config) runsWorker() bool { return c.Mode == "all" || c.Mode == "worker" }
