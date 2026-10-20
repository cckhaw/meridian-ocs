package main

import (
	"errors"
	"fmt"

	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
)

// Config is read from the environment.
//
//	OCS_AIOPS_ADDR       listen address                     (default :8445)
//	OCS_AIOPS_AUTH       required: "mtls" or "off"
//	OCS_AIOPS_TLS_CERT, OCS_AIOPS_TLS_KEY, OCS_AIOPS_CLIENT_CA   (mtls)
//
// Any client holding a certificate signed by the client CA may use the service:
// it only validates and tests mapping documents and changes nothing.
type Config struct {
	Addr, Auth, TLSCert, TLSKey, ClientCA string
}

// LoadConfig reads and validates the configuration.
func LoadConfig(g svcenv.Getenv) (Config, error) {
	c := Config{
		Addr:     g.String("OCS_AIOPS_ADDR", ":8445"),
		Auth:     g("OCS_AIOPS_AUTH"),
		TLSCert:  g("OCS_AIOPS_TLS_CERT"),
		TLSKey:   g("OCS_AIOPS_TLS_KEY"),
		ClientCA: g("OCS_AIOPS_CLIENT_CA"),
	}
	switch c.Auth {
	case "mtls":
		if c.TLSCert == "" || c.TLSKey == "" || c.ClientCA == "" {
			return c, errors.New("OCS_AIOPS_AUTH=mtls needs OCS_AIOPS_TLS_CERT, OCS_AIOPS_TLS_KEY and OCS_AIOPS_CLIENT_CA")
		}
	case "off":
	default:
		return c, fmt.Errorf(`OCS_AIOPS_AUTH must be set to "mtls" or "off" (got %q); "off" is for local development only`, c.Auth)
	}
	return c, nil
}
