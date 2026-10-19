package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr"
)

// Config is read from the environment.
//
//	OCS_MED_OUT_DIR            output directory (required)
//	OCS_MED_IN_DIR             watch this directory for CDR files (*.ber *.dat *.cdr, *.jsonl)
//	OCS_KAFKA_BROKERS          consume normalized JSON CDRs from Kafka
//	OCS_MED_KAFKA_TOPIC        (default ocs.mediation.cdr)
//	OCS_MED_KAFKA_GROUP        (default ocs-mediation)
//	                           at least one of OCS_MED_IN_DIR and OCS_KAFKA_BROKERS is required
//	OCS_MED_SCAN_INTERVAL      directory scan interval            (default 10s)
//	OCS_MED_SETTLE             a file must be unchanged this long (default 5s)
//	OCS_MED_MAX_FILE_BYTES     larger files are quarantined       (default 268435456)
//	OCS_MED_DEDUP_WINDOW       how long duplicates are remembered (default 72h)
//	OCS_REDIS_ADDR             ledger, duplicate window, identity directory (default 127.0.0.1:6379)
//	OCS_REDIS_REPLICAS, OCS_REDIS_WAIT_TIMEOUT, OCS_ALLOW_NO_REPLICA  see docs/architecture/07-durability.md
//	OCS_IDENTITY_PREFIX        identity directory key prefix      (default id:)
//	OCS_CDR_MAPPING            JSON file overriding the PGW BER field mapping
//	OCS_MED_RATING             "off" (default) or "catalog": add a list-price charge
//	OCS_DEFAULT_PLAN           price plan used for rating (required with catalog rating)
type Config struct {
	OutDir       string
	InDir        string
	Brokers      []string
	Topic, Group string
	ScanEvery    time.Duration
	Settle       time.Duration
	MaxFileBytes int64
	DedupWindow  time.Duration
	RedisAddr    string
	IdentityPfx  string
	Mapping      cdr.Mapping
	Rating       string
	Plan         string
}

// LoadConfig reads and validates the configuration.
func LoadConfig(g svcenv.Getenv) (Config, error) {
	c := Config{
		OutDir:      g("OCS_MED_OUT_DIR"),
		InDir:       g("OCS_MED_IN_DIR"),
		Brokers:     g.List("OCS_KAFKA_BROKERS"),
		Topic:       g.String("OCS_MED_KAFKA_TOPIC", "ocs.mediation.cdr"),
		Group:       g.String("OCS_MED_KAFKA_GROUP", "ocs-mediation"),
		RedisAddr:   g.String("OCS_REDIS_ADDR", "127.0.0.1:6379"),
		IdentityPfx: g.String("OCS_IDENTITY_PREFIX", "id:"),
		Mapping:     cdr.PGWMapping(),
		Rating:      g.String("OCS_MED_RATING", "off"),
		Plan:        g("OCS_DEFAULT_PLAN"),
	}
	var err error
	if c.ScanEvery, err = g.Duration("OCS_MED_SCAN_INTERVAL", 10*time.Second); err != nil {
		return c, err
	}
	if c.Settle, err = g.Duration("OCS_MED_SETTLE", 5*time.Second); err != nil {
		return c, err
	}
	if c.DedupWindow, err = g.Duration("OCS_MED_DEDUP_WINDOW", 72*time.Hour); err != nil {
		return c, err
	}
	if c.MaxFileBytes, err = g.Int64("OCS_MED_MAX_FILE_BYTES", 256<<20); err != nil {
		return c, err
	}
	if c.MaxFileBytes <= 0 {
		return c, errors.New("OCS_MED_MAX_FILE_BYTES must be positive")
	}
	if c.OutDir == "" {
		return c, errors.New("OCS_MED_OUT_DIR is required")
	}
	if c.InDir == "" && len(c.Brokers) == 0 {
		return c, errors.New("nothing to read: set OCS_MED_IN_DIR and/or OCS_KAFKA_BROKERS")
	}
	switch c.Rating {
	case "off":
	case "catalog":
		if c.Plan == "" {
			return c, errors.New("OCS_MED_RATING=catalog needs OCS_DEFAULT_PLAN")
		}
	default:
		return c, fmt.Errorf(`OCS_MED_RATING: want "off" or "catalog", got %q`, c.Rating)
	}
	if p := g("OCS_CDR_MAPPING"); p != "" {
		raw, err := os.ReadFile(p)
		if err != nil {
			return c, fmt.Errorf("OCS_CDR_MAPPING: %w", err)
		}
		var m cdr.Mapping
		dec := json.NewDecoder(bytesReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&m); err != nil {
			return c, fmt.Errorf("OCS_CDR_MAPPING %s: %w", p, err)
		}
		if len(m.Types) == 0 || len(m.Fields) == 0 {
			return c, fmt.Errorf("OCS_CDR_MAPPING %s: needs at least one record type and one field", p)
		}
		c.Mapping = m
	}
	return c, nil
}
