package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
)

// newTariff builds the pricing source from the environment.
//
//	OCS_TARIFF=static   (default) one flat cash price, OCS_CASH_BYTES_RATE
//	OCS_TARIFF=catalog  versioned rules published by ocs-catalog-service
//	  OCS_DEFAULT_PLAN        plan for subscribers with no offering (required)
//	  OCS_PLAN_CACHE_TTL      how long a subscriber's offering is cached (default 30s)
//	  OCS_CATALOG_RETAIN      versions kept for time travel (default 64)
//	  OCS_CATALOG_RESYNC      periodic resync interval      (default 30s)
//
// In catalog mode the gateway refuses to start unless a catalog version is in
// effect and the default plan exists in it, and afterwards follows new
// versions without a restart. A sync failure keeps the last good catalog.
// A subscriber's plan comes from the offering on its profile (set by the
// provisioning service) via the catalog; one with no offering is priced on the
// default plan. A failed profile lookup is never answered with the default
// plan: the unit is priced with no rates instead (no new quota).
func newTariff(ctx context.Context, env func(string, string) string, rdb redis.UniversalClient, reg *telemetry.Registry) (engine.Tariff, func(), error) {
	switch mode := env("OCS_TARIFF", "static"); mode {
	case "static":
		cash, err := parseRate(env("OCS_CASH_BYTES_RATE", "1/1024"))
		if err != nil {
			return nil, nil, err
		}
		return engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: cash}}, func() {}, nil
	case "catalog":
		plan := env("OCS_DEFAULT_PLAN", "")
		if plan == "" {
			return nil, nil, errors.New("OCS_TARIFF=catalog needs OCS_DEFAULT_PLAN")
		}
		retain, err := envInt("OCS_CATALOG_RETAIN", 64)
		if err != nil || retain < 1 {
			return nil, nil, errors.New("OCS_CATALOG_RETAIN must be a positive number")
		}
		resync, err := time.ParseDuration(env("OCS_CATALOG_RESYNC", "30s"))
		if err != nil || resync <= 0 {
			return nil, nil, errors.New("OCS_CATALOG_RESYNC must be a positive duration such as 30s")
		}
		store := catalog.NewStore(retain)
		rep := &catalog.Replicator{Redis: rdb, Store: store, Resync: resync,
			OnError: func(err error) { log.Printf("ocs-signaling-gateway: catalog sync: %v", err) }}
		if err := rep.Sync(ctx); err != nil {
			return nil, nil, fmt.Errorf("loading the catalog: %w", err)
		}
		rep.RegisterMetrics(reg)
		cur := store.Current()
		if cur == nil {
			return nil, nil, errors.New("OCS_TARIFF=catalog but no catalog version is in effect: publish one with ocs-catalog-service first")
		}
		if !cur.HasPlan(plan) {
			return nil, nil, fmt.Errorf("OCS_DEFAULT_PLAN %q does not exist in catalog version %d", plan, cur.Version)
		}
		ttl, err := time.ParseDuration(env("OCS_PLAN_CACHE_TTL", "30s"))
		if err != nil || ttl <= 0 {
			return nil, nil, errors.New("OCS_PLAN_CACHE_TTL must be a positive duration such as 30s")
		}
		resolver := catalog.NewProfileResolver(datalayer.New(rdb), store)
		resolver.TTL = ttl
		log.Printf("ocs-signaling-gateway: tariff from catalog version %d (latest %d), default plan %q", cur.Version, store.LatestVersion(), plan)
		rctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = rep.Run(rctx) }()
		return &catalog.Tariff{Store: store, Resolver: resolver, DefaultPlan: plan}, func() { cancel(); wg.Wait() }, nil
	default:
		return nil, nil, fmt.Errorf(`OCS_TARIFF: want "static" or "catalog", got %q`, mode)
	}
}
