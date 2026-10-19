package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/dedup"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/ledger"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/pipeline"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/sink"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/source"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// App is the running service.
type App struct {
	cfg   Config
	logf  func(string, ...any)
	rdb   *redis.Client
	led   *ledger.Ledger
	files *source.Files
	kafka *source.Kafka
	rep   *catalog.Replicator
}

// NewApp connects the dependencies, repairs any output left half-published by
// an earlier crash, and builds the sources.
func NewApp(ctx context.Context, cfg Config, g svcenv.Getenv, logf func(string, ...any)) (*App, error) {
	a := &App{cfg: cfg, logf: logf}
	a.rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 32})
	fail := func(err error) (*App, error) { a.Close(); return nil, err }

	replicas, err := g.Int("OCS_REDIS_REPLICAS", 1)
	if err != nil {
		return fail(err)
	}
	wait, err := g.Duration("OCS_REDIS_WAIT_TIMEOUT", 50*time.Millisecond)
	if err != nil {
		return fail(err)
	}
	if replicas > 0 {
		have, err := datalayer.New(a.rdb).ConnectedReplicas(ctx)
		if err != nil {
			return fail(fmt.Errorf("cannot check the Redis replicas: %w", err))
		}
		if have < replicas {
			if g("OCS_ALLOW_NO_REPLICA") != "true" {
				return fail(fmt.Errorf("Redis has %d connected replica(s) but OCS_REDIS_REPLICAS=%d; connect them or set OCS_ALLOW_NO_REPLICA=true for development", have, replicas))
			}
			logf("WARNING %d replica(s) connected, %d required: the ledger is not durable", have, replicas)
		}
	} else {
		logf("WARNING ledger commits do not wait for replicas (OCS_REDIS_REPLICAS=0)")
	}
	a.led = ledger.New(a.rdb, "")
	a.led.Replicas, a.led.Timeout = replicas, wait

	dd, err := dedup.New(ctx, a.rdb, "", cfg.DedupWindow)
	if err != nil {
		return fail(fmt.Errorf("duplicate window: %w", err))
	}
	snk, err := sink.New(cfg.OutDir)
	if err != nil {
		return fail(err)
	}
	pub, dis, err := snk.Recover(ctx, a.led)
	if err != nil {
		return fail(fmt.Errorf("recovering staged output: %w", err))
	}
	if pub+dis > 0 {
		logf("recovered staged output: %d published, %d discarded", pub, dis)
	}

	dir := identity.NewCachedDirectory(identity.NewRedisDirectory(a.rdb, cfg.IdentityPfx), identity.CacheConfig{})
	p := &pipeline.Pipeline{
		Dedup: dd, Ledger: a.led, Sink: snk,
		Enricher: pipeline.IdentityEnricher{Resolver: identity.NewResolver(dir, identity.ConflictReject)},
	}

	if cfg.Rating == "catalog" {
		store := catalog.NewStore(0)
		a.rep = &catalog.Replicator{Redis: a.rdb, Store: store, OnError: func(err error) { logf("catalog sync: %v", err) }}
		if err := a.rep.Sync(ctx); err != nil {
			return fail(fmt.Errorf("loading the catalog: %w", err))
		}
		latest := store.Latest()
		if latest == nil || !latest.HasPlan(cfg.Plan) {
			return fail(fmt.Errorf("OCS_DEFAULT_PLAN %q is not in the catalog (latest version %d)", cfg.Plan, store.LatestVersion()))
		}
		p.Rater = pipeline.CatalogRater{Tariff: &catalog.Tariff{Store: store}, Plan: cfg.Plan}
		logf("rating with catalog (latest version %d), plan %q", store.LatestVersion(), cfg.Plan)
	}

	if cfg.InDir != "" {
		a.files, err = source.NewFiles(source.FileConfig{Dir: cfg.InDir, Settle: cfg.Settle, MaxFileBytes: cfg.MaxFileBytes,
			Mapping: cfg.Mapping, Logf: func(f string, a ...any) { logf("files: "+f, a...) }}, p, a.led)
		if err != nil {
			return fail(err)
		}
	}
	if len(cfg.Brokers) > 0 {
		a.kafka, err = source.NewKafka(source.KafkaConfig{Brokers: cfg.Brokers, Topic: cfg.Topic, Group: cfg.Group,
			Logf: func(f string, a ...any) { logf("kafka: "+f, a...) }}, p, a.led)
		if err != nil {
			return fail(err)
		}
	}
	return a, nil
}

// Run reads until ctx ends.
func (a *App) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	if a.rep != nil {
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.rep.Run(ctx) }()
	}
	if a.files != nil {
		a.logf("watching %s", a.cfg.InDir)
		wg.Add(1)
		go func() { defer wg.Done(); a.files.Run(ctx, a.cfg.ScanEvery) }()
	}
	var kerr error
	if a.kafka != nil {
		a.logf("consuming %s as group %s", a.cfg.Topic, a.cfg.Group)
		wg.Add(1)
		go func() { defer wg.Done(); kerr = a.kafka.Run(ctx) }()
	}
	<-ctx.Done()
	wg.Wait()
	if n := a.led.Unreplicated(); n > 0 {
		a.logf("%d ledger commit(s) were not confirmed by a replica", n)
	}
	return errors.Join(kerr)
}

// Close releases connections.
func (a *App) Close() {
	if a.kafka != nil {
		a.kafka.Close()
	}
	if a.rdb != nil {
		a.rdb.Close()
	}
}
