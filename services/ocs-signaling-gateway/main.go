// Command ocs-signaling-gateway terminates network-facing charging protocols
// (4G Gy/Ro over Diameter/TCP, 5G Nchf_ConvergedCharging over HTTP/2) and
// hands canonical requests to the rating engine.
//
// This binary runs the rating engine in-process (the "co-located" mode of the
// design): no network hop between adapter and engine. A remote mode over gRPC
// will be added with the generated rating.proto stubs.
//
// Configuration (environment):
//
//	OCS_GY_ADDR          Diameter listen address, "off" = none (default :3868)
//	OCS_NCHF_ADDR        Nchf HTTP/2 listen address, "off"     (default :8000)
//	OCS_NCHF_TLS_CERT    server certificate; with OCS_NCHF_TLS_KEY enables TLS
//	OCS_NCHF_CLIENT_CA   CA file; when set, client certificates are required (mTLS)
//	OCS_NCHF_OAUTH       "required" or "off"; no default, the gateway will not
//	                     start the Nchf listener until you choose
//	OCS_NCHF_OAUTH_KEYS  PEM file with the NRF's access-token signing certificate(s)
//	                     or public key(s); reloaded when the file changes
//	OCS_NCHF_OAUTH_AUDIENCE  accepted `aud` values, comma separated
//	                     (default: CHF, plus OCS_NF_INSTANCE_ID if set)
//	OCS_NCHF_OAUTH_ISSUERS   optional allow-list of `iss` (NRF instance ids)
//	OCS_NCHF_OAUTH_SCOPE scope that must be granted (default nchf-convergedcharging)
//	OCS_REGION           region embedded in charging references (default r1)
//	OCS_IDENTITY_MODE    "directory" (default) or "direct" (see below)
//	OCS_IDENTITY_PREFIX  Redis key prefix of the identity directory (default id:)
//	OCS_IDENTITY_CACHE_TTL  how long a binding is cached     (default 30s)
//	OCS_IDENTITY_ON_CONFLICT  reject | prefer-first          (default reject)
//	OCS_ORIGIN_HOST      Diameter identity, required          (e.g. ocs1.example.net)
//	OCS_ORIGIN_REALM     Diameter realm, required
//	OCS_HOST_IP          address advertised in CEA, required
//	OCS_REDIS_ADDR       hot-state store                      (default 127.0.0.1:6379)
//	OCS_REDIS_REPLICAS   replicas that must acknowledge a commit     (default 1)
//	OCS_REDIS_WAIT_TIMEOUT  how long to wait for them             (default 25ms)
//	OCS_ALLOW_NO_REPLICA "true" starts without the required replicas (development only)
//	OCS_MAX_TPS          admission rate limit, 0 = unlimited  (default 0; shared by Gy and Nchf)
//	OCS_MAX_INFLIGHT     concurrent request limit, 0 = off    (default 10000)
//	OCS_TARIFF           "static" or "catalog"                (default static; see newTariff)
//	OCS_CASH_BYTES_RATE  static tariff: cash price as num/den (default 1/1024)
//	OCS_ADMIN_ADDR       /healthz, /readyz and /metrics       (default :8080; keep it cluster-internal)
//	OCS_LOG_LEVEL        debug | info | warn | error          (default info; JSON logs)
//	OCS_CONTROL_ADDR     guarded AI Ops control plane (gRPC); unset = none. With it:
//	                     OCS_CONTROL_AUTH=mtls|off, OCS_CONTROL_TLS_CERT/KEY/CLIENT_CA,
//	                     OCS_CONTROL_AGENTS, OCS_CONTROL_APPROVERS (docs/architecture/11-aiops.md)
//
// Nchf authorization: with OCS_NCHF_OAUTH=required every request must carry an
// NRF-issued OAuth2 access token (Authorization: Bearer ...), validated against
// the configured NRF key(s); see docs/architecture/05-nchf-adapter.md. Tokens
// are bearer credentials: run the listener with TLS (or inside a mesh that
// terminates it), or they can be stolen from the network.
//
// Durability: committing steps (updates, terminates, events) wait for
// OCS_REDIS_REPLICAS replicas to acknowledge the write before answering, so a
// primary failure right after the answer does not lose the charge. The gateway
// refuses to start if fewer replicas are connected, unless
// OCS_ALLOW_NO_REPLICA=true. If a replica stops answering, waiting is suspended
// after a few shortfalls (so it does not add the timeout to every commit) and
// the shortfalls are counted; see docs/architecture/07-durability.md.
//
// Identity: in "directory" mode every MSISDN / IMSI / NAI is resolved through
// the Redis identity directory (keys id:msisdn:<digits>, id:imsi:<digits>,
// id:nai:<nai>, value = subscriber key) to the one subscriber key balances are
// stored under, so a subscriber is charged against the same state over Gy and
// Nchf. Unknown identities are refused. "direct" mode uses the raw identifier
// as the key and does NOT unify identities; for development only.
//
// The cash rate is a placeholder until the catalog service exists.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/control"
	"github.com/cckhaw/meridian-ocs/pkg/control/controlrpc"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/failpolicy"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/service"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/gy"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/nchf"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/oauth"
)

func env(key, def string) string { return svcenv.OS.String(key, def) }

func envInt(key string, def int) (int, error) { return svcenv.OS.Int(key, def) }

func parseRate(s string) (engine.Frac, error) {
	num, den, ok := strings.Cut(s, "/")
	if !ok {
		return engine.Frac{}, fmt.Errorf("OCS_CASH_BYTES_RATE: want num/den, got %q", s)
	}
	n, err1 := strconv.ParseInt(num, 10, 64)
	d, err2 := strconv.ParseInt(den, 10, 64)
	if err1 != nil || err2 != nil || n <= 0 || d <= 0 || n > datalayer.MaxNum || d > datalayer.MaxDen {
		return engine.Frac{}, fmt.Errorf("OCS_CASH_BYTES_RATE: %q out of range (num <= %d, den <= %d)", s, datalayer.MaxNum, datalayer.MaxDen)
	}
	return engine.Frac{Num: n, Den: d}, nil
}

// newAuthenticator builds the Nchf access-token verifier. There is no default:
// running an unauthenticated charging API has to be a deliberate choice.
func newAuthenticator(ctx context.Context, tlsOn bool) (nchf.Authenticator, error) {
	switch mode := os.Getenv("OCS_NCHF_OAUTH"); mode {
	case "off":
		log.Printf("ocs-signaling-gateway: WARNING Nchf OAuth2 is OFF: any caller that can reach the listener can charge subscribers")
		return nil, nil
	case "required":
	default:
		return nil, fmt.Errorf("OCS_NCHF_OAUTH must be set to \"required\" (with OCS_NCHF_OAUTH_KEYS) or \"off\", got %q", mode)
	}
	path := os.Getenv("OCS_NCHF_OAUTH_KEYS")
	if path == "" {
		return nil, errors.New("OCS_NCHF_OAUTH=required needs OCS_NCHF_OAUTH_KEYS (PEM file with the NRF signing certificate or public key)")
	}
	ks, skipped, err := oauth.LoadKeySet(path, time.Now())
	if err != nil {
		return nil, fmt.Errorf("OCS_NCHF_OAUTH_KEYS: %w", err)
	}
	for _, s := range skipped {
		log.Printf("ocs-signaling-gateway: WARNING OAuth key file: skipped %s", s)
	}
	audience := splitList(os.Getenv("OCS_NCHF_OAUTH_AUDIENCE"))
	if len(audience) == 0 {
		audience = []string{"CHF"}
		if id := os.Getenv("OCS_NF_INSTANCE_ID"); id != "" {
			audience = append(audience, id)
		}
	}
	v, err := oauth.NewVerifier(oauth.Config{
		Audience: audience,
		Scope:    env("OCS_NCHF_OAUTH_SCOPE", oauth.DefaultScope),
		Issuers:  splitList(os.Getenv("OCS_NCHF_OAUTH_ISSUERS")),
	}, ks)
	if err != nil {
		return nil, err
	}
	go oauth.WatchKeyFile(ctx, path, v, 30*time.Second, log.Printf)
	log.Printf("ocs-signaling-gateway: Nchf OAuth2 on: %d key(s), audience %v", ks.Len(), audience)
	if !tlsOn {
		log.Printf("ocs-signaling-gateway: WARNING OAuth2 is on but the Nchf listener has no TLS: bearer tokens cross the network in cleartext unless a mesh terminates TLS")
	}
	return v, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newResolver(rdb *redis.Client) (*identity.Resolver, error) {
	switch mode := env("OCS_IDENTITY_MODE", "directory"); mode {
	case "direct":
		log.Printf("ocs-signaling-gateway: WARNING identity mode \"direct\": MSISDN and IMSI of one subscriber are NOT unified")
		return identity.NewPassthroughResolver(), nil
	case "directory":
		ttl, err := time.ParseDuration(env("OCS_IDENTITY_CACHE_TTL", "30s"))
		if err != nil || ttl <= 0 {
			return nil, fmt.Errorf("OCS_IDENTITY_CACHE_TTL: want a positive duration, got %q", os.Getenv("OCS_IDENTITY_CACHE_TTL"))
		}
		policy := identity.ConflictReject
		switch c := env("OCS_IDENTITY_ON_CONFLICT", "reject"); c {
		case "reject":
		case "prefer-first":
			policy = identity.ConflictPreferFirst
		default:
			return nil, fmt.Errorf("OCS_IDENTITY_ON_CONFLICT: want reject or prefer-first, got %q", c)
		}
		dir := identity.NewRedisDirectory(rdb, env("OCS_IDENTITY_PREFIX", "id:"))
		return identity.NewResolver(identity.NewCachedDirectory(dir, identity.CacheConfig{TTL: ttl}), policy), nil
	default:
		return nil, fmt.Errorf("OCS_IDENTITY_MODE: want directory or direct, got %q", mode)
	}
}

func run(ctx context.Context, ready func()) error {
	host, realm := os.Getenv("OCS_ORIGIN_HOST"), os.Getenv("OCS_ORIGIN_REALM")
	ip := net.ParseIP(os.Getenv("OCS_HOST_IP"))
	if host == "" || realm == "" || ip == nil {
		return fmt.Errorf("OCS_ORIGIN_HOST, OCS_ORIGIN_REALM and a valid OCS_HOST_IP are required")
	}
	maxTPS, err := envInt("OCS_MAX_TPS", 0)
	if err != nil {
		return err
	}
	maxInflight, err := envInt("OCS_MAX_INFLIGHT", 10000)
	if err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         env("OCS_REDIS_ADDR", "127.0.0.1:6379"),
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  250 * time.Millisecond, // bounds every data-tier call
		WriteTimeout: 250 * time.Millisecond,
		PoolSize:     256,
		MinIdleConns: 16,
	})
	defer rdb.Close()
	store := datalayer.New(rdb)
	if err := svcenv.ConfigureDurability(ctx, svcenv.OS, store, func(f string, a ...any) { log.Printf("ocs-signaling-gateway: "+f, a...) }); err != nil {
		return err
	}
	resolver, err := newResolver(rdb)
	if err != nil {
		return err
	}

	tariff, stopTariff, err := newTariff(ctx, env, rdb)
	if err != nil {
		return err
	}
	defer stopTariff()

	// Structured JSON logs; the level is adjustable at run time (control plane).
	lvl, ok := telemetry.ParseLevel(env("OCS_LOG_LEVEL", "info"))
	if !ok {
		return fmt.Errorf("OCS_LOG_LEVEL: want debug, info, warn or error")
	}
	logger, levelVar := telemetry.NewLogger(os.Stderr, "ocs-signaling-gateway", lvl)
	slog.SetDefault(logger) // the standard log package now writes through it too

	reg := telemetry.NewRegistry()
	latency := reg.Histogram("ocs_engine_rate_duration_seconds", "Time to rate one charging request (queueing and processing).", nil)
	breaker := failpolicy.NewBreaker(5, time.Second,
		func() { logger.Warn("data-tier circuit breaker opened: requests are answered by the fail policy") },
		func() { logger.Info("data-tier circuit breaker closed") })

	// No profile cache exists yet, so there is no fail policy engine: if the
	// data tier is down every request is denied (fail-closed for everyone).
	eng, err := engine.New(engine.Config{}, engine.Deps{
		Store:   store,
		Tariff:  tariff,
		Breaker: breaker,
		Latency: latency,
	})
	if err != nil {
		return err
	}
	defer eng.Close()

	adm := admission.New(float64(maxTPS), maxTPS/10+1, maxInflight)
	registerMetrics(reg, eng, adm, resolver, store, breaker)
	service.HandleAdmin("/metrics", reg.Handler())

	ctl := control.New(control.Config{Service: "ocs-signaling-gateway", Audit: func(e control.Event) {
		logger.Info("control", "event", e.Type, "change", e.ChangeID, "param", e.Param, "principal", e.Principal, "detail", e.Detail)
	}})
	defer ctl.Close() // reverts any unconfirmed change
	if err := registerControls(ctl, controlTargets{Adm: adm, Breaker: breaker, Level: levelVar, BaselineTPS: maxTPS, BaselineConc: maxInflight}); err != nil {
		return err
	}
	_, stopControl, err := controlrpc.ServeFromEnv(svcenv.OS, ctl, func(f string, a ...any) { logger.Info(fmt.Sprintf(f, a...)) })
	if err != nil {
		return err
	}
	defer stopControl()
	errCh := make(chan error, 2)
	var stops []func()

	if addr := env("OCS_GY_ADDR", ":3868"); addr != "off" {
		srv, err := gy.NewServer(gy.Config{OriginHost: host, OriginRealm: realm, HostIP: ip, Identity: resolver}, eng, adm)
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		log.Printf("ocs-signaling-gateway: Gy listening on %s as %s@%s", ln.Addr(), host, realm)
		go func() { errCh <- srv.Serve(ln) }()
		stops = append(stops, func() { _ = srv.Close() })
	}

	if addr := env("OCS_NCHF_ADDR", ":8000"); addr != "off" {
		var tlsCfg *tls.Config
		if cert := os.Getenv("OCS_NCHF_TLS_CERT"); cert != "" {
			var err error
			if tlsCfg, err = nchf.TLSConfig(cert, os.Getenv("OCS_NCHF_TLS_KEY"), os.Getenv("OCS_NCHF_CLIENT_CA")); err != nil {
				return err
			}
		}
		auth, err := newAuthenticator(ctx, tlsCfg != nil)
		if err != nil {
			return err
		}
		h, err := nchf.NewHandler(nchf.Config{Region: env("OCS_REGION", "r1"), Identity: resolver,
			Auth: auth, AllowUnauthenticated: auth == nil}, eng, adm)
		if err != nil {
			return err
		}
		srv := nchf.NewServer(addr, h, tlsCfg)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		mode := "h2c (cleartext HTTP/2)"
		if tlsCfg != nil {
			mode = "HTTP/2 over TLS"
			if tlsCfg.ClientAuth != tls.NoClientCert {
				mode += " with client certificates"
			}
		}
		log.Printf("ocs-signaling-gateway: Nchf listening on %s, %s", ln.Addr(), mode)
		go func() {
			var err error
			if tlsCfg != nil {
				err = srv.ServeTLS(ln, "", "")
			} else {
				err = srv.Serve(ln)
			}
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
		stops = append(stops, func() {
			c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(c)
		})
	}
	if len(stops) == 0 {
		return errors.New("both OCS_GY_ADDR and OCS_NCHF_ADDR are off: nothing to serve")
	}
	ready()

	var serveErr error
	select {
	case serveErr = <-errCh:
	case <-ctx.Done():
	}
	for _, stop := range stops {
		stop()
	}
	return serveErr
}

func main() { service.RunWith("ocs-signaling-gateway", run) }
