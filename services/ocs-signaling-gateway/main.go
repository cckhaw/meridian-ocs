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
//	OCS_REGION           region embedded in charging references (default r1)
//	OCS_IDENTITY_MODE    "directory" (default) or "direct" (see below)
//	OCS_IDENTITY_PREFIX  Redis key prefix of the identity directory (default id:)
//	OCS_IDENTITY_CACHE_TTL  how long a binding is cached     (default 30s)
//	OCS_IDENTITY_ON_CONFLICT  reject | prefer-first          (default reject)
//	OCS_ORIGIN_HOST      Diameter identity, required          (e.g. ocs1.example.net)
//	OCS_ORIGIN_REALM     Diameter realm, required
//	OCS_HOST_IP          address advertised in CEA, required
//	OCS_REDIS_ADDR       hot-state store                      (default 127.0.0.1:6379)
//	OCS_MAX_TPS          admission rate limit, 0 = unlimited  (default 0; shared by Gy and Nchf)
//	OCS_MAX_INFLIGHT     concurrent request limit, 0 = off    (default 10000)
//	OCS_CASH_BYTES_RATE  cash price of data as num/den        (default 1/1024)
//	OCS_ADMIN_ADDR       /healthz and /readyz                 (default :8080)
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
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/gy"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/nchf"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: want a non-negative integer, got %q", key, v)
	}
	return n, nil
}

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
	cash, err := parseRate(env("OCS_CASH_BYTES_RATE", "1/1024"))
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
	resolver, err := newResolver(rdb)
	if err != nil {
		return err
	}

	// No profile cache exists yet, so there is no fail policy engine: if the
	// data tier is down every request is denied (fail-closed for everyone).
	eng, err := engine.New(engine.Config{}, engine.Deps{
		Store:  store,
		Tariff: engine.StaticTariff{Cash: map[canonical.UnitKind]engine.Frac{canonical.UnitBytes: cash}},
	})
	if err != nil {
		return err
	}
	defer eng.Close()

	adm := admission.New(float64(maxTPS), maxTPS/10+1, maxInflight)
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
		h, err := nchf.NewHandler(nchf.Config{Region: env("OCS_REGION", "r1"), Identity: resolver}, eng, adm)
		if err != nil {
			return err
		}
		var tlsCfg *tls.Config
		if cert := os.Getenv("OCS_NCHF_TLS_CERT"); cert != "" {
			if tlsCfg, err = nchf.TLSConfig(cert, os.Getenv("OCS_NCHF_TLS_KEY"), os.Getenv("OCS_NCHF_CLIENT_CA")); err != nil {
				return err
			}
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
