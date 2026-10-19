// Command ocs-signaling-gateway terminates network-facing charging protocols
// (today: 4G Gy/Ro over Diameter/TCP) and hands canonical requests to the
// rating engine.
//
// This binary runs the rating engine in-process (the "co-located" mode of the
// design): no network hop between adapter and engine. A remote mode over gRPC
// will be added with the generated rating.proto stubs.
//
// Configuration (environment):
//
//	OCS_GY_ADDR          Diameter listen address              (default :3868)
//	OCS_ORIGIN_HOST      Diameter identity, required          (e.g. ocs1.example.net)
//	OCS_ORIGIN_REALM     Diameter realm, required
//	OCS_HOST_IP          address advertised in CEA, required
//	OCS_REDIS_ADDR       hot-state store                      (default 127.0.0.1:6379)
//	OCS_MAX_TPS          admission rate limit, 0 = unlimited  (default 0)
//	OCS_MAX_INFLIGHT     concurrent request limit, 0 = off    (default 10000)
//	OCS_CASH_BYTES_RATE  cash price of data as num/den        (default 1/1024)
//	OCS_ADMIN_ADDR       /healthz and /readyz                 (default :8080)
//
// The cash rate is a placeholder until the catalog service exists.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/pkg/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/gy"
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

	srv, err := gy.NewServer(gy.Config{OriginHost: host, OriginRealm: realm, HostIP: ip},
		eng, gy.NewAdmission(float64(maxTPS), maxTPS/10+1, maxInflight))
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", env("OCS_GY_ADDR", ":3868"))
	if err != nil {
		return err
	}
	log.Printf("ocs-signaling-gateway: Gy listening on %s as %s@%s", ln.Addr(), host, realm)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	ready()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		return srv.Close()
	}
}

func main() { service.RunWith("ocs-signaling-gateway", run) }
