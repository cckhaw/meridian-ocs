// Package redistest starts a throwaway redis-server for integration tests.
package redistest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Start launches redis-server on a free port and returns a client. The test is
// skipped when redis-server is not installed, unless OCS_REQUIRE_REDIS=1, in
// which case it fails.
func Start(tb testing.TB) *redis.Client {
	tb.Helper()
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("OCS_REQUIRE_REDIS") == "1" {
			tb.Fatal("redis-server not found and OCS_REQUIRE_REDIS=1")
		}
		tb.Skip("redis-server not installed")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--bind", "127.0.0.1",
		"--save", "", "--appendonly", "no", "--dir", tb.TempDir())
	if err := cmd.Start(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
	tb.Cleanup(func() { _ = rdb.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			return rdb
		}
		if time.Now().After(deadline) {
			tb.Fatal("redis-server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
