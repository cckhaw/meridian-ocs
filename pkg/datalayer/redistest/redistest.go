// Package redistest starts throwaway redis-server processes for integration tests.
package redistest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Start launches redis-server on a free port and returns a client. The test is
// skipped when redis-server is not installed, unless OCS_REQUIRE_REDIS=1, in
// which case it fails.
func Start(tb testing.TB) *redis.Client {
	tb.Helper()
	rdb, _ := start(tb)
	return rdb
}

// Replica controls a replica process.
type Replica struct{ cmd *exec.Cmd }

// Kill terminates the replica (a replica failure).
func (r *Replica) Kill() { _ = r.cmd.Process.Kill(); _, _ = r.cmd.Process.Wait() }

// Pause freezes the replica (SIGSTOP): it stays connected but acknowledges
// nothing, like a replica that has fallen behind. Resume undoes it.
func (r *Replica) Pause()  { _ = r.cmd.Process.Signal(syscall.SIGSTOP) }
func (r *Replica) Resume() { _ = r.cmd.Process.Signal(syscall.SIGCONT) }

// StartReplica launches a replica of primary and waits until its link is up and
// the primary counts it.
func StartReplica(tb testing.TB, primary *redis.Client) (*redis.Client, *Replica) {
	tb.Helper()
	host, port, err := net.SplitHostPort(primary.Options().Addr)
	if err != nil {
		tb.Fatal(err)
	}
	// By default a primary waits 5s before syncing a new replica.
	if err := primary.ConfigSet(context.Background(), "repl-diskless-sync-delay", "0").Err(); err != nil {
		tb.Fatal(err)
	}
	rdb, cmd := start(tb, "--replicaof", host, port)
	waitFor(tb, "replica link up", func() bool {
		info, err := rdb.Info(context.Background(), "replication").Result()
		return err == nil && strings.Contains(info, "master_link_status:up")
	})
	waitFor(tb, "primary sees the replica", func() bool {
		info, err := primary.Info(context.Background(), "replication").Result()
		return err == nil && strings.Contains(info, "connected_slaves:1")
	})
	return rdb, &Replica{cmd: cmd}
}

func waitFor(tb testing.TB, what string, ok func() bool) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func start(tb testing.TB, extra ...string) (*redis.Client, *exec.Cmd) {
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

	args := append([]string{"--port", fmt.Sprint(port), "--bind", "127.0.0.1",
		"--save", "", "--appendonly", "no", "--dir", tb.TempDir()}, extra...)
	cmd := exec.Command(bin, args...)
	if err := cmd.Start(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
	tb.Cleanup(func() { _ = rdb.Close() })
	waitFor(tb, "redis-server to start", func() bool { return rdb.Ping(context.Background()).Err() == nil })
	return rdb, cmd
}
