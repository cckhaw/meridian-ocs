// Package redistest starts throwaway redis-server processes for integration tests.
package redistest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
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

// Secure describes the access control of a server started by StartSecure.
type Secure struct {
	// Password is the default user's password (requirepass). Ignored when
	// DisableDefaultUser is set.
	Password string
	// User, if set, is created with full access and UserPassword.
	User, UserPassword string
	// DisableDefaultUser turns the default user off, so only User can log in.
	DisableDefaultUser bool
	// TLS, if set, makes the server accept TLS connections only (no plaintext port).
	TLS *ServerTLS
}

// ServerTLS are the server's certificate files. ClientCAFile, when set, makes
// the server demand a client certificate signed by that CA.
type ServerTLS struct {
	CertFile, KeyFile, CAFile string
	RequireClientCert         bool
}

// StartSecure launches a redis-server with the given access control and returns
// its address. The test is skipped (or fails, with OCS_REQUIRE_REDIS=1) when
// redis-server is missing or was built without TLS support and TLS is asked for.
func StartSecure(tb testing.TB, s Secure) (addr string, kill func()) {
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

	args := []string{"--bind", "127.0.0.1", "--save", "", "--appendonly", "no", "--dir", tb.TempDir()}
	if s.TLS != nil {
		clientAuth := "no"
		if s.TLS.RequireClientCert {
			clientAuth = "yes"
		}
		args = append(args, "--port", "0", "--tls-port", fmt.Sprint(port),
			"--tls-cert-file", s.TLS.CertFile, "--tls-key-file", s.TLS.KeyFile, "--tls-ca-cert-file", s.TLS.CAFile,
			"--tls-auth-clients", clientAuth)
	} else {
		args = append(args, "--port", fmt.Sprint(port))
	}
	if s.DisableDefaultUser {
		args = append(args, "--user", "default", "off")
	} else if s.Password != "" {
		args = append(args, "--requirepass", s.Password)
	}
	if s.User != "" {
		args = append(args, "--user", s.User, "on", ">"+s.UserPassword, "~*", "&*", "+@all")
	}
	cmd := exec.Command(bin, args...)
	out := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		tb.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	kill = func() {
		_ = cmd.Process.Kill()
		<-exited
	}
	tb.Cleanup(kill)

	addr = fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			c.Close()
			return addr, kill
		}
		select {
		case <-exited:
		default:
			if time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
		}
		if s.TLS != nil && strings.Contains(strings.ToLower(out.String()), "tls") {
			if os.Getenv("OCS_REQUIRE_REDIS") == "1" {
				tb.Fatalf("redis-server does not accept the TLS options: %s", out.String())
			}
			tb.Skipf("redis-server built without TLS support? %s", out.String())
		}
		tb.Fatalf("redis-server did not start: %s", out.String())
	}
}

// syncBuffer collects a process's output; it is written by the exec goroutines
// while the test reads it.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
