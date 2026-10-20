package svcenv

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

func envMap(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func ping(addr string, g Getenv) (string, error) {
	opts := &redis.Options{Addr: addr, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, MaxRetries: -1}
	desc, err := ApplyRedisAccess(opts, g)
	if err != nil {
		return desc, err
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return desc, rdb.Ping(ctx).Err()
}

func TestRedisAccessRefusesConfigurationsThatLookProtectedButAreNot(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	os.WriteFile(empty, []byte("\n"), 0o600)
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"password and file", map[string]string{"OCS_REDIS_PASSWORD": "a", "OCS_REDIS_PASSWORD_FILE": empty}, "choose one"},
		{"username alone", map[string]string{"OCS_REDIS_USERNAME": "app"}, "without OCS_REDIS_PASSWORD"},
		{"empty password file", map[string]string{"OCS_REDIS_PASSWORD_FILE": empty}, "is empty"},
		{"missing password file", map[string]string{"OCS_REDIS_PASSWORD_FILE": filepath.Join(dir, "nope")}, "OCS_REDIS_PASSWORD_FILE"},
		{"bad tls value", map[string]string{"OCS_REDIS_TLS": "true"}, `"on" or "off"`},
		{"ca with tls off", map[string]string{"OCS_REDIS_TLS_CA": "/x"}, "no effect with OCS_REDIS_TLS off"},
		{"server name with tls off", map[string]string{"OCS_REDIS_TLS_SERVER_NAME": "redis"}, "no effect"},
		{"client cert with tls off", map[string]string{"OCS_REDIS_TLS": "off", "OCS_REDIS_TLS_CERT": "/c", "OCS_REDIS_TLS_KEY": "/k"}, "no effect"},
		{"cert without key", map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CERT": "/c"}, "set together"},
		{"unreadable ca", map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": filepath.Join(dir, "nope")}, "OCS_REDIS_TLS_CA"},
		{"ca that is not pem", map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": empty}, "no PEM certificate"},
		{"unloadable client cert", map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CERT": empty, "OCS_REDIS_TLS_KEY": empty}, "OCS_REDIS_TLS_CERT/KEY"},
	}
	for _, c := range cases {
		_, err := ApplyRedisAccess(&redis.Options{Addr: "r:6379"}, envMap(c.env))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error mentioning %q", c.name, err, c.want)
		}
	}
}

func TestRedisAccessDescribesWithoutRevealing(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	os.WriteFile(pw, []byte("s3cret-from-file\n"), 0o600)
	for _, c := range []struct {
		env  map[string]string
		want string
	}{
		{nil, "not authenticated, not encrypted"},
		{map[string]string{"OCS_REDIS_PASSWORD": "s3cret-inline"}, "password, not encrypted"},
		{map[string]string{"OCS_REDIS_USERNAME": "app", "OCS_REDIS_PASSWORD_FILE": pw}, "user app (password from a file), not encrypted"},
	} {
		opts := &redis.Options{Addr: "r:6379"}
		d, err := ApplyRedisAccess(opts, envMap(c.env))
		if err != nil || !strings.Contains(d, c.want) {
			t.Errorf("%q err=%v, want %q", d, err, c.want)
		}
		if strings.Contains(d, "s3cret") {
			t.Errorf("the description leaks the password: %q", d)
		}
	}
	// A trailing newline in the file is not part of the password.
	opts := &redis.Options{Addr: "r:6379"}
	if _, err := ApplyRedisAccess(opts, envMap(map[string]string{"OCS_REDIS_PASSWORD_FILE": pw})); err != nil {
		t.Fatal(err)
	}
	if _, p := opts.CredentialsProvider(); p != "s3cret-from-file" {
		t.Fatalf("password %q", p)
	}
}

func TestAPasswordIsEnforcedAndANamedUserIsUsed(t *testing.T) {
	addr, _ := redistest.StartSecure(t, redistest.Secure{Password: "default-pw"})
	if _, err := ping(addr, envMap(nil)); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("no password: %v", err)
	}
	if _, err := ping(addr, envMap(map[string]string{"OCS_REDIS_PASSWORD": "wrong"})); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("wrong password: %v", err)
	}
	if _, err := ping(addr, envMap(map[string]string{"OCS_REDIS_PASSWORD": "default-pw"})); err != nil {
		t.Fatalf("right password: %v", err)
	}

	// With the default user off, only the named user gets in.
	addr2, _ := redistest.StartSecure(t, redistest.Secure{DisableDefaultUser: true, User: "ocs", UserPassword: "ocs-pw"})
	if _, err := ping(addr2, envMap(map[string]string{"OCS_REDIS_PASSWORD": "ocs-pw"})); err == nil {
		t.Fatal("the password of a named user must not work for the default user")
	}
	if _, err := ping(addr2, envMap(map[string]string{"OCS_REDIS_USERNAME": "ocs", "OCS_REDIS_PASSWORD": "ocs-pw"})); err != nil {
		t.Fatalf("named user: %v", err)
	}
}

// The password file is read for every new connection, so a rotated password
// works after the old connections are dropped, with no restart.
func TestARotatedPasswordInAFileIsPickedUp(t *testing.T) {
	addr, _ := redistest.StartSecure(t, redistest.Secure{DisableDefaultUser: true, User: "ocs", UserPassword: "first"})
	pwFile := filepath.Join(t.TempDir(), "password")
	os.WriteFile(pwFile, []byte("first\n"), 0o600)
	g := envMap(map[string]string{"OCS_REDIS_USERNAME": "ocs", "OCS_REDIS_PASSWORD_FILE": pwFile})
	opts := &redis.Options{Addr: addr, DialTimeout: 2 * time.Second, MaxRetries: -1, PoolSize: 1}
	if _, err := ApplyRedisAccess(opts, g); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	// Rotate on the server and in the file, then drop the existing connection.
	admin := redis.NewClient(&redis.Options{Addr: addr, Username: "ocs", Password: "first"})
	defer admin.Close()
	if err := admin.Do(ctx, "ACL", "SETUSER", "ocs", "resetpass", ">second").Err(); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(pwFile, []byte("second\n"), 0o600)
	_ = admin.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err()
	var err error
	for i := 0; i < 20; i++ { // the pool discards the dead connection and dials again
		if err = rdb.Ping(ctx).Err(); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("after the rotation: %v", err)
	}
}

func TestTLSIsVerifiedAndAClientCertificateCanBeRequired(t *testing.T) {
	pki := tlstest.New(t)
	other := tlstest.New(t) // a different CA
	srv := &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}
	addr, _ := redistest.StartSecure(t, redistest.Secure{TLS: srv})

	on := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile}
	if d, err := ping(addr, envMap(on)); err != nil || !strings.Contains(d, "TLS") {
		t.Fatalf("TLS with the right CA: %q %v", d, err)
	}
	if _, err := ping(addr, envMap(nil)); err == nil {
		t.Fatal("a plaintext client must not talk to a TLS-only server")
	}
	if _, err := ping(addr, envMap(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": other.CAFile})); err == nil ||
		!strings.Contains(err.Error(), "certificate") {
		t.Fatalf("a server certificate from another CA must be refused: %v", err)
	}
	if _, err := ping(addr, envMap(map[string]string{"OCS_REDIS_TLS": "on"})); err == nil {
		t.Fatal("the system roots do not know the test CA: the server must be refused")
	}
	if _, err := ping(addr, envMap(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_TLS_SERVER_NAME": "redis.example.net"})); err == nil ||
		!strings.Contains(err.Error(), "certificate") {
		t.Fatalf("a server name the certificate does not carry must be refused: %v", err)
	}

	// A server that demands client certificates.
	addr2, _ := redistest.StartSecure(t, redistest.Secure{TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile, RequireClientCert: true}})
	if _, err := ping(addr2, envMap(on)); err == nil {
		t.Fatal("no client certificate: the server must refuse")
	}
	certFile, keyFile := pki.ClientFiles(t, "ocs-client")
	mtls := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_TLS_CERT": certFile, "OCS_REDIS_TLS_KEY": keyFile}
	if d, err := ping(addr2, envMap(mtls)); err != nil || !strings.Contains(d, "client certificate") {
		t.Fatalf("with a client certificate: %q %v", d, err)
	}
	// A client certificate from another CA is refused.
	oc, ok2 := other.ClientFiles(t, "intruder")
	if _, err := ping(addr2, envMap(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_TLS_CERT": oc, "OCS_REDIS_TLS_KEY": ok2})); err == nil {
		t.Fatal("a client certificate from another CA must be refused")
	}
}

func TestTLSAndAPasswordTogether(t *testing.T) {
	pki := tlstest.New(t)
	addr, _ := redistest.StartSecure(t, redistest.Secure{Password: "pw", TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	g := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_PASSWORD": "pw"}
	if d, err := ping(addr, envMap(g)); err != nil || d != "redis access: password, TLS" {
		t.Fatalf("%q %v", d, err)
	}
	delete(g, "OCS_REDIS_PASSWORD")
	if _, err := ping(addr, envMap(g)); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("encrypted but not authenticated: %v", err)
	}
}

func TestATLSDialGetsMoreTimeThanAPlainConnect(t *testing.T) {
	opts := &redis.Options{Addr: "r:6379", DialTimeout: 200 * time.Millisecond}
	pki := tlstest.New(t)
	if _, err := ApplyRedisAccess(opts, envMap(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile})); err != nil {
		t.Fatal(err)
	}
	if opts.DialTimeout < time.Second {
		t.Fatalf("a TLS handshake needs more than %v", opts.DialTimeout)
	}
	plain := &redis.Options{Addr: "r:6379", DialTimeout: 200 * time.Millisecond}
	if _, err := ApplyRedisAccess(plain, envMap(nil)); err != nil || plain.DialTimeout != 200*time.Millisecond {
		t.Fatal("a plain connection keeps its budget")
	}
}

func TestCheckRedisAccessFailsOnCredentialsAndTLSButNotOnAnOutage(t *testing.T) {
	pki := tlstest.New(t)
	addr, kill := redistest.StartSecure(t, redistest.Secure{Password: "pw",
		TLS: &redistest.ServerTLS{CertFile: pki.CertFile, KeyFile: pki.KeyFile, CAFile: pki.CAFile}})
	check := func(env map[string]string) error {
		opts := &redis.Options{Addr: addr, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, MaxRetries: -1}
		if _, err := ApplyRedisAccess(opts, envMap(env)); err != nil {
			t.Fatal(err)
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		return CheckRedisAccess(context.Background(), rdb)
	}
	tlsOn := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile}
	good := map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_PASSWORD": "pw"}
	if err := check(good); err != nil {
		t.Fatalf("good credentials: %v", err)
	}
	if err := check(tlsOn); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("no password: %v", err)
	}
	if err := check(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_TLS_CA": pki.CAFile, "OCS_REDIS_PASSWORD": "wrong"}); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("wrong password: %v", err)
	}
	if err := check(map[string]string{"OCS_REDIS_TLS": "on", "OCS_REDIS_PASSWORD": "pw"}); err == nil {
		t.Fatal("a server certificate the process does not trust must be an error")
	}
	if err := check(map[string]string{"OCS_REDIS_PASSWORD": "pw"}); err == nil {
		t.Fatal("plaintext to a TLS server must be an error")
	}
	// An outage is not a configuration error.
	kill()
	if err := check(good); err != nil {
		t.Fatalf("an unreachable Redis must not stop the service from starting: %v", err)
	}
}
