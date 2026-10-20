package svcenv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ApplyRedisAccess reads how this process authenticates to, and encrypts its
// connection with, Redis, and applies it to opts. It returns a one-line
// description for the start-up log (never the password).
//
//	OCS_REDIS_USERNAME     ACL user (Redis 6+); empty = the default user
//	OCS_REDIS_PASSWORD     the password
//	OCS_REDIS_PASSWORD_FILE  a file holding the password (preferred: mount a Secret). It is
//	                       read again for every new connection, so a rotated password is picked
//	                       up without a restart. Not both of these.
//	OCS_REDIS_TLS          "on" or "off" (default off)
//	OCS_REDIS_TLS_CA       PEM file with the CA that signed the server certificate
//	                       (default: the system roots)
//	OCS_REDIS_TLS_CERT, OCS_REDIS_TLS_KEY   client certificate for a server that requires one;
//	                       read again for every handshake, so a renewed certificate needs no restart
//	OCS_REDIS_TLS_SERVER_NAME   name to verify in the server certificate (default: the host of OCS_REDIS_ADDR)
//
// Verification is never turned off. Settings that would have no effect (TLS
// options with TLS off, a username without a password) are errors, so a
// configuration that looks protected but is not is refused.
func ApplyRedisAccess(opts *redis.Options, g Getenv) (string, error) {
	user, pass, passFile := g("OCS_REDIS_USERNAME"), g("OCS_REDIS_PASSWORD"), g("OCS_REDIS_PASSWORD_FILE")
	if pass != "" && passFile != "" {
		return "", errors.New("OCS_REDIS_PASSWORD and OCS_REDIS_PASSWORD_FILE are both set: choose one")
	}
	if user != "" && pass == "" && passFile == "" {
		return "", errors.New("OCS_REDIS_USERNAME is set without OCS_REDIS_PASSWORD or OCS_REDIS_PASSWORD_FILE")
	}
	var parts []string
	switch {
	case passFile != "":
		first, err := readSecretFile(passFile)
		if err != nil {
			return "", fmt.Errorf("OCS_REDIS_PASSWORD_FILE: %w", err)
		}
		if first == "" {
			return "", fmt.Errorf("OCS_REDIS_PASSWORD_FILE: %s is empty", passFile)
		}
		// Re-read for every new connection: a rotated password needs no restart.
		opts.CredentialsProvider = func() (string, string) {
			if p, err := readSecretFile(passFile); err == nil && p != "" {
				return user, p
			}
			return user, first // keep working with the last good value if the file is briefly unreadable
		}
		parts = append(parts, authDesc(user)+" (password from a file)")
	case pass != "":
		opts.Username, opts.Password = user, pass
		parts = append(parts, authDesc(user))
	default:
		parts = append(parts, "not authenticated")
	}

	mode := g("OCS_REDIS_TLS")
	switch mode {
	case "", "off":
		for _, k := range []string{"OCS_REDIS_TLS_CA", "OCS_REDIS_TLS_CERT", "OCS_REDIS_TLS_KEY", "OCS_REDIS_TLS_SERVER_NAME"} {
			if g(k) != "" {
				return "", fmt.Errorf("%s has no effect with OCS_REDIS_TLS off: refusing a configuration that looks encrypted but is not", k)
			}
		}
		parts = append(parts, "not encrypted")
	case "on":
		cfg, err := redisTLS(opts.Addr, g)
		if err != nil {
			return "", err
		}
		opts.TLSConfig = cfg
		// A TLS handshake takes longer than the connect budget some services give a
		// plain connection; connections are pooled, so this is off the hot path.
		if opts.DialTimeout > 0 && opts.DialTimeout < time.Second {
			opts.DialTimeout = time.Second
		}
		if g("OCS_REDIS_TLS_CERT") != "" {
			parts = append(parts, "TLS with a client certificate")
		} else {
			parts = append(parts, "TLS")
		}
	default:
		return "", fmt.Errorf(`OCS_REDIS_TLS must be "on" or "off" (got %q)`, mode)
	}
	return "redis access: " + strings.Join(parts, ", "), nil
}

func authDesc(user string) string {
	if user == "" {
		return "password"
	}
	return "user " + user
}

// readSecretFile returns a file's content without surrounding whitespace (a
// Secret written with a trailing newline must not become part of the password).
func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func redisTLS(addr string, g Getenv) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if name := g("OCS_REDIS_TLS_SERVER_NAME"); name != "" {
		cfg.ServerName = name
	}
	if caFile := g("OCS_REDIS_TLS_CA"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("OCS_REDIS_TLS_CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("OCS_REDIS_TLS_CA: %s holds no PEM certificate", caFile)
		}
		cfg.RootCAs = pool
	}
	cert, key := g("OCS_REDIS_TLS_CERT"), g("OCS_REDIS_TLS_KEY")
	if (cert == "") != (key == "") {
		return nil, errors.New("OCS_REDIS_TLS_CERT and OCS_REDIS_TLS_KEY must be set together")
	}
	if cert != "" {
		if _, err := tls.LoadX509KeyPair(cert, key); err != nil { // fail at start, not at the first handshake
			return nil, fmt.Errorf("OCS_REDIS_TLS_CERT/KEY: %w", err)
		}
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c, err := tls.LoadX509KeyPair(cert, key) // renewed certificates are picked up per handshake
			if err != nil {
				return nil, err
			}
			return &c, nil
		}
	}
	return cfg, nil
}

// LogRedisAccess logs the description from ApplyRedisAccess, as a warning when
// the connection is not authenticated or not encrypted (fine on a laptop,
// not on a network).
func LogRedisAccess(logf func(string, ...any), desc string) {
	if strings.Contains(desc, "not authenticated") || strings.Contains(desc, "not encrypted") {
		logf("WARNING %s: acceptable for development only; set OCS_REDIS_PASSWORD_FILE and OCS_REDIS_TLS=on otherwise", desc)
		return
	}
	logf("%s", desc)
}

// CheckRedisAccess fails, with the reason, when Redis refuses this process's
// credentials or its TLS setup: those are configuration errors, and finding out
// at start-up beats finding out at the first charge. An unreachable Redis is NOT
// an error here: the gateway must still start during an outage to serve its fail
// policy, and readiness and the circuit breaker report the outage.
func CheckRedisAccess(ctx context.Context, rdb redis.UniversalClient) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = rdb.Ping(pctx).Err()
		cancel()
		if err == nil {
			return nil
		}
		msg := err.Error()
		for _, bad := range []string{"NOAUTH", "WRONGPASS", "invalid username-password", "NOPERM", "ERR AUTH", "x509:", "tls:", "remote error"} {
			if strings.Contains(msg, bad) {
				return fmt.Errorf("redis refuses this process's credentials or TLS setup (check OCS_REDIS_USERNAME/PASSWORD/TLS*): %w", err)
			}
		}
		// A server that accepts the connection and then closes it before answering is what a
		// TLS-only Redis does to a plaintext client (a Redis that is down refuses the
		// connection instead). Once is a blip; three times in a row is a mismatch.
		if !strings.Contains(msg, "EOF") && !strings.Contains(msg, "connection reset") {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(150 * time.Millisecond):
		}
	}
	return fmt.Errorf("redis closes the connection before answering, repeatedly (is TLS required? check OCS_REDIS_TLS): %w", err)
}
