// Package identity maps the identifiers a network element presents (MSISDN,
// IMSI/SUPI, NAI) to the one stable subscriber key under which balances and
// sessions are stored.
//
// Neither MSISDN nor IMSI can be that key: a SIM swap keeps the MSISDN and
// changes the IMSI, number porting does the opposite, and 5G subscribers need
// not have an MSISDN at all. The key is an opaque id chosen by provisioning;
// this package resolves identities to it, for every protocol adapter, so a
// subscriber charged over Gy and over Nchf is charged against the same state.
package identity

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
)

// Type is the kind of identifier.
type Type uint8

const (
	MSISDN Type = iota + 1
	IMSI
	NAI
)

func (t Type) String() string {
	switch t {
	case MSISDN:
		return "msisdn"
	case IMSI:
		return "imsi"
	case NAI:
		return "nai"
	}
	return "unknown"
}

// Identity is one identifier. MSISDN values are E.164 digits without '+'.
type Identity struct {
	Type  Type
	Value string
}

var (
	// ErrUnknown: no subscriber is bound to the identity.
	ErrUnknown = errors.New("identity: unknown subscriber")
	// ErrConflict: the presented identities resolve to different subscribers
	// (or, when binding, the identity already belongs to another subscriber).
	ErrConflict = errors.New("identity: conflicting identities")
	// ErrInvalid: an identifier is malformed.
	ErrInvalid = errors.New("identity: invalid identifier")
	// ErrNoIdentity: nothing to resolve.
	ErrNoIdentity = errors.New("identity: no identifier presented")
)

// Directory looks one identity up. Lookup returns ErrUnknown when the identity
// is not bound; any other error means the directory could not answer.
type Directory interface {
	Lookup(ctx context.Context, id Identity) (key string, err error)
}

// ConflictPolicy decides what happens when identities resolve to different keys.
type ConflictPolicy uint8

const (
	// ConflictReject refuses the request. A wrong-account charge on a
	// prepaid balance is worse than a refused one, so this is the default.
	ConflictReject ConflictPolicy = iota
	// ConflictPreferFirst keeps the key of the first identity that resolved
	// (callers pass identities in precedence order) and counts the conflict.
	ConflictPreferFirst
)

// Stats are cumulative counters.
type Stats struct {
	Resolutions uint64
	Unknown     uint64
	Conflicts   uint64
	Errors      uint64 // directory failures
}

// Resolver turns the identities of one request into a subscriber key.
type Resolver struct {
	dir      Directory
	policy   ConflictPolicy
	validate bool

	resolutions, unknown, conflicts, errs atomic.Uint64
}

// NewResolver builds a Resolver on a directory. Identifiers are validated and
// normalised before lookup.
func NewResolver(dir Directory, policy ConflictPolicy) *Resolver {
	return &Resolver{dir: dir, policy: policy, validate: true}
}

type passthrough struct{}

func (passthrough) Lookup(_ context.Context, id Identity) (string, error) { return id.Value, nil }

// NewPassthroughResolver uses the highest-precedence identifier itself as the
// subscriber key, with no validation and no directory. It exists for
// single-interface deployments and development: it does NOT unify a
// subscriber's balances across identifier types.
func NewPassthroughResolver() *Resolver {
	return &Resolver{dir: passthrough{}, policy: ConflictPreferFirst}
}

// Stats returns a snapshot of the counters.
func (r *Resolver) Stats() Stats {
	return Stats{r.resolutions.Load(), r.unknown.Load(), r.conflicts.Load(), r.errs.Load()}
}

// Resolve returns the subscriber key for ids, given in precedence order. All
// identities are looked up so inconsistencies are noticed. If one resolves and
// the others are unknown, the one that resolved wins. A directory failure
// fails the whole resolution: guessing could charge the wrong account.
func (r *Resolver) Resolve(ctx context.Context, ids []Identity) (string, error) {
	if len(ids) == 0 {
		return "", ErrNoIdentity
	}
	r.resolutions.Add(1)
	var key string
	conflict := false
	for _, id := range ids {
		if r.validate {
			v, ok := Normalize(id.Type, id.Value)
			if !ok {
				return "", ErrInvalid
			}
			id.Value = v
		}
		k, err := r.dir.Lookup(ctx, id)
		switch {
		case err == nil:
			if key == "" {
				key = k
			} else if k != key {
				conflict = true
			}
		case errors.Is(err, ErrUnknown):
		default:
			r.errs.Add(1)
			return "", err
		}
	}
	if conflict {
		r.conflicts.Add(1)
		if r.policy == ConflictReject {
			return "", ErrConflict
		}
	}
	if key == "" {
		r.unknown.Add(1)
		return "", ErrUnknown
	}
	return key, nil
}

func digits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// Normalize validates an identifier and returns its canonical form.
//
// MSISDN: E.164 digits, a leading '+' is dropped, 6-15 digits. National-format
// numbers are NOT converted; provisioning must store E.164.
// IMSI: 6-15 digits.
// NAI: 3-128 printable ASCII characters without spaces.
func Normalize(t Type, v string) (string, bool) {
	switch t {
	case MSISDN:
		v = strings.TrimPrefix(v, "+")
		return v, digits(v) && len(v) >= 6 && len(v) <= 15
	case IMSI:
		return v, digits(v) && len(v) >= 6 && len(v) <= 15
	case NAI:
		if len(v) < 3 || len(v) > 128 {
			return v, false
		}
		for i := 0; i < len(v); i++ {
			if v[i] <= ' ' || v[i] > '~' {
				return v, false
			}
		}
		return v, true
	}
	return v, false
}

// ValidKey reports whether s can be used as a subscriber key: it is embedded
// in a Redis hash tag, so it must not contain braces, and it is carried in URLs.
func ValidKey(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] > '~' || s[i] == '{' || s[i] == '}' {
			return false
		}
	}
	return true
}
