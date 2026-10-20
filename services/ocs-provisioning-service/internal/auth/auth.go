// Package auth decides who may submit which provisioning commands.
//
// Callers are identified by the common name of their verified client
// certificate (mutual TLS). There are two levels:
//
//   - standard callers may create subscribers, top up, and add or reload
//     bundles: the routine CRM flows;
//   - privileged callers may in addition adjust balances, remove buckets and
//     change lifecycle state (including termination): corrections and
//     destructive operations.
//
// Reading commands and subscribers needs any authenticated caller.
package auth

import (
	"errors"
	"sort"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

var (
	// ErrUnauthenticated: no verified client identity.
	ErrUnauthenticated = errors.New("auth: no verified client certificate")
	// ErrForbidden: authenticated but not allowed to do this.
	ErrForbidden = errors.New("auth: not permitted")
)

// Anonymous is the principal when authentication is switched off.
const Anonymous = "anonymous"

// Authorizer holds the allow-lists.
type Authorizer struct {
	off        bool
	standard   map[string]struct{}
	privileged map[string]struct{}
}

// Off returns an Authorizer that lets everyone do everything. For development
// only.
func Off() *Authorizer { return &Authorizer{off: true} }

// New returns an Authorizer for mutual TLS. If standard is empty, every
// verified client is a standard caller (the CA is then the only gate);
// privileged callers always have to be named.
func New(standard, privileged []string) *Authorizer {
	a := &Authorizer{standard: map[string]struct{}{}, privileged: map[string]struct{}{}}
	for _, s := range standard {
		a.standard[s] = struct{}{}
	}
	for _, s := range privileged {
		a.privileged[s] = struct{}{}
	}
	return a
}

// Principal normalises the caller identity: with authentication off everyone
// is Anonymous.
func (a *Authorizer) Principal(commonName string) string {
	if a.off {
		return Anonymous
	}
	return commonName
}

// Read checks that the caller may read subscriber data (profiles, balances,
// identifiers, command outcomes). When an allow-list of standard clients is
// configured, a verified client that is not on it, and not privileged, may not
// read either: subscriber data is personal data and "has a certificate from our
// CA" is not a reason to see it.
func (a *Authorizer) Read(principal string) error {
	if a.off {
		return nil
	}
	if principal == "" {
		return ErrUnauthenticated
	}
	if len(a.standard) == 0 {
		return nil
	}
	if _, ok := a.standard[principal]; ok {
		return nil
	}
	if _, ok := a.privileged[principal]; ok {
		return nil
	}
	return ErrForbidden
}

// Allow checks that the caller may submit a command of type t.
func (a *Authorizer) Allow(principal string, t command.Type) error {
	if a.off {
		return nil
	}
	if principal == "" {
		return ErrUnauthenticated
	}
	if _, ok := a.privileged[principal]; ok {
		return nil
	}
	if t.Privileged() {
		return ErrForbidden
	}
	if len(a.standard) == 0 {
		return nil
	}
	if _, ok := a.standard[principal]; ok {
		return nil
	}
	return ErrForbidden
}

// Describe summarises the configuration for logs.
func (a *Authorizer) Describe() string {
	if a.off {
		return "authentication OFF"
	}
	names := func(m map[string]struct{}) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	std := "any verified client"
	if len(a.standard) > 0 {
		std = "clients " + join(names(a.standard))
	}
	return "standard: " + std + "; privileged: " + join(names(a.privileged))
}

func join(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	out := s[0]
	for _, x := range s[1:] {
		out += "," + x
	}
	return out
}
