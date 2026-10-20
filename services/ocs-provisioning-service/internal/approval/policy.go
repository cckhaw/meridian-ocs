// Package approval decides which provisioning commands need a second person.
//
// A privileged caller can move or destroy value: correct a balance, remove a
// bucket, suspend or terminate a subscriber. With approval on, such a command
// is not queued when it is submitted. It waits as a request until a different
// authorised person approves exactly what was requested; one person alone can
// never make it happen.
//
// The policy is one setting, OCS_PROV_APPROVAL: "off", or a comma separated
// list of rules:
//
//	adjust_balance            every balance correction
//	adjust_balance>5000       corrections larger than 5000 (absolute, minor units or units)
//	change_lifecycle          every status change
//	change_lifecycle:terminated|suspended   only these target states
//	remove_bucket             every bucket removal
//
// Only the privileged command types can be named: the routine flows (create,
// top-up, bundles, offering changes) are never held back.
package approval

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cckhaw/meridian-ocs/pkg/lifecycle"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

type rule struct {
	text    string
	typ     command.Type
	above   int64               // adjust_balance: absolute amount above which approval is needed (0 = always)
	targets map[string]struct{} // change_lifecycle: target states (nil = all)
}

// Policy is a parsed OCS_PROV_APPROVAL. The zero value approves nothing.
type Policy struct {
	rules []rule
	text  string
}

// ParsePolicy parses the setting. It is deliberately strict: an unknown type
// or state, a malformed threshold, an empty rule or a duplicate is an error,
// so a typo cannot silently leave a command unprotected. The empty string is
// an error too: the choice has to be made ("off" is a choice).
func ParsePolicy(s string) (Policy, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "":
		return Policy{}, fmt.Errorf(`OCS_PROV_APPROVAL must be set: "off", or rules such as adjust_balance>5000,change_lifecycle:terminated,remove_bucket`)
	case "off":
		return Policy{text: "off"}, nil
	}
	var p Policy
	seen := map[command.Type]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return Policy{}, fmt.Errorf("OCS_PROV_APPROVAL: empty rule in %q", s)
		}
		r, err := parseRule(part)
		if err != nil {
			return Policy{}, fmt.Errorf("OCS_PROV_APPROVAL: %w", err)
		}
		if seen[r.typ] {
			return Policy{}, fmt.Errorf("OCS_PROV_APPROVAL: %s is named twice", r.typ)
		}
		seen[r.typ] = true
		p.rules = append(p.rules, r)
	}
	p.text = s
	return p, nil
}

func parseRule(part string) (rule, error) {
	r := rule{text: part}
	name := part
	switch {
	case strings.Contains(part, ">"):
		i := strings.Index(part, ">")
		name = part[:i]
		n, err := strconv.ParseInt(part[i+1:], 10, 64)
		if err != nil || n < 1 {
			return r, fmt.Errorf("rule %q: the threshold must be a whole number of at least 1", part)
		}
		r.above = n
	case strings.Contains(part, ":"):
		i := strings.Index(part, ":")
		name = part[:i]
		r.targets = map[string]struct{}{}
		for _, t := range strings.Split(part[i+1:], "|") {
			if _, ok := lifecycle.Parse(t); !ok {
				return r, fmt.Errorf("rule %q: %q is not a lifecycle state (active, grace, suspended, terminated)", part, t)
			}
			r.targets[t] = struct{}{}
		}
	}
	r.typ = command.Type(name)
	if !r.typ.Privileged() {
		return r, fmt.Errorf("rule %q: only privileged commands can need approval (adjust_balance, remove_bucket, change_lifecycle), not %q", part, name)
	}
	if r.above > 0 && r.typ != command.AdjustBalance {
		return r, fmt.Errorf("rule %q: a threshold applies to adjust_balance only", part)
	}
	if r.targets != nil && r.typ != command.ChangeLifecycle {
		return r, fmt.Errorf("rule %q: target states apply to change_lifecycle only", part)
	}
	return r, nil
}

// Enabled reports whether any command can need approval.
func (p Policy) Enabled() bool { return len(p.rules) > 0 }

// String is the policy as configured ("off" when disabled).
func (p Policy) String() string {
	if p.text == "" {
		return "off"
	}
	return p.text
}

// Required reports whether c needs approval, and the rule that says so.
func (p Policy) Required(c *command.Command) (bool, string) {
	for _, r := range p.rules {
		if r.typ != c.Type {
			continue
		}
		switch r.typ {
		case command.AdjustBalance:
			amt := c.Amount
			if amt < 0 {
				amt = -amt
			}
			if r.above == 0 || amt > r.above {
				return true, r.text
			}
		case command.ChangeLifecycle:
			if r.targets == nil {
				return true, r.text
			}
			if _, ok := r.targets[c.Target]; ok {
				return true, r.text
			}
		default:
			return true, r.text
		}
	}
	return false, ""
}

// Summary describes a command in words, for an approver who has to decide on
// it: what will happen, to whom, and why.
func Summary(c *command.Command) string {
	switch c.Type {
	case command.AdjustBalance:
		dir := "add"
		amt := c.Amount
		if amt < 0 {
			dir, amt = "remove", -amt
		}
		return fmt.Sprintf("Correct balance of %s: %s %d in bucket %s. Reason: %s", c.Key, dir, amt, c.BucketID, c.Reason)
	case command.RemoveBucket:
		force := ""
		if c.Force {
			force = " (forced, even if it still holds a balance)"
		}
		return fmt.Sprintf("Remove bucket %s of %s%s", c.BucketID, c.Key, force)
	case command.ChangeLifecycle:
		reason := ""
		if c.Reason != "" {
			reason = ". Reason: " + c.Reason
		}
		return fmt.Sprintf("Change status of %s to %s%s", c.Key, c.Target, reason)
	}
	return fmt.Sprintf("%s for %s", c.Type, c.Key)
}
