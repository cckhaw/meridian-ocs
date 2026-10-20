package worker

import (
	"context"
	"testing"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/approval"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// inject puts a command on the topic the way a writer that skipped the API's approval step would:
// recorded as accepted and queued, with whatever approval fields it chooses to carry.
func (h *harness) inject(c command.Command) {
	h.t.Helper()
	ctx := context.Background()
	c.AcceptedAt = 1_800_000_000_000
	if _, _, err := h.st.Begin(ctx, &c); err != nil {
		h.t.Fatal(err)
	}
	raw, err := c.Encode()
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.prod.Produce(ctx, h.cfg.CommandsTopic, c.Key, raw, nil); err != nil {
		h.t.Fatal(err)
	}
}

// The worker enforces the approval policy a second time: a privileged command that needs a second
// person is applied only if it carries an approval by someone other than its requester.
func TestTheWorkerRefusesACommandThatNeedsApprovalAndHasNone(t *testing.T) {
	h := newHarness(t, 1)
	policy, err := approval.ParsePolicy("adjust_balance,change_lifecycle:terminated")
	if err != nil {
		t.Fatal(err)
	}
	h.start(h.applier(), func(c *Config) { c.Policy = policy })
	h.submit(create("w-c", "acct-w", msisdn))
	h.waitFinal("w-c")
	h.submit(topup("w-t", "acct-w", 1000))
	h.waitFinal("w-t")

	withBy := func(c command.Command, requester, approver string) command.Command {
		c.Principal, c.ApprovedBy = requester, approver
		return c
	}
	h.inject(withBy(adjust("w-none", "acct-w", -300), "alice", ""))
	if rec := h.waitFinal("w-none"); rec.State != status.Failed || rec.Outcome != "approval_required" {
		t.Fatalf("no approval: %+v", rec)
	}
	h.inject(withBy(adjust("w-self", "acct-w", -300), "alice", "alice"))
	if rec := h.waitFinal("w-self"); rec.State != status.Failed || rec.Outcome != "self_approval" {
		t.Fatalf("self-approval: %+v", rec)
	}
	h.inject(withBy(command.Command{ID: "w-term", Type: command.ChangeLifecycle, Key: "acct-w", Target: "terminated"}, "alice", ""))
	if rec := h.waitFinal("w-term"); rec.Outcome != "approval_required" {
		t.Fatalf("termination without approval: %+v", rec)
	}
	if got := cash(t, h, "acct-w"); got != 1000 {
		t.Fatalf("a refused command changed the balance: %d", got)
	}

	// A command that is not held back by the policy is untouched, with or without an approval.
	h.inject(withBy(adjust("w-small-ok", "acct-w", 0), "alice", ""))
	h.inject(withBy(command.Command{ID: "w-act", Type: command.ChangeLifecycle, Key: "acct-w", Target: "active"}, "alice", ""))
	if rec := h.waitFinal("w-act"); rec.Outcome == "approval_required" {
		t.Fatalf("a rule that does not apply refused the command: %+v", rec)
	}

	// With a proper approval it is applied, and the event names both people.
	h.inject(withBy(adjust("w-ok", "acct-w", -300), "alice", "bob"))
	if rec := h.waitFinal("w-ok"); rec.State != status.Applied {
		t.Fatalf("approved: %+v", rec)
	}
	if got := cash(t, h, "acct-w"); got != 700 {
		t.Fatalf("balance %d", got)
	}
	var found bool
	for _, e := range h.events() {
		if e.CommandID == "w-ok" && e.Type == EventBalanceModified {
			found = true
			if e.By != "alice" || e.ApprovedBy != "bob" {
				t.Fatalf("the event must name both people: %+v", e)
			}
		}
	}
	if !found {
		t.Fatal("no balance.modified event for the approved command")
	}
}

func TestTheWorkerHonoursSelfApprovalOnlyWhenTolerated(t *testing.T) {
	h := newHarness(t, 1)
	policy, _ := approval.ParsePolicy("adjust_balance")
	h.start(h.applier(), func(c *Config) { c.Policy, c.AllowSelfApproval = policy, true })
	h.submit(create("s-c", "acct-s", msisdn))
	h.waitFinal("s-c")
	h.submit(topup("s-t", "acct-s", 500))
	h.waitFinal("s-t")
	c := adjust("s-self", "acct-s", -100)
	c.Principal, c.ApprovedBy = "anonymous", "anonymous"
	h.inject(c)
	if rec := h.waitFinal("s-self"); rec.State != status.Applied {
		t.Fatalf("%+v", rec)
	}
}
