package approval

import (
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
)

func TestParsingIsStrictSoATypoCannotLeaveACommandUnprotected(t *testing.T) {
	for in, want := range map[string]string{
		"":                              "must be set",
		"  ":                            "must be set",
		"adjust_balance,":               "empty rule",
		"adjust_balance,adjust_balance": "named twice",
		"top_up":                        "only privileged commands",
		"adjust_balanc":                 "only privileged commands",
		"adjust_balance>0":              "at least 1",
		"adjust_balance>many":           "whole number",
		"remove_bucket>5":               "threshold applies to adjust_balance only",
		"change_lifecycle>5":            "threshold applies to adjust_balance only",
		"change_lifecycle:terminate":    "not a lifecycle state",
		"adjust_balance:terminated":     "target states apply to change_lifecycle only",
		"change_lifecycle:terminated|":  "not a lifecycle state",
		"OFF":                           "only privileged commands",
	} {
		if _, err := ParsePolicy(in); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want an error mentioning %q", in, err, want)
		}
	}
	if p, err := ParsePolicy("off"); err != nil || p.Enabled() || p.String() != "off" {
		t.Fatalf("off: %v %v", p, err)
	}
}

func TestWhichCommandsNeedApproval(t *testing.T) {
	p, err := ParsePolicy("adjust_balance>1000, change_lifecycle:terminated|suspended,remove_bucket")
	if err != nil || !p.Enabled() {
		t.Fatal(err)
	}
	type c = command.Command
	for _, tc := range []struct {
		name string
		cmd  c
		want bool
	}{
		{"small correction", c{Type: command.AdjustBalance, Amount: -1000}, false},
		{"large debit", c{Type: command.AdjustBalance, Amount: -1001}, true},
		{"large credit", c{Type: command.AdjustBalance, Amount: 5000}, true},
		{"terminate", c{Type: command.ChangeLifecycle, Target: "terminated"}, true},
		{"suspend", c{Type: command.ChangeLifecycle, Target: "suspended"}, true},
		{"reinstate", c{Type: command.ChangeLifecycle, Target: "active"}, false},
		{"grace", c{Type: command.ChangeLifecycle, Target: "grace"}, false},
		{"remove bucket", c{Type: command.RemoveBucket, BucketID: "x"}, true},
		{"top-up is never held", c{Type: command.TopUp, Amount: 99999999}, false},
		{"create is never held", c{Type: command.CreateSubscriber}, false},
		{"offering is never held", c{Type: command.AttachOffering}, false},
	} {
		got, rule := p.Required(&tc.cmd)
		if got != tc.want || (got && rule == "") {
			t.Errorf("%s: %v %q, want %v", tc.name, got, rule, tc.want)
		}
	}
	off, _ := ParsePolicy("off")
	if got, _ := off.Required(&c{Type: command.ChangeLifecycle, Target: "terminated"}); got {
		t.Error("with approval off nothing is held")
	}
	always, _ := ParsePolicy("adjust_balance")
	if got, _ := always.Required(&c{Type: command.AdjustBalance, Amount: 1}); !got {
		t.Error("without a threshold every correction is held")
	}
}

func TestSummaryTellsAnApproverWhatWillHappen(t *testing.T) {
	for _, tc := range []struct {
		c    command.Command
		want []string
	}{
		{command.Command{Type: command.AdjustBalance, Key: "s1", Amount: -250, BucketID: "cash", Reason: "goodwill"}, []string{"remove 250", "cash", "s1", "goodwill"}},
		{command.Command{Type: command.AdjustBalance, Key: "s1", Amount: 40, BucketID: "cash", Reason: "refund"}, []string{"add 40"}},
		{command.Command{Type: command.ChangeLifecycle, Key: "s1", Target: "terminated", Reason: "fraud"}, []string{"terminated", "fraud"}},
		{command.Command{Type: command.RemoveBucket, Key: "s1", BucketID: "data", Force: true}, []string{"Remove bucket data", "forced"}},
	} {
		got := Summary(&tc.c)
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%q lacks %q", got, w)
			}
		}
	}
}
