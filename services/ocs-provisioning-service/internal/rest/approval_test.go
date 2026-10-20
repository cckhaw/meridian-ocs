package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/approval"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
)

func approvalRig(t *testing.T) *rig {
	t.Helper()
	p, err := approval.ParsePolicy("adjust_balance>1000,change_lifecycle:terminated,remove_bucket")
	if err != nil {
		t.Fatal(err)
	}
	// crm: standard; care and risk: privileged; audit: may approve but not request.
	az := auth.New([]string{"crm"}, []string{"care", "risk"}).WithApprovers([]string{"audit"})
	return newRigWith(t, az, service.Config{Policy: p, ApprovalTTL: time.Hour})
}

const bigAdjust = `{"bucketId":"cash","delta":-2500,"currency":"EUR","reason":"goodwill"}`

func TestAPrivilegedRequestThatNeedsApprovalIsAcceptedButNotQueued(t *testing.T) {
	g := approvalRig(t)
	w := g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")
	m := obj(t, w)
	ap, _ := m["approval"].(map[string]any)
	if w.Code != 202 || m["state"] != "pending_approval" || ap["required"] != true || ap["rule"] != "adjust_balance>1000" || ap["expiresAt"] == "" {
		t.Fatalf("%d %v", w.Code, m)
	}
	if g.prod.count() != 0 {
		t.Fatal("a request waiting for approval was queued")
	}
	// Under the threshold goes straight through.
	if w := g.do("POST", "/v1/subscribers/a/adjustments", `{"bucketId":"cash","delta":-1000,"currency":"EUR","reason":"x"}`, "adj-small", "care"); w.Code != 202 || obj(t, w)["state"] != "accepted" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if g.prod.count() != 1 {
		t.Fatalf("%d queued", g.prod.count())
	}
	// Its status shows who asked and the rule.
	m = obj(t, g.do("GET", "/v1/commands/adj-1", "", "", "care"))
	ap, _ = m["approval"].(map[string]any)
	if m["state"] != "pending_approval" || ap["requestedBy"] != "care" || ap["rule"] != "adjust_balance>1000" || ap["expiresAt"] == nil || m["outcome"] != nil {
		t.Fatalf("%v", m)
	}
}

func TestWhoMayApproveAndWhoMayNot(t *testing.T) {
	g := approvalRig(t)
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")

	// A caller who is not privileged and not an approver cannot even look at the queue.
	for _, who := range []string{"crm", "stranger"} {
		if w := g.do("GET", "/v1/approvals", "", "", who); w.Code != 403 {
			t.Errorf("%s lists approvals: %d", who, w.Code)
		}
		if w := g.do("POST", "/v1/commands/adj-1/approve", "", "", who); w.Code != 403 {
			t.Errorf("%s approves: %d", who, w.Code)
		}
		if w := g.do("POST", "/v1/commands/adj-1/reject", `{"reason":"x"}`, "", who); w.Code != 403 {
			t.Errorf("%s rejects: %d", who, w.Code)
		}
	}
	if w := g.do("GET", "/v1/approvals", "", "", ""); w.Code != 401 {
		t.Errorf("no certificate: %d", w.Code)
	}
	// The requester cannot approve their own request.
	w := g.do("POST", "/v1/commands/adj-1/approve", "", "", "care")
	if w.Code != 403 || errCode(t, w) != "self_approval" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if g.prod.count() != 0 {
		t.Fatal("self-approval queued the command")
	}
	// An approver who may not request privileged things can approve.
	if w := g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-2", "audit"); w.Code != 403 {
		t.Fatalf("an approver-only caller must not be able to request: %d", w.Code)
	}
	w = g.do("POST", "/v1/commands/adj-1/approve", "", "", "audit")
	if w.Code != 200 || obj(t, w)["state"] != "accepted" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if g.prod.count() != 1 {
		t.Fatalf("%d queued", g.prod.count())
	}
	c := g.prod.last()
	if c.ID != "adj-1" || c.Principal != "care" || c.ApprovedBy != "audit" || c.Amount != -2500 {
		t.Fatalf("%+v", c)
	}
	// A second privileged caller can approve too (risk), but only once.
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-3", "care")
	if w := g.do("POST", "/v1/commands/adj-3/approve", "", "", "risk"); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	w = g.do("POST", "/v1/commands/adj-3/approve", "", "", "audit")
	if w.Code != 409 || errCode(t, w) != "not_pending" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	m := obj(t, g.do("GET", "/v1/commands/adj-3", "", "", "care"))
	if ap, _ := m["approval"].(map[string]any); ap["approvedBy"] != "risk" || ap["requestedBy"] != "care" {
		t.Fatalf("%v", m)
	}
}

func TestTheQueueShowsWhatEachRequestWouldDo(t *testing.T) {
	g := approvalRig(t)
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")
	g.do("POST", "/v1/subscribers/b/lifecycle", `{"target":"terminated","reason":"fraud"}`, "term-1", "risk")
	g.do("DELETE", "/v1/subscribers/c/buckets/promo", "", "rm-1", "care")
	w := g.do("GET", "/v1/approvals", "", "", "audit")
	m := obj(t, w)
	list, _ := m["approvals"].([]any)
	if w.Code != 200 || len(list) != 3 {
		t.Fatalf("%d %v", w.Code, m)
	}
	// Requests made within the same millisecond have no defined order among themselves: look them up by id.
	byID := map[string]map[string]any{}
	for _, it := range list {
		byID[it.(map[string]any)["commandId"].(string)] = it.(map[string]any)
	}
	first := byID["adj-1"]
	if first["type"] != "adjust_balance" || first["subscriberKey"] != "a" || first["requestedBy"] != "care" ||
		first["amount"] != float64(-2500) || first["bucketId"] != "cash" || first["reason"] != "goodwill" || first["summary"] == "" || first["expiresAt"] == "" {
		t.Fatalf("%v", first)
	}
	if second := byID["term-1"]; second["target"] != "terminated" || second["requestedBy"] != "risk" || second["reason"] != "fraud" {
		t.Fatalf("%v", second)
	}
	if third := byID["rm-1"]; third["type"] != "remove_bucket" || third["bucketId"] != "promo" {
		t.Fatalf("%v", third)
	}
}

func TestRejectingARequest(t *testing.T) {
	g := approvalRig(t)
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")
	w := g.do("POST", "/v1/commands/adj-1/reject", `{"reason":"no evidence"}`, "", "audit")
	if w.Code != 200 || obj(t, w)["state"] != "rejected" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	m := obj(t, g.do("GET", "/v1/commands/adj-1", "", "", "care"))
	if ap, _ := m["approval"].(map[string]any); m["state"] != "rejected" || ap["rejectedBy"] != "audit" || ap["reason"] != "no evidence" {
		t.Fatalf("%v", m)
	}
	if w := g.do("POST", "/v1/commands/adj-1/approve", "", "", "risk"); w.Code != 409 || errCode(t, w) != "not_pending" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := g.do("POST", "/v1/commands/nope/reject", `{"reason":"x"}`, "", "audit"); w.Code != 404 {
		t.Fatalf("%d", w.Code)
	}
	// The requester may withdraw their own request.
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-2", "care")
	if w := g.do("POST", "/v1/commands/adj-2/reject", `{"reason":"mistake"}`, "", "care"); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if g.prod.count() != 0 {
		t.Fatal("a rejected request was queued")
	}
	// Strict body handling like every other endpoint.
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-3", "care")
	if w := g.do("POST", "/v1/commands/adj-3/reject", `{"reason":"x","extra":1}`, "", "audit"); w.Code != 400 {
		t.Fatalf("unknown field: %d", w.Code)
	}
}

func TestAnExpiredRequestAnswers409(t *testing.T) {
	p, _ := approval.ParsePolicy("adjust_balance")
	az := auth.New(nil, []string{"care", "risk"})
	now := time.Now()
	g := newRigWith(t, az, service.Config{Policy: p, ApprovalTTL: time.Hour, Now: func() time.Time { return now }})
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")
	// The status store compares with the real clock, so use a request that was accepted long ago.
	now = time.Now().Add(-2 * time.Hour)
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-old", "care")
	w := g.do("POST", "/v1/commands/adj-old/approve", "", "", "risk")
	if w.Code != 409 || errCode(t, w) != "approval_expired" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := g.do("GET", "/v1/commands/adj-old", "", "", "care"); obj(t, w)["state"] != "expired" {
		t.Fatalf("%s", w.Body)
	}
}

func TestMetaTellsTheConsoleAboutApprovals(t *testing.T) {
	g := approvalRig(t)
	for who, want := range map[string]bool{"care": true, "risk": true, "audit": true, "crm": false} {
		m := obj(t, g.do("GET", "/v1/meta", "", "", who))
		can, _ := m["can"].(map[string]any)
		ap, _ := m["approvals"].(map[string]any)
		if can["approve"] != want || ap["enabled"] != true || ap["policy"] != "adjust_balance>1000,change_lifecycle:terminated,remove_bucket" ||
			ap["ttlSeconds"] != float64(3600) || ap["allowSelf"] != false {
			t.Errorf("%s: %v", who, m)
		}
	}
	off := newRig(t, auth.New(nil, []string{"care"}))
	m := obj(t, off.do("GET", "/v1/meta", "", "", "care"))
	if ap, _ := m["approvals"].(map[string]any); ap["enabled"] != false || ap["policy"] != "off" {
		t.Fatalf("%v", m)
	}
}

func TestApprovalsAcrossOriginsAreRefused(t *testing.T) {
	g := approvalRig(t)
	g.do("POST", "/v1/subscribers/a/adjustments", bigAdjust, "adj-1", "care")
	r := asClient(httptestPost("/v1/commands/adj-1/approve"), "risk")
	r.Header.Set("Origin", "https://evil.example")
	w := newRecorder()
	g.h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("a cross-origin approval: %d", w.Code)
	}
	if g.prod.count() != 0 {
		t.Fatal("it queued the command")
	}
}

func httptestPost(path string) *http.Request  { return httptest.NewRequest("POST", path, nil) }
func newRecorder() *httptest.ResponseRecorder { return httptest.NewRecorder() }
