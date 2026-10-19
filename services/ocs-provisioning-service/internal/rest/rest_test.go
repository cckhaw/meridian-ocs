package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

type capturingProducer struct {
	mu   sync.Mutex
	cmds []*command.Command
	err  error
}

func (p *capturingProducer) Produce(_ context.Context, _, key string, value []byte, _ map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	c, err := command.Decode(value)
	if err != nil {
		return err
	}
	if key != c.Key {
		panic("message not keyed by subscriber")
	}
	p.cmds = append(p.cmds, c)
	return nil
}

func (p *capturingProducer) last() *command.Command {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.cmds) == 0 {
		return nil
	}
	return p.cmds[len(p.cmds)-1]
}

func (p *capturingProducer) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.cmds) }

type rig struct {
	h    *Handler
	prod *capturingProducer
	st   *status.Store
	data *datalayer.Client
}

var lim = command.Limits{Currency: "EUR", MaxTopUp: 100_000, MaxAdjust: 5_000}

func newRig(t *testing.T, az *auth.Authorizer) *rig {
	t.Helper()
	rdb := redistest.Start(t)
	st := status.New(rdb, "", 0)
	prod := &capturingProducer{}
	data := datalayer.New(rdb)
	svc := service.New(service.Config{Limits: lim, Catalog: testCatalog(t)}, st, prod, data)
	return &rig{h: New(svc, az), prod: prod, st: st, data: data}
}

func asClient(r *http.Request, cn string) *http.Request {
	if cn != "" {
		cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	}
	return r
}

func (g *rig) do(method, path, body, idem, client string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if idem != "" {
		r.Header.Set("Idempotency-Key", idem)
	}
	w := httptest.NewRecorder()
	g.h.ServeHTTP(w, asClient(r, client))
	return w
}

func obj(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, w.Body.String())
	}
	return m
}

func errCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	e, _ := obj(t, w)["error"].(map[string]any)
	if e == nil {
		t.Fatalf("no error object: %s", w.Body.String())
	}
	return e["code"].(string)
}

func TestEveryWriteEndpointQueuesTheRightCommand(t *testing.T) {
	g := newRig(t, auth.New(nil, []string{"care"}))
	tests := []struct {
		name, method, path, body string
		check                    func(*command.Command) bool
	}{
		{"create", "POST", "/v1/subscribers", `{"subscriberKey":"acct-1","paymentType":"prepaid","identifiers":[{"type":"msisdn","value":"491701234567"},{"type":"imsi","value":"262011234567890"}]}`,
			func(c *command.Command) bool {
				return c.Type == command.CreateSubscriber && c.Key == "acct-1" && c.Payment == "prepaid" && len(c.Identifiers) == 2
			}},
		{"top-up defaults to the cash bucket", "POST", "/v1/subscribers/acct-1/top-ups", `{"amountMinorUnits":500,"currency":"EUR"}`,
			func(c *command.Command) bool {
				return c.Type == command.TopUp && c.Key == "acct-1" && c.Amount == 500 && c.BucketID == "cash" && c.Currency == "EUR"
			}},
		{"adjustment", "POST", "/v1/subscribers/acct-1/adjustments", `{"delta":-50,"currency":"EUR","reason":"goodwill","allowNegative":true}`,
			func(c *command.Command) bool {
				return c.Type == command.AdjustBalance && c.Amount == -50 && c.Reason == "goodwill" && c.AllowNegative && c.BucketID == "cash"
			}},
		{"bundle", "POST", "/v1/subscribers/acct-1/bundles", `{"bucketId":"data","class":"bundle","unit":"bytes","amount":1000,"expiresAt":"2030-01-01T00:00:00Z","priority":7}`,
			func(c *command.Command) bool {
				return c.Type == command.AddBundle && c.BucketID == "data" && c.Unit == "bytes" && c.ExpiresAt == time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() && *c.Priority == 7
			}},
		{"reload takes the bucket from the path", "PUT", "/v1/subscribers/acct-1/buckets/data", `{"amount":2000,"keepExpiry":true}`,
			func(c *command.Command) bool {
				return c.Type == command.ReloadBucket && c.BucketID == "data" && c.Amount == 2000 && c.KeepExpiry
			}},
		{"remove, forced", "DELETE", "/v1/subscribers/acct-1/buckets/data?force=true", ``,
			func(c *command.Command) bool {
				return c.Type == command.RemoveBucket && c.BucketID == "data" && c.Force
			}},
		{"remove, not forced", "DELETE", "/v1/subscribers/acct-1/buckets/data", ``,
			func(c *command.Command) bool { return c.Type == command.RemoveBucket && !c.Force }},
		{"offering", "POST", "/v1/subscribers/acct-1/offering", `{"offering":"gold-offer","renew":true}`,
			func(c *command.Command) bool {
				return c.Type == command.AttachOffering && c.Offering == "gold-offer" && c.Renew && c.Key == "acct-1" && len(c.Allowances) == 1 && c.Allowances[0].ID == "data" && c.CatalogVersion == 1
			}},
		{"lifecycle", "POST", "/v1/subscribers/acct-1/lifecycle", `{"target":"grace","graceUntil":"2030-01-01T00:00:00Z","reason":"low balance"}`,
			func(c *command.Command) bool {
				return c.Type == command.ChangeLifecycle && c.Target == "grace" && c.GraceUntil != 0 && c.Reason == "low balance"
			}},
	}
	for i, tc := range tests {
		id := "req-" + string(rune('a'+i))
		w := g.do(tc.method, tc.path, tc.body, id, "care")
		if w.Code != http.StatusAccepted {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
			continue
		}
		m := obj(t, w)
		if m["commandId"] != id || m["state"] != "accepted" || m["duplicate"] != false || w.Header().Get("Location") != "/v1/commands/"+id {
			t.Errorf("%s: %v %q", tc.name, m, w.Header().Get("Location"))
		}
		c := g.prod.last()
		if c == nil || c.ID != id || c.Principal != "care" || !tc.check(c) {
			t.Errorf("%s: queued %+v", tc.name, c)
		}
	}
}

func TestRepeatingARequestIsIdempotentOverHTTP(t *testing.T) {
	g := newRig(t, auth.Off())
	body := `{"amountMinorUnits":500,"currency":"EUR"}`
	w1 := g.do("POST", "/v1/subscribers/acct-1/top-ups", body, "pay-1", "")
	w2 := g.do("POST", "/v1/subscribers/acct-1/top-ups", body, "pay-1", "")
	if w1.Code != 202 || w2.Code != 202 || obj(t, w2)["duplicate"] != true || g.prod.count() != 1 {
		t.Fatalf("%d %d dup=%v queued=%d", w1.Code, w2.Code, obj(t, w2)["duplicate"], g.prod.count())
	}
	w3 := g.do("POST", "/v1/subscribers/acct-1/top-ups", `{"amountMinorUnits":900,"currency":"EUR"}`, "pay-1", "")
	if w3.Code != 409 || errCode(t, w3) != "idempotency_conflict" {
		t.Fatalf("%d %s", w3.Code, w3.Body.String())
	}
}

func TestValidationAndRequestErrors(t *testing.T) {
	g := newRig(t, auth.Off())
	tests := []struct {
		name, method, path, body, idem string
		code                           int
		err                            string
	}{
		{"no idempotency key", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "", 400, "invalid_request"},
		{"bad idempotency key", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "a b", 400, "invalid_request"},
		{"amount over the limit", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":100001,"currency":"EUR"}`, "k1", 400, "invalid_request"},
		{"wrong currency", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"USD"}`, "k2", 400, "invalid_request"},
		{"unknown field (a typo in a money field)", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnit":5,"currency":"EUR"}`, "k3", 400, "invalid_request"},
		{"misspelled optional flag next to valid fields", "POST", "/v1/subscribers/a/adjustments", `{"delta":-1,"currency":"EUR","reason":"x","allowNegatve":true}`, "k3b", 400, "invalid_request"},
		{"extra field next to valid ones", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR","note":"hello"}`, "k3c", 400, "invalid_request"},
		{"wrong field type", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":"5","currency":"EUR"}`, "k4", 400, "invalid_request"},
		{"not JSON", "POST", "/v1/subscribers/a/top-ups", `{`, "k5", 400, "invalid_request"},
		{"two JSON values", "POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}{}`, "k6", 400, "invalid_request"},
		{"empty body", "POST", "/v1/subscribers/a/top-ups", ``, "k7", 415, "unsupported_media_type"},
		{"bad timestamp", "POST", "/v1/subscribers/a/bundles", `{"bucketId":"d","class":"bundle","unit":"bytes","amount":1,"expiresAt":"tomorrow"}`, "k8", 400, "invalid_request"},
		{"bad key", "POST", "/v1/subscribers/a%7Bb%7D/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "k9", 400, "invalid_request"},
		{"create without identifiers", "POST", "/v1/subscribers", `{"subscriberKey":"a","paymentType":"prepaid"}`, "k10", 400, "invalid_request"},
		{"oversized body", "POST", "/v1/subscribers/a/top-ups", `{"currency":"` + strings.Repeat("x", MaxBody) + `"}`, "k11", 413, "body_too_large"},
		{"unknown command", "GET", "/v1/commands/never-seen", ``, "", 404, "not_found"},
		{"unknown subscriber", "GET", "/v1/subscribers/nobody", ``, "", 404, "not_found"},
	}
	for _, tc := range tests {
		w := g.do(tc.method, tc.path, tc.body, tc.idem, "")
		if tc.name == "empty body" { // no Content-Type header is sent for an empty body
			w = g.do(tc.method, tc.path, tc.body, tc.idem, "")
		}
		if w.Code != tc.code || errCode(t, w) != tc.err {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	if g.prod.count() != 0 {
		t.Fatalf("%d rejected requests were queued", g.prod.count())
	}
}

func TestWrongContentTypeAndMethod(t *testing.T) {
	g := newRig(t, auth.Off())
	r := httptest.NewRequest("POST", "/v1/subscribers/a/top-ups", strings.NewReader(`{"amountMinorUnits":5,"currency":"EUR"}`))
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("Idempotency-Key", "k")
	w := httptest.NewRecorder()
	g.h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatalf("%d", w.Code)
	}
	if w = g.do("GET", "/v1/subscribers/a/top-ups", "", "", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("%d", w.Code)
	}
	if w = g.do("POST", "/v1/nowhere", "{}", "k", ""); w.Code != http.StatusNotFound {
		t.Fatalf("%d", w.Code)
	}
}

func TestAuthorisationPerOperation(t *testing.T) {
	g := newRig(t, auth.New([]string{"crm"}, []string{"care"}))
	type op struct{ method, path, body string }
	standard := []op{
		{"POST", "/v1/subscribers", `{"subscriberKey":"a","paymentType":"prepaid","identifiers":[{"type":"msisdn","value":"491701234567"}]}`},
		{"POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`},
		{"POST", "/v1/subscribers/a/bundles", `{"bucketId":"d","class":"bundle","unit":"bytes","amount":1}`},
		{"PUT", "/v1/subscribers/a/buckets/d", `{"amount":1,"keepExpiry":true}`},
	}
	privileged := []op{
		{"POST", "/v1/subscribers/a/adjustments", `{"delta":-1,"currency":"EUR","reason":"x"}`},
		{"DELETE", "/v1/subscribers/a/buckets/d", ``},
		{"POST", "/v1/subscribers/a/lifecycle", `{"target":"terminated"}`},
	}
	n := 0
	next := func() string { n++; return "id-" + string(rune('a'+n)) }
	for _, o := range append(append([]op{}, standard...), privileged...) {
		if w := g.do(o.method, o.path, o.body, next(), ""); w.Code != 401 || errCode(t, w) != "unauthenticated" {
			t.Errorf("%s %s without a certificate: %d", o.method, o.path, w.Code)
		}
		if w := g.do(o.method, o.path, o.body, next(), "stranger"); w.Code != 403 {
			t.Errorf("%s %s by an unlisted client: %d", o.method, o.path, w.Code)
		}
	}
	for _, o := range standard {
		if w := g.do(o.method, o.path, o.body, next(), "crm"); w.Code != 202 {
			t.Errorf("%s %s by crm: %d %s", o.method, o.path, w.Code, w.Body.String())
		}
	}
	for _, o := range privileged {
		if w := g.do(o.method, o.path, o.body, next(), "crm"); w.Code != 403 || errCode(t, w) != "forbidden" {
			t.Errorf("%s %s by crm: %d", o.method, o.path, w.Code)
		}
		if w := g.do(o.method, o.path, o.body, next(), "care"); w.Code != 202 {
			t.Errorf("%s %s by care: %d %s", o.method, o.path, w.Code, w.Body.String())
		}
	}
	// Refused requests must not have left anything behind.
	if got, want := g.prod.count(), len(standard)+len(privileged); got != want {
		t.Fatalf("%d queued, want %d", got, want)
	}
	// Reads need an authenticated caller.
	if w := g.do("GET", "/v1/commands/x", "", "", ""); w.Code != 401 {
		t.Fatalf("%d", w.Code)
	}
	if w := g.do("GET", "/v1/commands/x", "", "", "crm"); w.Code != 404 {
		t.Fatalf("%d", w.Code)
	}
}

func TestServiceOutagesAreRetryable(t *testing.T) {
	g := newRig(t, auth.Off())
	g.prod.err = context.DeadlineExceeded
	w := g.do("POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "retry-me", "")
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || errCode(t, w) != "unavailable" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	g.prod.err = nil
	if w = g.do("POST", "/v1/subscribers/a/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "retry-me", ""); w.Code != 202 || obj(t, w)["duplicate"] != false {
		t.Fatalf("the same key must work again once the outage is over: %d %s", w.Code, w.Body.String())
	}
}

func TestReadEndpoints(t *testing.T) {
	g := newRig(t, auth.Off())
	ctx := context.Background()
	ok := func(op datalayer.ProvisionOp) {
		t.Helper()
		op.Now, op.Fingerprint = time.Unix(1_800_000_000, 0), op.Token
		if r, err := g.data.Provision(ctx, op); err != nil || r.Code != datalayer.ProvApplied {
			t.Fatalf("%+v %v", r, err)
		}
	}
	ok(datalayer.ProvisionOp{Op: datalayer.ProvCreate, Key: "acct-9", Token: "c", Payment: "postpaid"})
	ok(datalayer.ProvisionOp{Op: datalayer.ProvTopUp, Key: "acct-9", Token: "t", BucketID: "cash", Amount: 750, Order: 1000})
	ok(datalayer.ProvisionOp{Op: datalayer.ProvLifecycle, Key: "acct-9", Token: "l", Target: "grace", Until: time.Unix(1_800_086_400, 0)})

	m := obj(t, g.do("GET", "/v1/subscribers/acct-9", "", "", ""))
	if m["status"] != "grace" || m["paymentType"] != "postpaid" || m["version"] != float64(3) || m["graceUntil"] == nil {
		t.Fatalf("%v", m)
	}
	bs := m["buckets"].([]any)
	b := bs[0].(map[string]any)
	if len(bs) != 1 || b["id"] != "cash" || b["balance"] != float64(750) || b["class"] != "cash" {
		t.Fatalf("%v", bs)
	}

	// A command's status: accepted first, then its final outcome.
	g.do("POST", "/v1/subscribers/acct-9/top-ups", `{"amountMinorUnits":5,"currency":"EUR"}`, "pay-9", "")
	m = obj(t, g.do("GET", "/v1/commands/pay-9", "", "", ""))
	if m["state"] != "accepted" || m["type"] != "top_up" || m["subscriberKey"] != "acct-9" || m["outcome"] != nil {
		t.Fatalf("%v", m)
	}
	g.st.Complete(ctx, "pay-9", status.Result{State: status.Applied, Outcome: "applied", Balance: 755, Version: 4})
	m = obj(t, g.do("GET", "/v1/commands/pay-9", "", "", ""))
	if m["state"] != "applied" || m["outcome"] != "applied" || m["balance"] != float64(755) || m["version"] != float64(4) || m["completedAt"] == nil {
		t.Fatalf("%v", m)
	}

	g.do("POST", "/v1/subscribers/acct-9/lifecycle", `{"target":"suspended"}`, "susp-9", "")
	g.st.Complete(ctx, "susp-9", status.Result{State: status.Failed, Outcome: "invalid_transition", Message: "cannot move from grace to grace"})
	m = obj(t, g.do("GET", "/v1/commands/susp-9", "", "", ""))
	if m["state"] != "failed" || m["outcome"] != "invalid_transition" || m["message"] == nil || m["balance"] != nil {
		t.Fatalf("a failed command reports its reason and no balance: %v", m)
	}
}

func testCatalog(t testing.TB) *catalog.Store {
	t.Helper()
	d := catalog.Document{EffectiveFrom: time.Now().Add(-time.Hour),
		Plans: []catalog.PricePlan{{ID: "gold", Rules: []catalog.ChargeRule{{ID: "r", Cash: &catalog.Price{Num: 1, Den: 1}}}}},
		Offerings: []catalog.ProductOffering{{ID: "gold-offer", PlanID: "gold", Allowances: []catalog.AllowanceBucket{
			{ID: "data", Class: "bundle", Unit: "bytes", Amount: 1000}}}}}
	s := catalog.NewStore(0)
	if _, _, err := s.Publish(d, 0); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOfferingErrorsAndRead(t *testing.T) {
	g := newRig(t, auth.Off())
	if w := g.do("POST", "/v1/subscribers/acct-1/offering", `{"offering":"platinum"}`, "o-1", ""); w.Code != http.StatusBadRequest || errCode(t, w) != "invalid_request" {
		t.Fatalf("unknown offering: %d %s", w.Code, w.Body.String())
	}
	if w := g.do("POST", "/v1/subscribers/acct-1/offering", `{"offering":"gold-offer","surprise":1}`, "o-2", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d", w.Code)
	}
	if w := g.do("POST", "/v1/subscribers/acct-1/offering", `{}`, "o-3", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("no offering: %d", w.Code)
	}
	if g.prod.count() != 0 {
		t.Fatal("a refused request was queued")
	}

	// With no catalog configured the answer is a retryable 503, not a 400.
	rdb := redistest.Start(t)
	svc := service.New(service.Config{Limits: lim}, status.New(rdb, "", 0), &capturingProducer{}, datalayer.New(rdb))
	h := New(svc, auth.Off())
	r := httptest.NewRequest("POST", "/v1/subscribers/acct-1/offering", strings.NewReader(`{"offering":"gold-offer"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "o-4")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || errCode(t, w) != "catalog_unavailable" || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}

	// The subscriber read shows the offering.
	ctx := context.Background()
	for _, op := range []datalayer.ProvisionOp{
		{Op: datalayer.ProvCreate, Key: "acct-2", Token: "c", Payment: "prepaid"},
		{Op: datalayer.ProvOffering, Key: "acct-2", Token: "o", Offering: "gold-offer"},
	} {
		op.Now, op.Fingerprint = time.Now(), op.Token
		if res, err := g.data.Provision(ctx, op); err != nil || res.Code != datalayer.ProvApplied {
			t.Fatalf("%v %+v", err, res)
		}
	}
	if m := obj(t, g.do("GET", "/v1/subscribers/acct-2", "", "", "")); m["offering"] != "gold-offer" {
		t.Fatalf("%v", m)
	}
}
