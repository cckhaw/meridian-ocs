package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
)

type system struct {
	app  *App
	base string
	pki  *tlstest.PKI
	rdb  string
}

func (s *system) client(t *testing.T, cn string) *http.Client {
	t.Helper()
	cfg := s.pki.ClientTLS(t, cn)
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
}

func startSystem(t *testing.T) *system {
	t.Helper()
	pki := tlstest.New(t)
	rdb := redistest.Start(t)
	e := map[string]string{
		"OCS_REDIS_ADDR": rdb.Options().Addr, "OCS_CATALOG_ADDR": "127.0.0.1:0", "OCS_CATALOG_AUTH": "mtls",
		"OCS_CATALOG_TLS_CERT": pki.CertFile, "OCS_CATALOG_TLS_KEY": pki.KeyFile, "OCS_CATALOG_CLIENT_CA": pki.CAFile,
		"OCS_CATALOG_PUBLISHERS": "pricing", "OCS_CATALOG_RESYNC": "1h",
	}
	cfg, err := LoadConfig(func(k string) string { return e[k] })
	if err != nil {
		t.Fatal(err)
	}
	app, err := NewApp(context.Background(), cfg, func(string) string { return "" }, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("Run did not return after cancel")
		}
		app.Close()
	})
	return &system{app: app, base: "https://" + app.Addr(), pki: pki, rdb: rdb.Options().Addr}
}

func call(t *testing.T, c *http.Client, method, url string, body any, ctype string) (int, map[string]any, http.Header) {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, url, rd)
	if body != nil {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	if out == nil {
		out = map[string]any{"_raw": string(raw)}
	}
	return resp.StatusCode, out, resp.Header
}

func draft(from time.Time, dataNum int64) catalog.Document {
	return catalog.Document{
		EffectiveFrom: from, Note: "test",
		Plans: []catalog.PricePlan{{
			ID: "std", TimeZone: "UTC",
			Filters: []catalog.PolicyFilter{{ID: "no-roam", Action: "deny", When: `rat == "ROAM"`}},
			Rules: []catalog.ChargeRule{
				{ID: "data", When: `unit == "bytes"`, Cash: &catalog.Price{Num: dataNum, Den: 1}},
				{ID: "rest", Cash: &catalog.Price{Num: 1, Den: 1}},
			},
		}},
		Offerings: []catalog.ProductOffering{{ID: "gold", PlanID: "std",
			Allowances: []catalog.AllowanceBucket{{ID: "g10", Class: "bundle", Unit: "bytes", Amount: 10 << 30, ValidityDays: 30}}}},
	}
}

func TestCatalogServiceEndToEnd(t *testing.T) {
	s := startSystem(t)
	editor := s.client(t, "pricing") // may publish
	viewer := s.client(t, "care")    // may read, validate, dry-run
	past := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	// No client certificate: refused by TLS itself.
	anon := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: s.pki.ClientTLS(t, "x").RootCAs}}}
	if resp, err := anon.Get(s.base + "/v1/catalog"); err == nil {
		resp.Body.Close()
		t.Fatal("a client without a certificate was served")
	}

	// Empty catalog.
	if code, _, _ := call(t, viewer, "GET", s.base+"/v1/catalog", nil, ""); code != 404 {
		t.Fatalf("empty catalog: %d", code)
	}
	if code, body, _ := call(t, viewer, "GET", s.base+"/v1/catalog/versions", nil, ""); code != 200 || body["_raw"] == nil {
		t.Fatalf("%d %v", code, body)
	}

	// Validation finds every problem, valid drafts pass.
	bad := draft(past, 2)
	bad.Plans[0].Rules[0].When = `bogus ==`
	bad.Offerings[0].PlanID = "ghost"
	code, v, _ := call(t, viewer, "POST", s.base+"/v1/catalog/validate", bad, "application/json")
	if code != 200 || v["valid"] != false || len(v["errors"].([]any)) != 2 {
		t.Fatalf("%d %v", code, v)
	}
	if code, v, _ = call(t, viewer, "POST", s.base+"/v1/catalog/validate", draft(past, 2), "application/json"); code != 200 || v["valid"] != true {
		t.Fatalf("%d %v", code, v)
	}

	// Strict decoding and media type.
	if code, _, _ := call(t, editor, "POST", s.base+"/v1/catalog/validate", `{"effectiveFrom":"2030-01-01T00:00:00Z","plans":[],"surprise":1}`, "application/json"); code != 400 {
		t.Fatalf("unknown field: %d", code)
	}
	if code, _, _ := call(t, editor, "POST", s.base+"/v1/catalog/validate", `{}`, "text/plain"); code != 415 {
		t.Fatalf("media type: %d", code)
	}

	// Only a publisher may publish; the draft must be valid.
	pub := map[string]any{"document": draft(past, 2), "expectedVersion": 1}
	if code, _, _ := call(t, viewer, "POST", s.base+"/v1/catalog/versions", pub, "application/json"); code != 403 {
		t.Fatalf("non-publisher: %d", code)
	}
	invalid := map[string]any{"document": bad}
	if code, body, _ := call(t, editor, "POST", s.base+"/v1/catalog/versions", invalid, "application/json"); code != 422 {
		t.Fatalf("invalid publish: %d %v", code, body)
	}
	code, body, hdr := call(t, editor, "POST", s.base+"/v1/catalog/versions", pub, "application/json")
	if code != 201 || body["version"].(float64) != 1 || hdr.Get("Location") != "/v1/catalog?version=1" {
		t.Fatalf("publish: %d %v %v", code, body, hdr)
	}
	// A stale editor is told so.
	if code, body, _ = call(t, editor, "POST", s.base+"/v1/catalog/versions", pub, "application/json"); code != 409 {
		t.Fatalf("stale: %d %v", code, body)
	}
	// Dates must increase.
	if code, _, _ = call(t, editor, "POST", s.base+"/v1/catalog/versions", map[string]any{"document": draft(past, 3)}, "application/json"); code != 422 {
		t.Fatalf("not monotonic: %d", code)
	}

	// Reads.
	if code, body, _ = call(t, viewer, "GET", s.base+"/v1/catalog", nil, ""); code != 200 || body["note"] != "test" {
		t.Fatalf("%d %v", code, body)
	}
	if code, _, _ = call(t, viewer, "GET", s.base+"/v1/catalog?version=1", nil, ""); code != 200 {
		t.Fatalf("%d", code)
	}
	if code, _, _ = call(t, viewer, "GET", s.base+"/v1/catalog?version=9", nil, ""); code != 404 {
		t.Fatalf("%d", code)
	}
	if code, _, _ = call(t, viewer, "GET", s.base+"/v1/catalog?at=yesterday", nil, ""); code != 400 {
		t.Fatalf("%d", code)
	}
	if code, _, _ = call(t, viewer, "GET", s.base+"/v1/catalog?at="+past.Add(-time.Hour).Format(time.RFC3339), nil, ""); code != 404 {
		t.Fatalf("before the first version: %d", code)
	}
	if code, body, _ = call(t, viewer, "GET", s.base+"/v1/catalog/offerings/gold", nil, ""); code != 200 || body["planId"] != "std" {
		t.Fatalf("%d %v", code, body)
	}
	if code, _, _ = call(t, viewer, "GET", s.base+"/v1/catalog/offerings/none", nil, ""); code != 404 {
		t.Fatalf("%d", code)
	}
	if code, body, _ = call(t, viewer, "GET", s.base+"/v1/catalog/schema", nil, ""); code != 200 || body["variables"] == nil || body["limits"] == nil {
		t.Fatalf("%d %v", code, body)
	}

	// Dry run: price 2 -> 5 on data, roaming barred in both, nothing published.
	next := draft(time.Now().Add(48*time.Hour).UTC(), 5)
	dr := map[string]any{"document": next, "cases": []catalog.Case{
		{Name: "data", PlanID: "std", RAT: "LTE"},
		{Name: "roaming", PlanID: "std", RAT: "ROAM"},
		{Name: "voice", PlanID: "std", Unit: "seconds"},
	}}
	code, body, _ = call(t, viewer, "POST", s.base+"/v1/catalog/dry-run", dr, "application/json")
	if code != 200 {
		t.Fatalf("%d %v", code, body)
	}
	res := body["results"].([]any)
	if len(res) != 3 || res[0].(map[string]any)["changed"] != true || res[1].(map[string]any)["changed"] != false || res[2].(map[string]any)["changed"] != false {
		t.Fatalf("%v", res)
	}
	dr["document"] = bad
	if code, _, _ = call(t, viewer, "POST", s.base+"/v1/catalog/dry-run", dr, "application/json"); code != 422 {
		t.Fatalf("dry run of an invalid draft: %d", code)
	}

	// A future-dated publish becomes a second version but does not take effect yet.
	if code, body, _ = call(t, editor, "POST", s.base+"/v1/catalog/versions", map[string]any{"document": next, "expectedVersion": 2}, "application/json"); code != 201 {
		t.Fatalf("%d %v", code, body)
	}
	_, vs, _ := call(t, viewer, "GET", s.base+"/v1/catalog/versions", nil, "")
	_ = vs

	// A rating node that connects later sees the same timeline and prices from it.
	node := &catalog.Replicator{Redis: redis.NewClient(&redis.Options{Addr: s.rdb}), Store: catalog.NewStore(0)}
	if err := node.Sync(context.Background()); err != nil || node.Store.LatestVersion() != 2 {
		t.Fatalf("%v v%d", err, node.Store.LatestVersion())
	}
	tf := &catalog.Tariff{Store: node.Store, DefaultPlan: "std"}
	req := &canonical.Request{SubscriberID: "x", RATType: "LTE"}
	u := &canonical.UnitRequest{Kind: canonical.UnitBytes}
	now := time.Now()
	rates, err := tf.Rates(req, u, nil)
	if err != nil || rates[len(rates)-1].Num != 2 {
		t.Fatalf("now: %v %v", rates, err)
	}
	rates, _, err = tf.RatesAt(now.Add(72*time.Hour), "std", req, u, nil)
	if err != nil || rates[len(rates)-1].Num != 5 {
		t.Fatalf("in three days: %v %v", rates, err)
	}
}

// The editor is served by the same authenticated listener, with a policy that
// forbids inline script, and a browser cannot be made to publish from another
// site.
func TestEditorIsServedSafelyAndCannotBeDrivenCrossOrigin(t *testing.T) {
	s := startSystem(t)
	editor := s.client(t, "pricing")
	viewer := s.client(t, "care")

	get := func(c *http.Client, path string) (*http.Response, string) {
		resp, err := c.Get(s.base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}
	resp, body := get(viewer, "/ui/")
	if resp.StatusCode != 200 || !strings.Contains(body, "Meridian OCS") {
		t.Fatalf("%d", resp.StatusCode)
	}
	csp := resp.Header.Get("Content-Security-Policy")
	if !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "script-src 'self'") || strings.Contains(csp, "unsafe-inline") ||
		resp.Header.Get("X-Frame-Options") != "DENY" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("headers: %v", resp.Header)
	}
	if strings.Contains(body, "<script>") || strings.Contains(body, "onclick=") {
		t.Fatal("the page must not carry inline script")
	}
	if resp, js := get(viewer, "/ui/app.js"); resp.StatusCode != 200 || strings.Contains(js, "innerHTML") {
		t.Fatalf("app.js %d (must never use innerHTML)", resp.StatusCode)
	}
	noFollow := *viewer
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if resp, _ := get(&noFollow, "/"); resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/ui/" {
		t.Fatalf("root: %d %v", resp.StatusCode, resp.Header)
	}

	// Who am I: the publisher can publish, the viewer cannot.
	if code, b, _ := call(t, editor, "GET", s.base+"/v1/catalog/me", nil, ""); code != 200 || b["principal"] != "pricing" || b["canPublish"] != true {
		t.Fatalf("%d %v", code, b)
	}
	if code, b, _ := call(t, viewer, "GET", s.base+"/v1/catalog/me", nil, ""); code != 200 || b["principal"] != "care" || b["canPublish"] != false {
		t.Fatalf("%d %v", code, b)
	}

	// A browser on another site attaches the certificate but also an Origin.
	past := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	raw, _ := json.Marshal(map[string]any{"document": draft(past, 2)})
	post := func(origin string) int {
		req, _ := http.NewRequest("POST", s.base+"/v1/catalog/versions", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := editor.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("https://evil.example"); code != 403 {
		t.Fatalf("cross-origin publish: %d", code)
	}
	if n, _ := redis.NewClient(&redis.Options{Addr: s.rdb}).LLen(context.Background(), "ocs:catalog:versions").Result(); n != 0 {
		t.Fatalf("a cross-origin request published %d version(s)", n)
	}
	if code := post(s.base); code != 201 {
		t.Fatalf("same-origin publish: %d", code)
	}
}

// TestServeEditorForManualTesting serves the editor on OCS_UI_DEV_ADDR with
// authentication off and a small catalog, for a browser check:
//
//	OCS_UI_DEV_ADDR=127.0.0.1:18444 go test -run ServeEditorForManualTesting -timeout 15m ./services/ocs-catalog-service
func TestServeEditorForManualTesting(t *testing.T) {
	addr := os.Getenv("OCS_UI_DEV_ADDR")
	if addr == "" {
		t.Skip("set OCS_UI_DEV_ADDR (for example 127.0.0.1:8444) to serve the editor")
	}
	rdb := redistest.Start(t)
	seed := &catalog.Replicator{Redis: rdb, Store: catalog.NewStore(0)}
	doc := draft(time.Now().Add(-time.Hour), 2)
	doc.Note = "initial catalog"
	if _, _, err := seed.Publish(context.Background(), doc, 0); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"OCS_REDIS_ADDR": rdb.Options().Addr, "OCS_CATALOG_ADDR": addr, "OCS_CATALOG_AUTH": "off", "OCS_CATALOG_RESYNC": "1h"}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	app, err := NewApp(context.Background(), cfg, func(string) string { return "" }, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	secs := 3600
	if n, err := strconv.Atoi(os.Getenv("OCS_UI_DEV_SECONDS")); err == nil && n > 0 {
		secs = n
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(secs)*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	fmt.Printf("editor ready: http://%s/ui/\n", app.Addr())
	<-ctx.Done()
	<-done
	app.Close()
}

// /metrics says what happened to each publish and which version this node holds.
func TestMetricsCountPublishesAndShowVersions(t *testing.T) {
	s := startSystem(t)
	editor, viewer := s.client(t, "pricing"), s.client(t, "care")
	past := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	bad := draft(past, 2)
	bad.Offerings[0].PlanID = "ghost"

	publish := func(c *http.Client, doc catalog.Document, expect int64) int {
		code, _, _ := call(t, c, "POST", s.base+"/v1/catalog/versions", map[string]any{"document": doc, "expectedVersion": expect}, "application/json")
		return code
	}
	if c := publish(viewer, draft(past, 2), 1); c != 403 {
		t.Fatalf("forbidden: %d", c)
	}
	if c := publish(editor, bad, 1); c != 422 {
		t.Fatalf("invalid: %d", c)
	}
	if c := publish(editor, draft(past, 2), 1); c != 201 {
		t.Fatalf("ok: %d", c)
	}
	if c := publish(editor, draft(past.Add(time.Minute), 3), 1); c != 409 { // stale editor
		t.Fatalf("conflict: %d", c)
	}
	if c := publish(editor, draft(past, 3), 2); c != 422 { // not later than version 1
		t.Fatalf("not monotonic: %d", c)
	}
	call(t, viewer, "GET", s.base+"/v1/catalog/nope", nil, "") // a 404

	var sb strings.Builder
	if _, err := s.app.Metrics().WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	out := sb.String()
	for _, want := range []string{
		`ocs_catalog_publishes_total{result="ok"} 1`, `ocs_catalog_publishes_total{result="forbidden"} 1`,
		`ocs_catalog_publishes_total{result="invalid"} 1`, `ocs_catalog_publishes_total{result="conflict"} 1`,
		`ocs_catalog_publishes_total{result="not_monotonic"} 1`, `ocs_catalog_publishes_total{result="error"} 0`,
		"ocs_catalog_latest_version 1\n", "ocs_catalog_version_in_effect 1\n",
		`ocs_catalog_http_requests_total{code="4xx"}`, `ocs_catalog_http_request_duration_seconds_count`,
		"ocs_catalog_sync_errors_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "pricing") || strings.Contains(out, "care") {
		t.Error("a client name leaked into the metrics")
	}
}
