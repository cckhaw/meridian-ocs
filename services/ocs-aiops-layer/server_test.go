package main

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/mapping"
)

func post(h http.Handler, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(body)))
	return w
}

func TestValidateReportsEveryProblem(t *testing.T) {
	h := New(true)
	w := post(h, "/v1/mappings/validate", string(mapping.ExampleAcmePGW))
	var ok problemList
	json.Unmarshal(w.Body.Bytes(), &ok)
	if w.Code != 200 || !ok.Valid || ok.Name != "acme-pgw" || len(ok.Problems) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	w = post(h, "/v1/mappings/validate", "version: 2\nname: x y\nrules:\n - id: r\n   output:\n    k: {path: nonsense, as: telepathy}\n")
	var bad problemList
	json.Unmarshal(w.Body.Bytes(), &bad)
	if w.Code != 200 || bad.Valid || len(bad.Problems) < 3 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	w = post(h, "/v1/mappings/validate", "version: [1")
	json.Unmarshal(w.Body.Bytes(), &bad)
	if bad.Valid || len(bad.Problems) != 1 {
		t.Fatalf("%s", w.Body.String())
	}
}

func TestTestAppliesTheDraftToSamples(t *testing.T) {
	h := New(true)
	req, _ := json.Marshal(map[string]any{"mapping": string(mapping.ExampleAcmePGW), "samples": []json.RawMessage{
		json.RawMessage(`{"recordType":"pgw","node":{"name":"p"},"chargingId":1,"subscriber":{"imsi":"262011234567890"},"openedAtMs":1893499200000,"rat":"6"}`),
		json.RawMessage(`{"type":"PGW","nodeId":"already-normalized"}`),
		json.RawMessage(`{"recordType":"pgw","subscriber":{"imsi":"1"}}`),
	}})
	w := post(h, "/v1/mappings/test", string(req))
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	var res struct {
		Results []sampleResult `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || len(res.Results) != 3 {
		t.Fatalf("%v %s", err, w.Body.String())
	}
	if r := res.Results[0]; r.Outcome != "translated" || r.RuleID != "acme-pgw" || !strings.Contains(string(r.Output), `"ratType":"NR"`) {
		t.Fatalf("%+v", r)
	}
	if res.Results[1].Outcome != "no_match" {
		t.Fatalf("%+v", res.Results[1])
	}
	if r := res.Results[2]; r.Outcome != "error" || r.Error == "" {
		t.Fatalf("%+v", r)
	}

	// An invalid draft is 422 with the problems, and nothing is applied.
	bad, _ := json.Marshal(map[string]any{"mapping": "version: 1\nname: a\nrules: []\n", "samples": []json.RawMessage{json.RawMessage(`{}`)}})
	if w = post(h, "/v1/mappings/test", string(bad)); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestRequestLimitsAndStrictness(t *testing.T) {
	h := New(true)
	for name, body := range map[string]string{
		"unknown field": `{"mapping":"x","samples":[{}],"surprise":1}`,
		"no samples":    `{"mapping":"x","samples":[]}`,
		"not json":      `nope`,
		"too many":      `{"mapping":"x","samples":[` + strings.TrimSuffix(strings.Repeat("{},", maxSamples+1), ",") + `]}`,
	} {
		if w := post(h, "/v1/mappings/test", body); w.Code != 400 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}
	if w := post(h, "/v1/mappings/test", strings.Repeat("x", maxBody+1)); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized: %d", w.Code)
	}
}

func TestSchemaAndExample(t *testing.T) {
	h := New(true)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/schema/mapping", nil))
	var schema map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &schema) != nil || schema["title"] != "OCS declarative mapping" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/examples/mapping", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "name: acme-pgw") {
		t.Fatalf("%d", w.Code)
	}
}

func TestAClientCertificateIsRequiredUnlessAuthIsOff(t *testing.T) {
	h := New(false)
	w := post(h, "/v1/mappings/validate", string(mapping.ExampleAcmePGW))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("%d", w.Code)
	}
	r := httptest.NewRequest("POST", "/v1/mappings/validate", strings.NewReader(string(mapping.ExampleAcmePGW)))
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "agent"}}}}}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("%d", w.Code)
	}
}

func TestConfig(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	for name, e := range map[string]map[string]string{
		"auth unset":  {},
		"auth typo":   {"OCS_AIOPS_AUTH": "tls"},
		"mtls no TLS": {"OCS_AIOPS_AUTH": "mtls"},
	} {
		if _, err := LoadConfig(env(e)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if c, err := LoadConfig(env(map[string]string{"OCS_AIOPS_AUTH": "off"})); err != nil || c.Addr != ":8445" {
		t.Fatalf("%+v %v", c, err)
	}
}
