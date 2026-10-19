package nchf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

type raterFunc func(ctx context.Context, req *canonical.Request, resp *canonical.Response) error

func (f raterFunc) Rate(ctx context.Context, req *canonical.Request, resp *canonical.Response) error {
	return f(ctx, req, resp)
}

func grantAll(_ context.Context, req *canonical.Request, resp *canonical.Response) error {
	resp.Result = canonical.ResultGranted
	for _, u := range req.Units {
		resp.Grants = append(resp.Grants, canonical.UnitGrant{
			RatingGroup: u.RatingGroup, Result: canonical.ResultGranted, Granted: u.Requested, ValiditySeconds: 30})
	}
	return nil
}

const prefix = "/nchf-convergedcharging/v3"

type obj = map[string]any

// baseCreate is a valid SMF create request.
func baseCreate() obj {
	return obj{
		"subscriberIdentifier":     "imsi-262011234567890",
		"nfConsumerIdentification": obj{"nodeFunctionality": "SMF", "nFName": "smf1"},
		"invocationTimeStamp":      "2026-10-19T08:00:00Z",
		"invocationSequenceNumber": 1,
		"multipleUnitUsage": []obj{{
			"ratingGroup":   100,
			"requestedUnit": obj{"totalVolume": 1000},
		}},
		"pDUSessionChargingInformation": obj{"pduSessionInformation": obj{
			"pduSessionID": 5, "ratType": "NR",
			"networkSlicingInfo": obj{"sNSSAI": obj{"sst": 1, "sd": "010203"}},
		}},
	}
}

// baseUpdate is a valid update (no subscriber: it comes from the reference).
func baseUpdate(seq int, used, requested uint64) obj {
	mu := obj{"ratingGroup": 100, "usedUnitContainer": []obj{{"localSequenceNumber": 1, "totalVolume": used}}}
	if requested > 0 {
		mu["requestedUnit"] = obj{"totalVolume": requested}
	}
	return obj{
		"nfConsumerIdentification": obj{"nodeFunctionality": "SMF", "nFName": "smf1"},
		"invocationTimeStamp":      "2026-10-19T08:00:30Z",
		"invocationSequenceNumber": seq,
		"multipleUnitUsage":        []obj{mu},
	}
}

func with(base obj, mut func(obj)) obj {
	// deep copy through JSON so mutations never leak between tests
	raw, _ := json.Marshal(base)
	var out obj
	_ = json.Unmarshal(raw, &out)
	mut(out)
	return out
}

func newHandler(t testing.TB, r Rater, adm *admission.Admission, mod func(*Config)) *Handler {
	t.Helper()
	cfg := Config{AllowUnauthenticated: true}
	if mod != nil {
		mod(&cfg)
	}
	h, err := NewHandler(cfg, r, adm)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func post(h http.Handler, path string, body any) *httptest.ResponseRecorder {
	var s string
	switch b := body.(type) {
	case string:
		s = b
	default:
		raw, _ := json.Marshal(b)
		s = string(raw)
	}
	r := httptest.NewRequest(http.MethodPost, "http://ocs.example.net"+path, strings.NewReader(s))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type muInfo struct {
	RatingGroup uint32 `json:"ratingGroup"`
	ResultCode  string `json:"resultCode"`
	Granted     *struct {
		Time                 *uint32 `json:"time"`
		TotalVolume          *uint64 `json:"totalVolume"`
		ServiceSpecificUnits *uint64 `json:"serviceSpecificUnits"`
	} `json:"grantedUnit"`
	ValidityTime *uint32 `json:"validityTime"`
	FUI          *struct {
		Action string `json:"finalUnitAction"`
	} `json:"finalUnitIndication"`
}

type chargingResponse struct {
	InvocationTimeStamp      string   `json:"invocationTimeStamp"`
	InvocationSequenceNumber *uint32  `json:"invocationSequenceNumber"`
	MUI                      []muInfo `json:"multipleUnitInformation"`
}

func decodeResp(t testing.TB, w *httptest.ResponseRecorder) chargingResponse {
	t.Helper()
	var r chargingResponse
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		t.Fatalf("response is not valid JSON: %v\n%s", err, w.Body.String())
	}
	return r
}

type problemBody struct {
	Title  string `json:"title"`
	Status int    `json:"status"`
	Cause  string `json:"cause"`
	Detail string `json:"detail"`
}

func decodeProblem(t testing.TB, w *httptest.ResponseRecorder) problemBody {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type %q, body %s", ct, w.Body.String())
	}
	var p problemBody
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatalf("problem is not valid JSON: %v\n%s", err, w.Body.String())
	}
	if p.Status != w.Code {
		t.Fatalf("problem status %d != HTTP %d", p.Status, w.Code)
	}
	return p
}

// refFromLocation extracts the ChargingDataRef from a Location header.
func refFromLocation(t testing.TB, loc string) string {
	t.Helper()
	i := strings.LastIndex(loc, "/chargingdata/")
	if i < 0 {
		t.Fatalf("bad Location %q", loc)
	}
	return loc[i+len("/chargingdata/"):]
}

// capture records the canonical request the handler built.
type capture struct {
	req canonical.Request
	n   int
}

func (c *capture) rater(inner func(context.Context, *canonical.Request, *canonical.Response) error) Rater {
	return raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		c.n++
		c.req = *r
		c.req.Units = append([]canonical.UnitRequest(nil), r.Units...)
		return inner(ctx, r, s)
	})
}

func mustJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func httptestRequest(method, path, body, contentType string) *http.Request {
	r := httptest.NewRequest(method, "http://ocs.example.net"+path, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	return r
}

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
