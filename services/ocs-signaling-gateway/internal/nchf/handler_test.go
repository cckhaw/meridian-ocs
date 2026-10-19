package nchf

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

func TestCreateMapsToCanonicalAndAnswers201(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, nil)
	w := post(h, prefix+"/chargingdata", baseCreate())

	if w.Code != http.StatusCreated {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatal(ct)
	}
	loc := w.Header().Get("Location")
	ref := refFromLocation(t, loc)
	if !strings.HasPrefix(loc, "http://ocs.example.net"+prefix+"/chargingdata/r1.") {
		t.Fatalf("Location %q", loc)
	}
	resp := decodeResp(t, w)
	if _, err := time.Parse(time.RFC3339, resp.InvocationTimeStamp); err != nil || resp.InvocationSequenceNumber == nil || *resp.InvocationSequenceNumber != 1 {
		t.Fatalf("%+v", resp)
	}
	m := resp.MUI[0]
	if m.RatingGroup != 100 || m.ResultCode != "SUCCESS" || m.Granted == nil || *m.Granted.TotalVolume != 1000 || *m.ValidityTime != 30 || m.FUI != nil {
		t.Fatalf("%+v", m)
	}

	r := c.req
	if r.Source != canonical.SourceNchf || r.Type != canonical.TypeInitial || r.SubscriberID != "262011234567890" ||
		r.SessionID != ref || r.RequestNumber != 1 || r.RATType != "NR" || r.Slice != "1-010203" ||
		!r.EventTime.Equal(time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("%+v", r)
	}
	if u := r.Units[0]; u.RatingGroup != 100 || u.Kind != canonical.UnitBytes || u.Requested != 1000 || u.Used != 0 {
		t.Fatalf("%+v", u)
	}
}

func TestBothAPIVersionsAreServed(t *testing.T) {
	h := newHandler(t, raterFunc(grantAll), nil, nil)
	for _, p := range []string{"/nchf-convergedcharging/v3", "/nchf-convergedcharging/v1"} {
		if w := post(h, p+"/chargingdata", baseCreate()); w.Code != http.StatusCreated {
			t.Errorf("%s: %d", p, w.Code)
		}
	}
	if w := post(h, "/nchf-convergedcharging/v9/chargingdata", baseCreate()); w.Code != http.StatusNotFound {
		t.Errorf("unknown version: %d", w.Code)
	}
}

func TestRetransmittedCreateReusesTheReference(t *testing.T) {
	var refs []string
	h := newHandler(t, raterFunc(grantAll), nil, nil)
	for i := 0; i < 2; i++ {
		w := post(h, prefix+"/chargingdata", baseCreate())
		refs = append(refs, refFromLocation(t, w.Header().Get("Location")))
	}
	if refs[0] != refs[1] {
		t.Fatalf("a retransmitted create must map to the same reference: %v", refs)
	}
	other := post(h, prefix+"/chargingdata", with(baseCreate(), func(o obj) {
		o["pDUSessionChargingInformation"].(obj)["pduSessionInformation"].(obj)["pduSessionID"] = 6
	}))
	if refFromLocation(t, other.Header().Get("Location")) == refs[0] {
		t.Fatal("another PDU session of the same subscriber must get its own reference")
	}
	later := post(h, prefix+"/chargingdata", with(baseCreate(), func(o obj) { o["invocationTimeStamp"] = "2026-10-19T08:00:01Z" }))
	if refFromLocation(t, later.Header().Get("Location")) == refs[0] {
		t.Fatal("a later create must get its own reference")
	}
}

func TestUpdateAndReleaseRecoverTheSubscriberFromTheReference(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, nil)
	ref := refFromLocation(t, post(h, prefix+"/chargingdata", baseCreate()).Header().Get("Location"))

	w := post(h, prefix+"/chargingdata/"+ref+"/update", baseUpdate(2, 400, 1000))
	if w.Code != http.StatusOK {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if r := c.req; r.Type != canonical.TypeUpdate || r.SubscriberID != "262011234567890" || r.SessionID != ref || r.RequestNumber != 2 ||
		r.Units[0].Used != 400 || r.Units[0].Requested != 1000 {
		t.Fatalf("%+v", r)
	}
	if got := *decodeResp(t, w).InvocationSequenceNumber; got != 2 {
		t.Fatalf("sequence number not echoed: %d", got)
	}

	rel := baseUpdate(3, 300, 0)
	w = post(h, prefix+"/chargingdata/"+ref+"/release", rel)
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
	if r := c.req; r.Type != canonical.TypeTerminate || r.Units[0].Used != 300 || r.Units[0].Requested != 0 {
		t.Fatalf("%+v", r)
	}

	// Release may carry no usage at all.
	noUsage := baseUpdate(4, 0, 0)
	delete(noUsage, "multipleUnitUsage")
	if w = post(h, prefix+"/chargingdata/"+ref+"/release", noUsage); w.Code != http.StatusNoContent {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestSubscriberInBodyMustAgreeWithReference(t *testing.T) {
	h := newHandler(t, raterFunc(grantAll), nil, nil)
	ref := refFromLocation(t, post(h, prefix+"/chargingdata", baseCreate()).Header().Get("Location"))
	ok := with(baseUpdate(2, 1, 1), func(o obj) { o["subscriberIdentifier"] = "imsi-262011234567890" })
	if w := post(h, prefix+"/chargingdata/"+ref+"/update", ok); w.Code != http.StatusOK {
		t.Fatalf("%d", w.Code)
	}
	bad := with(baseUpdate(3, 1, 1), func(o obj) { o["subscriberIdentifier"] = "imsi-999999999999999" })
	w := post(h, prefix+"/chargingdata/"+ref+"/update", bad)
	if w.Code != 400 || decodeProblem(t, w).Cause != "MANDATORY_IE_INCORRECT" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestPerUnitResultCodes(t *testing.T) {
	tests := []struct {
		res    canonical.Result
		code   string
		grant  int64
		final  bool
		hasGSU bool
	}{
		{canonical.ResultGranted, "SUCCESS", 1000, false, true},
		{canonical.ResultPartialFinal, "SUCCESS", 300, true, true},
		{canonical.ResultCreditLimitReached, "QUOTA_LIMIT_REACHED", 0, false, false},
		{canonical.ResultNoBucket, "RATING_FAILED", 0, false, false},
		{canonical.ResultUnavailable, "END_USER_SERVICE_DENIED", 0, false, false},
	}
	var cur canonical.UnitGrant
	h := newHandler(t, raterFunc(func(_ context.Context, r *canonical.Request, s *canonical.Response) error {
		g := cur
		g.RatingGroup = r.Units[0].RatingGroup
		s.Result = g.Result
		s.Grants = append(s.Grants, g)
		return nil
	}), nil, nil)
	for _, tc := range tests {
		cur = canonical.UnitGrant{Result: tc.res, Granted: tc.grant, FinalUnit: tc.final}
		if tc.grant > 0 {
			cur.ValiditySeconds = 30
		}
		w := post(h, prefix+"/chargingdata", baseCreate())
		if w.Code != http.StatusCreated {
			t.Errorf("%d: quota outcomes are not HTTP errors, got %d", tc.res, w.Code)
			continue
		}
		m := decodeResp(t, w).MUI[0]
		if m.ResultCode != tc.code || (m.Granted != nil) != tc.hasGSU || (m.FUI != nil) != tc.final {
			t.Errorf("%d: %+v", tc.res, m)
		}
		if m.FUI != nil && m.FUI.Action != "TERMINATE" {
			t.Errorf("%d: final unit action %q", tc.res, m.FUI.Action)
		}
		if tc.hasGSU && *m.Granted.TotalVolume != uint64(tc.grant) {
			t.Errorf("%d: granted %d", tc.res, *m.Granted.TotalVolume)
		}
	}
}

func TestGrantedUnitKinds(t *testing.T) {
	h := newHandler(t, raterFunc(grantAll), nil, nil)
	body := with(baseCreate(), func(o obj) {
		o["multipleUnitUsage"] = []obj{
			{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": 500}},
			{"ratingGroup": 2, "requestedUnit": obj{"time": 60}},
			{"ratingGroup": 3, "requestedUnit": obj{"serviceSpecificUnits": 2}},
		}
	})
	m := decodeResp(t, post(h, prefix+"/chargingdata", body)).MUI
	if *m[0].Granted.TotalVolume != 500 || *m[1].Granted.Time != 60 || *m[2].Granted.ServiceSpecificUnits != 2 {
		t.Fatalf("%+v", m)
	}
}

func TestEngineOutcomesMapToHTTPStatus(t *testing.T) {
	tests := []struct {
		name  string
		rate  func(context.Context, *canonical.Request, *canonical.Response) error
		code  int
		retry bool
	}{
		{"unknown session", func(_ context.Context, _ *canonical.Request, s *canonical.Response) error {
			s.Result = canonical.ResultUnknownSession
			return nil
		}, 404, false},
		{"stale", func(_ context.Context, _ *canonical.Request, s *canonical.Response) error {
			s.Result = canonical.ResultStaleRequest
			return nil
		}, 400, false},
		{"too busy", func(_ context.Context, _ *canonical.Request, s *canonical.Response) error {
			s.Result = canonical.ResultTooBusy
			return nil
		}, 429, true},
		{"engine overloaded", func(context.Context, *canonical.Request, *canonical.Response) error { return engine.ErrOverloaded }, 429, true},
		{"engine invalid", func(context.Context, *canonical.Request, *canonical.Response) error { return engine.ErrInvalid }, 400, false},
		{"engine closed", func(context.Context, *canonical.Request, *canonical.Response) error { return engine.ErrClosed }, 503, true},
		{"other error", func(context.Context, *canonical.Request, *canonical.Response) error { return errors.New("boom") }, 503, true},
	}
	for _, tc := range tests {
		h := newHandler(t, raterFunc(tc.rate), nil, nil)
		w := post(h, prefix+"/chargingdata/r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef/update", baseUpdate(2, 1, 1))
		if w.Code != tc.code {
			t.Errorf("%s: got %d want %d (%s)", tc.name, w.Code, tc.code, w.Body.String())
			continue
		}
		decodeProblem(t, w)
		if (w.Header().Get("Retry-After") != "") != tc.retry {
			t.Errorf("%s: Retry-After = %q", tc.name, w.Header().Get("Retry-After"))
		}
	}
}

func TestRejections(t *testing.T) {
	var calls int
	h := newHandler(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls++
		return grantAll(ctx, r, s)
	}), nil, nil)
	del := func(k string) func(obj) { return func(o obj) { delete(o, k) } }
	tests := []struct {
		name  string
		body  any
		code  int
		cause string
	}{
		{"not json", "{", 400, "INVALID_MSG_FORMAT"},
		{"wrong field type", with(baseCreate(), func(o obj) { o["invocationSequenceNumber"] = "one" }), 400, "INVALID_MSG_FORMAT"},
		{"negative volume", with(baseCreate(), func(o obj) {
			o["multipleUnitUsage"] = []obj{{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": -5}}}
		}), 400, "INVALID_MSG_FORMAT"},
		{"no nfConsumerIdentification", with(baseCreate(), del("nfConsumerIdentification")), 400, "MANDATORY_IE_MISSING"},
		{"no nodeFunctionality", with(baseCreate(), func(o obj) { o["nfConsumerIdentification"] = obj{"nFName": "x"} }), 400, "MANDATORY_IE_MISSING"},
		{"no timestamp", with(baseCreate(), del("invocationTimeStamp")), 400, "MANDATORY_IE_MISSING"},
		{"bad timestamp", with(baseCreate(), func(o obj) { o["invocationTimeStamp"] = "yesterday" }), 400, "MANDATORY_IE_INCORRECT"},
		{"no sequence number", with(baseCreate(), del("invocationSequenceNumber")), 400, "MANDATORY_IE_MISSING"},
		{"no subscriber", with(baseCreate(), del("subscriberIdentifier")), 400, "MANDATORY_IE_MISSING"},
		{"gpsi instead of supi", with(baseCreate(), func(o obj) { o["subscriberIdentifier"] = "msisdn-4917" }), 400, "MANDATORY_IE_INCORRECT"},
		{"no units", with(baseCreate(), del("multipleUnitUsage")), 400, "MANDATORY_IE_MISSING"},
		{"no rating group", with(baseCreate(), func(o obj) {
			o["multipleUnitUsage"] = []obj{{"requestedUnit": obj{"totalVolume": 1}}}
		}), 400, "MANDATORY_IE_MISSING"},
		{"mixed unit kinds", with(baseCreate(), func(o obj) {
			o["multipleUnitUsage"] = []obj{{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": 1, "time": 1}}}
		}), 400, "MANDATORY_IE_INCORRECT"},
		{"requested and used kinds differ", with(baseCreate(), func(o obj) {
			o["multipleUnitUsage"] = []obj{{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": 1},
				"usedUnitContainer": []obj{{"localSequenceNumber": 1, "time": 5}}}}
		}), 400, "MANDATORY_IE_INCORRECT"},
		{"event without type", with(baseCreate(), func(o obj) { o["oneTimeEvent"] = true }), 400, "MANDATORY_IE_MISSING"},
		{"pending event charging", with(baseCreate(), func(o obj) { o["oneTimeEvent"] = true; o["oneTimeEventType"] = "PEC" }), 501, ""},
	}
	for _, tc := range tests {
		w := post(h, prefix+"/chargingdata", tc.body)
		if w.Code != tc.code {
			t.Errorf("%s: got %d want %d (%s)", tc.name, w.Code, tc.code, w.Body.String())
			continue
		}
		if p := decodeProblem(t, w); p.Cause != tc.cause || p.Title == "" || p.Detail == "" {
			t.Errorf("%s: %+v", tc.name, p)
		}
	}
	many := make([]obj, 17)
	for i := range many {
		many[i] = obj{"ratingGroup": i + 1, "requestedUnit": obj{"totalVolume": 1}}
	}
	if w := post(h, prefix+"/chargingdata", with(baseCreate(), func(o obj) { o["multipleUnitUsage"] = many })); w.Code != 400 {
		t.Errorf("17 units: %d", w.Code)
	}
	if calls != 0 {
		t.Fatalf("%d invalid requests reached the rating engine", calls)
	}
}

func TestHTTPLevelErrors(t *testing.T) {
	h := newHandler(t, raterFunc(grantAll), nil, func(c *Config) { c.MaxBodyBytes = 512 })

	r := post(h, prefix+"/chargingdata", baseCreate())
	if r.Code != 201 {
		t.Fatal(r.Code)
	}

	get := httptestRequest(http.MethodGet, prefix+"/chargingdata", "", "")
	w := serve(h, get)
	if w.Code != 405 || w.Header().Get("Allow") != "POST" {
		t.Errorf("GET: %d Allow=%q", w.Code, w.Header().Get("Allow"))
	}
	decodeProblem(t, w)

	for name, ct := range map[string]string{"text": "text/plain", "none": "", "xml": "application/xml"} {
		if w := serve(h, httptestRequest(http.MethodPost, prefix+"/chargingdata", "{}", ct)); w.Code != 415 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}
	if w := serve(h, httptestRequest(http.MethodPost, prefix+"/chargingdata", "{}", "application/json; charset=utf-8")); w.Code != 400 {
		t.Errorf("content type with parameters must be accepted, got %d", w.Code)
	}

	for _, p := range []string{prefix, prefix + "/chargingdata/", prefix + "/chargingdata/x", prefix + "/chargingdata/x/delete",
		prefix + "/chargingdata//update", "/other"} {
		if w := post(h, p, "{}"); w.Code != 404 {
			t.Errorf("%s: %d", p, w.Code)
		}
	}
	for _, ref := range []string{"nodots", "r1..0123456789abcdef", "r1.!!!.0123456789abcdef", "r1.MjYy.short", ".MjYy.0123456789abcdef"} {
		if w := post(h, prefix+"/chargingdata/"+ref+"/update", baseUpdate(2, 1, 1)); w.Code != 404 {
			t.Errorf("ref %q: %d", ref, w.Code)
		}
	}

	big := strings.Repeat("x", 600)
	if w := post(h, prefix+"/chargingdata", `{"pad":"`+big+`"}`); w.Code != 413 {
		t.Errorf("oversized body: %d", w.Code)
	}
}

func TestUnitDecoding(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, nil)
	tests := []struct {
		name string
		mu   obj
		want canonical.UnitRequest
	}{
		{"uplink+downlink without total", obj{"ratingGroup": 1, "requestedUnit": obj{"uplinkVolume": 300, "downlinkVolume": 200}},
			canonical.UnitRequest{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 500}},
		{"total wins", obj{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": 900, "uplinkVolume": 1, "downlinkVolume": 1}},
			canonical.UnitRequest{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 900}},
		{"time", obj{"ratingGroup": 2, "requestedUnit": obj{"time": 60}},
			canonical.UnitRequest{RatingGroup: 2, Kind: canonical.UnitSeconds, Requested: 60}},
		{"events", obj{"ratingGroup": 3, "requestedUnit": obj{"serviceSpecificUnits": 1}},
			canonical.UnitRequest{RatingGroup: 3, Kind: canonical.UnitEvents, Requested: 1}},
		{"empty requestedUnit uses the default of the usage kind", obj{"ratingGroup": 4, "requestedUnit": obj{},
			"usedUnitContainer": []obj{{"localSequenceNumber": 1, "time": 5}}},
			canonical.UnitRequest{RatingGroup: 4, Kind: canonical.UnitSeconds, Requested: 300, Used: 5}},
		{"empty requestedUnit defaults to bytes", obj{"ratingGroup": 5, "requestedUnit": obj{}},
			canonical.UnitRequest{RatingGroup: 5, Kind: canonical.UnitBytes, Requested: 10 << 20}},
		{"usage only asks for nothing", obj{"ratingGroup": 6, "usedUnitContainer": []obj{{"localSequenceNumber": 1, "totalVolume": 77}}},
			canonical.UnitRequest{RatingGroup: 6, Kind: canonical.UnitBytes, Used: 77}},
		{"containers are summed and the service id kept", obj{"ratingGroup": 7, "requestedUnit": obj{"totalVolume": 10},
			"usedUnitContainer": []obj{
				{"localSequenceNumber": 1, "serviceId": 9, "totalVolume": 100},
				{"localSequenceNumber": 2, "uplinkVolume": 20, "downlinkVolume": 30}}},
			canonical.UnitRequest{RatingGroup: 7, ServiceID: 9, Kind: canonical.UnitBytes, Requested: 10, Used: 150}},
	}
	for _, tc := range tests {
		ref := "r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef"
		body := baseUpdate(2, 0, 0)
		body["multipleUnitUsage"] = []obj{tc.mu}
		if w := post(h, prefix+"/chargingdata/"+ref+"/update", body); w.Code != 200 {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
			continue
		}
		if got := c.req.Units[0]; got != tc.want {
			t.Errorf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}
}

func TestHugeVolumesAreFlaggedNotWrapped(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, nil)
	body := with(baseCreate(), func(o obj) {
		o["multipleUnitUsage"] = []obj{{"ratingGroup": 1, "requestedUnit": obj{"totalVolume": uint64(1) << 63}}}
	})
	raw := strings.Replace(mustJSON(body), "9223372036854775808", "18446744073709551615", 1)
	if w := post(h, prefix+"/chargingdata", raw); w.Code != 201 {
		t.Fatalf("%d", w.Code)
	}
	if got := c.req.Units[0].Requested; got <= 1<<40 {
		t.Fatalf("an out-of-range volume must stay out of range so the data layer refuses it, got %d", got)
	}
}

func TestOneTimeEventImmediateCharging(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, nil)
	body := with(baseCreate(), func(o obj) {
		o["oneTimeEvent"] = true
		o["oneTimeEventType"] = "IEC"
		o["multipleUnitUsage"] = []obj{{"ratingGroup": 7, "requestedUnit": obj{"serviceSpecificUnits": 1}}}
	})
	w := post(h, prefix+"/chargingdata", body)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if r := c.req; r.Type != canonical.TypeEvent || r.Units[0].Kind != canonical.UnitEvents || r.Units[0].Requested != 1 {
		t.Fatalf("%+v", r)
	}
	ref1 := refFromLocation(t, w.Header().Get("Location"))
	again := with(body, func(o obj) { o["invocationSequenceNumber"] = 2 })
	if refFromLocation(t, post(h, prefix+"/chargingdata", again).Header().Get("Location")) == ref1 {
		t.Fatal("two distinct events must not share a reference")
	}
	if ref2 := refFromLocation(t, post(h, prefix+"/chargingdata", body).Header().Get("Location")); ref2 != ref1 {
		t.Fatal("a retransmitted event must keep its reference (so it is not charged twice)")
	}
}

func TestSubscriberIdentifierHandling(t *testing.T) {
	var c capture
	h := newHandler(t, c.rater(grantAll), nil, func(cfg *Config) {
		cfg.ResolveSubscriber = func(k string) (string, bool) {
			if k == "262011234567890" {
				return "491701234567", true // IMSI -> MSISDN, as the Gy side keys it
			}
			return "", false
		}
	})
	w := post(h, prefix+"/chargingdata", baseCreate())
	if w.Code != 201 || c.req.SubscriberID != "491701234567" {
		t.Fatalf("%d %q", w.Code, c.req.SubscriberID)
	}
	ref := refFromLocation(t, w.Header().Get("Location"))
	if w := post(h, prefix+"/chargingdata/"+ref+"/update", baseUpdate(2, 1, 1)); w.Code != 200 || c.req.SubscriberID != "491701234567" {
		t.Fatalf("the resolved key must be what the reference carries: %d %q", w.Code, c.req.SubscriberID)
	}
	unknown := with(baseCreate(), func(o obj) { o["subscriberIdentifier"] = "imsi-999" })
	if w := post(h, prefix+"/chargingdata", unknown); w.Code != 400 {
		t.Fatalf("unresolvable subscriber: %d", w.Code)
	}

	h2 := newHandler(t, c.rater(grantAll), nil, nil)
	nai := with(baseCreate(), func(o obj) { o["subscriberIdentifier"] = "nai-user@example.com" })
	if w := post(h2, prefix+"/chargingdata", nai); w.Code != 201 || c.req.SubscriberID != "user@example.com" {
		t.Fatalf("%d %q", w.Code, c.req.SubscriberID)
	}
}

func TestForeignRegionReferences(t *testing.T) {
	h := newHandler(t, raterFunc(grantAll), nil, func(c *Config) {
		c.Region = "eu1"
		c.RegionBaseURLs = map[string]string{"us1": "https://ocs.us1.example.net"}
	})
	ref := "us1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef"
	w := post(h, prefix+"/chargingdata/"+ref+"/update", baseUpdate(2, 1, 1))
	if w.Code != http.StatusTemporaryRedirect || w.Header().Get("Location") != "https://ocs.us1.example.net"+prefix+"/chargingdata/"+ref+"/update" {
		t.Fatalf("%d %q", w.Code, w.Header().Get("Location"))
	}
	w = post(h, prefix+"/chargingdata/ap1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef/update", baseUpdate(2, 1, 1))
	if w.Code != 404 {
		t.Fatalf("an unknown region must be a 404, got %d", w.Code)
	}
	// New sessions are always minted in the local region.
	loc := post(h, prefix+"/chargingdata", baseCreate()).Header().Get("Location")
	if !strings.Contains(loc, "/chargingdata/eu1.") {
		t.Fatal(loc)
	}
	if _, err := NewHandler(Config{Region: "a.b"}, raterFunc(grantAll), nil); err == nil {
		t.Fatal("a region containing '.' would break reference parsing")
	}
}

func TestAdmissionControlShedsWith429BeforeRating(t *testing.T) {
	var calls int
	h := newHandler(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls++
		return grantAll(ctx, r, s)
	}), admission.New(1, 1, 0), nil)
	if w := post(h, prefix+"/chargingdata", baseCreate()); w.Code != 201 {
		t.Fatal(w.Code)
	}
	w := post(h, prefix+"/chargingdata", baseCreate())
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %q", w.Code, w.Header().Get("Retry-After"))
	}
	decodeProblem(t, w)
	if calls != 1 || h.Stats().Shed != 1 {
		t.Fatalf("calls=%d %+v", calls, h.Stats())
	}
}

func TestClientDisconnectDoesNotCancelTheChargingCall(t *testing.T) {
	var ctxErr error
	h := newHandler(t, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		ctxErr = ctx.Err()
		return grantAll(ctx, r, s)
	}), nil, nil)
	req := httptestRequest(http.MethodPost, prefix+"/chargingdata", mustJSON(baseCreate()), "application/json")
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	serve(h, req.WithContext(ctx))
	if ctxErr != nil {
		t.Fatalf("the rating call saw a cancelled context: %v", ctxErr)
	}
}
