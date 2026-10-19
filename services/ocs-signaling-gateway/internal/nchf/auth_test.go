package nchf

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/oauth"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/oauth/oauthtest"
)

func newVerifier(t testing.TB, iss *oauthtest.Issuer) *oauth.Verifier {
	t.Helper()
	ks, _, err := oauth.ParseKeySet(iss.PEM, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	v, err := oauth.NewVerifier(oauth.Config{Audience: []string{"CHF"}}, ks)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func secured(t testing.TB, iss *oauthtest.Issuer, r Rater, adm *admission.Admission) *Handler {
	t.Helper()
	h, err := NewHandler(Config{Auth: newVerifier(t, iss)}, r, adm)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func postWith(h http.Handler, path string, body any, authorization ...string) *httptest.ResponseRecorder {
	r := httptestRequest(http.MethodPost, prefix+path, mustJSON(body), "application/json")
	for _, a := range authorization {
		r.Header.Add("Authorization", a)
	}
	return serve(h, r)
}

func TestHandlerRequiresAnAuthenticatorUnlessExplicitlyDisabled(t *testing.T) {
	if _, err := NewHandler(Config{}, raterFunc(grantAll), nil); err == nil {
		t.Fatal("a handler with no authenticator and no explicit opt-out must not be constructible")
	}
	if _, err := NewHandler(Config{AllowUnauthenticated: true}, raterFunc(grantAll), nil); err != nil {
		t.Fatal(err)
	}
}

func TestUnauthenticatedRequestsNeverReachTheRatingEngine(t *testing.T) {
	iss, other := oauthtest.NewIssuer(t), oauthtest.NewIssuer(t)
	var calls int
	h := secured(t, iss, raterFunc(func(ctx context.Context, r *canonical.Request, s *canonical.Response) error {
		calls++
		return grantAll(ctx, r, s)
	}), nil)
	ref := "r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef"

	const wantRealm = `Bearer realm="nchf-convergedcharging"`
	tests := []struct {
		name string
		auth []string
		code int
		www  string
	}{
		{"no Authorization header", nil, 401, wantRealm},
		{"signed by a different issuer", []string{"Bearer " + other.Token(t)}, 401, wantRealm + `, error="invalid_token"`},
		{"expired", []string{"Bearer " + iss.Token(t, oauthtest.Expires(time.Now().Add(-time.Hour)))}, 401, wantRealm + `, error="invalid_token"`},
		{"garbage token", []string{"Bearer not-a-token"}, 401, wantRealm + `, error="invalid_token"`},
		{"Basic scheme", []string{"Basic dXNlcjpwYXNz"}, 401, wantRealm + `, error="invalid_token"`},
		{"scheme without a token", []string{"Bearer "}, 401, wantRealm + `, error="invalid_token"`},
		{"two Authorization headers", []string{"Bearer " + iss.Token(t), "Bearer " + iss.Token(t)}, 401, wantRealm + `, error="invalid_token"`},
		{"token for another service", []string{"Bearer " + iss.Token(t, oauthtest.Audience("UDM"))}, 403, wantRealm + `, error="insufficient_scope", scope="nchf-convergedcharging"`},
		{"token without the scope", []string{"Bearer " + iss.Token(t, oauthtest.Scope("nudm-sdm"))}, 403, wantRealm + `, error="insufficient_scope", scope="nchf-convergedcharging"`},
	}
	for _, op := range []struct {
		path string
		body any
	}{
		{"/chargingdata", baseCreate()},
		{"/chargingdata/" + ref + "/update", baseUpdate(2, 1, 1)},
		{"/chargingdata/" + ref + "/release", baseUpdate(3, 1, 0)},
	} {
		for _, tc := range tests {
			w := postWith(h, op.path, op.body, tc.auth...)
			if w.Code != tc.code || w.Header().Get("WWW-Authenticate") != tc.www {
				t.Errorf("%s %s: %d WWW-Authenticate=%q", op.path, tc.name, w.Code, w.Header().Get("WWW-Authenticate"))
				continue
			}
			p := decodeProblem(t, w)
			if strings.Contains(p.Detail, "signature") || strings.Contains(p.Detail, "audience") || strings.Contains(p.Detail, "scope") {
				t.Errorf("%s: the answer leaks why the token was refused: %q", tc.name, p.Detail)
			}
		}
	}
	if calls != 0 {
		t.Fatalf("%d unauthenticated requests reached the rating engine", calls)
	}
	st := h.Stats()
	if st.Unauthorized != 3*7 || st.Forbidden != 3*2 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestValidTokenIsAcceptedOnEveryOperation(t *testing.T) {
	iss := oauthtest.NewIssuer(t)
	h := secured(t, iss, raterFunc(grantAll), nil)
	tok := iss.Token(t)

	w := postWith(h, "/chargingdata", baseCreate(), "Bearer "+tok)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	ref := refFromLocation(t, w.Header().Get("Location"))
	if w = postWith(h, "/chargingdata/"+ref+"/update", baseUpdate(2, 5, 5), "bearer  "+tok); w.Code != 200 {
		t.Fatalf("scheme is case-insensitive: %d", w.Code)
	}
	if w = postWith(h, "/chargingdata/"+ref+"/release", baseUpdate(3, 5, 0), "Bearer "+tok); w.Code != 204 {
		t.Fatalf("%d", w.Code)
	}
}

// A flood of bad tokens must neither read request bodies nor use up the
// admission budget that real traffic needs.
func TestAuthenticationHappensBeforeTheBodyIsReadAndBeforeAdmission(t *testing.T) {
	iss := oauthtest.NewIssuer(t)
	adm := admission.New(1, 1, 0) // a single request, then nothing for a long time
	h := secured(t, iss, raterFunc(grantAll), adm)

	for i := 0; i < 20; i++ {
		r := httptest.NewRequest(http.MethodPost, "http://x"+prefix+"/chargingdata", &explodingBody{t: t})
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer forged")
		if w := serve(h, r); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	if w := postWith(h, "/chargingdata", baseCreate(), "Bearer "+iss.Token(t)); w.Code != 201 {
		t.Fatalf("the budget was spent by unauthenticated traffic: %d", w.Code)
	}
}

type explodingBody struct{ t testing.TB }

func (b *explodingBody) Read([]byte) (int, error) {
	b.t.Error("the request body was read before the token was checked")
	return 0, io.EOF
}

func TestRepeatedUseOfATokenDoesNotRepeatTheSignatureCheck(t *testing.T) {
	iss := oauthtest.NewIssuer(t)
	v := newVerifier(t, iss)
	h, _ := NewHandler(Config{Auth: v}, raterFunc(grantAll), nil)
	tok := iss.Token(t)
	for i := 0; i < 10; i++ {
		if w := postWith(h, "/chargingdata", baseCreate(), "Bearer "+tok); w.Code != 201 {
			t.Fatal(w.Code)
		}
	}
	if st := v.Stats(); st.Verified != 1 || st.CacheHits != 9 {
		t.Fatalf("%+v", st)
	}
}

func TestAuthenticatorErrorsOtherThanForbiddenAreUnauthorized(t *testing.T) {
	h, _ := NewHandler(Config{Auth: failingAuth{errors.New("boom")}}, raterFunc(grantAll), nil)
	if w := postWith(h, "/chargingdata", baseCreate(), "Bearer x"); w.Code != 401 {
		t.Fatalf("an unexpected verifier failure must fail closed: %d", w.Code)
	}
}

type failingAuth struct{ err error }

func (f failingAuth) Verify(string) (*oauth.Claims, error) { return nil, f.err }

func BenchmarkAuthenticatedUpdate(b *testing.B) {
	iss := oauthtest.NewIssuer(b)
	h, _ := NewHandler(Config{Auth: newVerifier(b, iss)}, raterFunc(grantAll), nil)
	body := mustJSON(baseUpdate(2, 400, 1000))
	path := prefix + "/chargingdata/r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef/update"
	tok := "Bearer " + iss.Token(b)
	r := httptest.NewRequest(http.MethodPost, "http://ocs.example.net"+path, nil)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", tok)
	w := &discardWriter{h: http.Header{}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Body = io.NopCloser(bytes.NewReader([]byte(body)))
		clear(w.h)
		h.ServeHTTP(w, r)
		if w.code != 200 {
			b.Fatal(w.code)
		}
	}
}
