// Package api is the catalog management REST API, built for a catalog editor
// UI: everything it needs to render a form (the schema endpoint), check a
// draft (validate, dry-run) and publish it.
//
//	GET  /v1/catalog                        the version in effect (?at=<RFC3339> or ?version=<n>)
//	GET  /v1/catalog/versions               retained versions, oldest first
//	GET  /v1/catalog/offerings/{id}         one offering of the version in effect (?at=)
//	GET  /v1/catalog/schema                 editor metadata: variables, functions, enums, limits
//	POST /v1/catalog/validate               validate a draft document
//	POST /v1/catalog/dry-run                price synthetic requests under the draft and the live version
//	POST /v1/catalog/versions               publish a draft as the next version   (publisher)
//	GET  /v1/catalog/me                     who the caller is and whether it may publish
//	GET  /ui/                               the catalog editor (web console)
//
// A publish is validated exactly as /validate does, is atomic cluster-wide and
// is rejected with 409 when the editor's expectedVersion is stale.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/catalog/expr"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/services/ocs-catalog-service/internal/ui"
)

// MaxBody bounds request bodies (a large catalog is a few hundred KiB).
const MaxBody = 8 << 20

// Publisher appends a validated document as the next version cluster-wide.
type Publisher interface {
	Publish(ctx context.Context, doc catalog.Document, expect int64) (*catalog.Snapshot, []catalog.Problem, error)
}

// Handler serves the API.
type Handler struct {
	store      *catalog.Store
	pub        Publisher
	publishers map[string]bool // CNs allowed to publish; nil with open=true means anyone
	open       bool            // authentication off
	now        func() time.Time
	mux        *http.ServeMux
	// Logf records publishes (who, which version); nil discards.
	Logf func(format string, args ...any)
}

// New builds the handler. With open (authentication off) every caller may do
// everything; otherwise a caller needs a verified client certificate and only
// the listed common names may publish.
func New(store *catalog.Store, pub Publisher, open bool, publishers []string) *Handler {
	h := &Handler{store: store, pub: pub, open: open, now: time.Now, mux: http.NewServeMux(),
		publishers: map[string]bool{}}
	for _, p := range publishers {
		h.publishers[p] = true
	}
	h.mux.HandleFunc("GET /v1/catalog", h.get)
	h.mux.HandleFunc("GET /v1/catalog/versions", h.versions)
	h.mux.HandleFunc("GET /v1/catalog/offerings/{id}", h.offering)
	h.mux.HandleFunc("GET /v1/catalog/schema", h.schema)
	h.mux.HandleFunc("POST /v1/catalog/validate", h.validate)
	h.mux.HandleFunc("POST /v1/catalog/dry-run", h.dryRun)
	h.mux.HandleFunc("POST /v1/catalog/versions", h.publish)
	h.mux.HandleFunc("GET /v1/catalog/me", h.me)
	h.mux.Handle("GET /ui/", ui.Handler())
	h.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	return h
}

// ServeHTTP implements http.Handler. Requests that change state, or that only
// check a draft, are refused when a browser says they come from another origin:
// with client-certificate authentication the browser attaches the certificate
// to any request, so without this check a page on another site could publish a
// catalog for a user who has a certificate installed. (The JSON content type
// already forces a CORS pre-flight that is never granted; this is the second
// layer.)
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
	default:
		if o := r.Header.Get("Origin"); o != "" {
			if u, err := url.Parse(o); err != nil || u.Host != r.Host {
				fail(w, http.StatusForbidden, "cross_origin", "cross-origin request refused", nil)
				return
			}
		}
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) logf(format string, args ...any) {
	if h.Logf != nil {
		h.Logf(format, args...)
	}
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	cn, ok := h.authn(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"principal":  cn,
		"canPublish": h.open || h.publishers[cn],
		"authOff":    h.open,
	})
}

type errBody struct {
	Error struct {
		Code     string            `json:"code"`
		Message  string            `json:"message"`
		Problems []catalog.Problem `json:"problems,omitempty"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code, msg string, probs []catalog.Problem) {
	var b errBody
	b.Error.Code, b.Error.Message, b.Error.Problems = code, msg, probs
	writeJSON(w, status, b)
}

// authn returns false (and answers) when the caller is not allowed in.
func (h *Handler) authn(w http.ResponseWriter, r *http.Request) (cn string, ok bool) {
	if h.open {
		return "", true
	}
	cn = tlsconf.PeerCommonName(r.TLS)
	if cn == "" {
		fail(w, http.StatusUnauthorized, "unauthenticated", "a verified client certificate is required", nil)
		return "", false
	}
	return cn, true
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if ct := r.Header.Get("Content-Type"); len(ct) < 16 || !strings.EqualFold(ct[:16], "application/json") {
		fail(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", nil)
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			fail(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large", nil)
		} else {
			fail(w, http.StatusBadRequest, "invalid_json", "invalid JSON body: "+err.Error(), nil)
		}
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		fail(w, http.StatusBadRequest, "invalid_json", "invalid JSON body: unexpected data after the object", nil)
		return false
	}
	return true
}

type versionView struct {
	Version       int64     `json:"version"`
	EffectiveFrom time.Time `json:"effectiveFrom"`
	Note          string    `json:"note,omitempty"`
	InEffect      bool      `json:"inEffect"`
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	q := r.URL.Query()
	var sn *catalog.Snapshot
	switch {
	case q.Get("version") != "":
		v, err := strconv.ParseInt(q.Get("version"), 10, 64)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_parameter", "version must be a number", nil)
			return
		}
		sn, _ = h.store.Version(v)
	case q.Get("at") != "":
		at, err := time.Parse(time.RFC3339, q.Get("at"))
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_parameter", "at must be an RFC 3339 time", nil)
			return
		}
		sn = h.store.At(at)
	default:
		sn = h.store.At(h.now())
	}
	if sn == nil {
		fail(w, http.StatusNotFound, "not_found", "no such catalog version", nil)
		return
	}
	writeJSON(w, http.StatusOK, sn.Document())
}

func (h *Handler) versions(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	cur := h.store.At(h.now())
	out := []versionView{}
	for _, sn := range h.store.Versions() {
		out = append(out, versionView{sn.Version, sn.EffectiveFrom, sn.Document().Note, cur != nil && cur.Version == sn.Version})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) offering(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	at := h.now()
	if s := r.URL.Query().Get("at"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_parameter", "at must be an RFC 3339 time", nil)
			return
		}
		at = t
	}
	if sn := h.store.At(at); sn != nil {
		if o, ok := sn.Offering(r.PathValue("id")); ok {
			writeJSON(w, http.StatusOK, o)
			return
		}
	}
	fail(w, http.StatusNotFound, "not_found", "no such offering", nil)
}

type validateResult struct {
	Valid    bool              `json:"valid"`
	Errors   []catalog.Problem `json:"errors"`
	Warnings []catalog.Problem `json:"warnings"`
}

func problems(err error) []catalog.Problem {
	var ve *catalog.ValidationError
	if errors.As(err, &ve) {
		return ve.Problems
	}
	return []catalog.Problem{{Path: "", Message: err.Error()}}
}

func (h *Handler) validate(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	var doc catalog.Document
	if !decode(w, r, &doc) {
		return
	}
	doc.Version = h.store.LatestVersion() + 1
	_, warns, err := catalog.Compile(doc)
	res := validateResult{Valid: err == nil, Errors: []catalog.Problem{}, Warnings: nonNil(warns)}
	if err != nil {
		res.Errors = problems(err)
	} else if last := h.store.Latest(); last != nil && !doc.EffectiveFrom.After(last.EffectiveFrom) {
		res.Valid = false
		res.Errors = []catalog.Problem{{Path: "effectiveFrom", Message: catalog.ErrNotMonotonic.Error()}}
	}
	writeJSON(w, http.StatusOK, res)
}

func nonNil(p []catalog.Problem) []catalog.Problem {
	if p == nil {
		return []catalog.Problem{}
	}
	return p
}

type dryRunRequest struct {
	Document catalog.Document `json:"document"`
	Cases    []catalog.Case   `json:"cases"`
}

type dryRunResponse struct {
	validateResult
	Results []catalog.CaseResult `json:"results"`
}

func (h *Handler) dryRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	var req dryRunRequest
	if !decode(w, r, &req) {
		return
	}
	req.Document.Version = h.store.LatestVersion() + 1
	sn, warns, err := catalog.Compile(req.Document)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, "invalid_catalog", "the draft catalog is not valid", problems(err))
		return
	}
	res, err := catalog.DryRun(h.store, sn, req.Cases, h.now())
	if err != nil {
		fail(w, http.StatusBadRequest, "too_many_cases", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, dryRunResponse{
		validateResult: validateResult{Valid: true, Errors: []catalog.Problem{}, Warnings: nonNil(warns)},
		Results:        res,
	})
}

type publishRequest struct {
	Document        catalog.Document `json:"document"`
	ExpectedVersion int64            `json:"expectedVersion,omitempty"`
}

type publishResponse struct {
	Version       int64             `json:"version"`
	EffectiveFrom time.Time         `json:"effectiveFrom"`
	Warnings      []catalog.Problem `json:"warnings"`
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	cn, ok := h.authn(w, r)
	if !ok {
		return
	}
	if !h.open && !h.publishers[cn] {
		fail(w, http.StatusForbidden, "forbidden", "this client may not publish catalog versions", nil)
		return
	}
	var req publishRequest
	if !decode(w, r, &req) {
		return
	}
	sn, warns, err := h.pub.Publish(r.Context(), req.Document, req.ExpectedVersion)
	var ve *catalog.ValidationError
	switch {
	case err == nil:
		h.logf("catalog version %d published by %q, effective %s", sn.Version, cn, sn.EffectiveFrom.Format(time.RFC3339))
		w.Header().Set("Location", "/v1/catalog?version="+strconv.FormatInt(sn.Version, 10))
		writeJSON(w, http.StatusCreated, publishResponse{sn.Version, sn.EffectiveFrom, nonNil(warns)})
	case errors.As(err, &ve):
		fail(w, http.StatusUnprocessableEntity, "invalid_catalog", "the catalog is not valid", ve.Problems)
	case errors.Is(err, catalog.ErrVersionConflict):
		fail(w, http.StatusConflict, "version_conflict", err.Error(), nil)
	case errors.Is(err, catalog.ErrNotMonotonic):
		fail(w, http.StatusUnprocessableEntity, "not_monotonic", err.Error(), nil)
	default:
		fail(w, http.StatusServiceUnavailable, "unavailable", "the catalog could not be published, try again", nil)
	}
}

// ---- editor metadata ----

type field struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

func (h *Handler) schema(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authn(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"variables": []field{
			{"rat", "string", "radio access technology, for example NR, LTE"},
			{"slice", "string", "network slice identifier"},
			{"cell", "string", "cell identifier"},
			{"unit", "string", `"bytes", "seconds" or "events"`},
			{"rg", "number", "rating group"},
			{"service", "number", "service identifier"},
			{"hour", "number", "0-23 in the plan's time zone"},
			{"minute", "number", "0-59 in the plan's time zone"},
			{"weekday", "number", "0 = Sunday ... 6 = Saturday, in the plan's time zone"},
		},
		"functions": []field{
			{"startsWith(s, prefix)", "bool", "true when s begins with prefix"},
			{"endsWith(s, suffix)", "bool", "true when s ends with suffix"},
			{"contains(s, part)", "bool", "true when s contains part"},
		},
		"operators":        []string{"||", "&&", "!", "==", "!=", "<", "<=", ">", ">=", "in [..]"},
		"example":          `rat in ["NR","LTE"] && (hour >= 22 || hour < 6) && !startsWith(cell, "262-01")`,
		"classes":          []string{"promo", "bundle", "cash"},
		"allowanceClasses": []string{"promo", "bundle"},
		"units":            []string{"bytes", "seconds", "events"},
		"filterActions":    []string{"deny"},
		"limits": map[string]any{
			"maxExpressionLength": expr.MaxLength,
			"maxPlans":            catalog.MaxPlans,
			"maxRulesPerPlan":     catalog.MaxRulesPerPlan,
			"maxFiltersPerPlan":   catalog.MaxFiltersInPlan,
			"maxOfferings":        catalog.MaxOfferings,
			"maxDryRunCases":      catalog.MaxDryRunCases,
			"maxIdLength":         catalog.MaxIDLength,
			"priceNumeratorMax":   1 << 20,
			"priceDenominatorMax": 1 << 12,
		},
	})
}
