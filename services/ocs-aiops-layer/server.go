package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/cckhaw/meridian-ocs/pkg/mapping"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
)

// maxBody bounds a request: a mapping document plus its samples.
const maxBody = 4 << 20

// maxSamples bounds one test call.
const maxSamples = 100

// Handler serves the mapping workbench: validate a draft mapping and try it on
// sample records before it is put into a ConfigMap. It is stateless and changes
// nothing; deploying a mapping is a deliberate act in the deployment pipeline.
//
//	GET  /v1/schema/mapping     JSON Schema of a mapping document
//	GET  /v1/examples/mapping   a complete example mapping (YAML)
//	POST /v1/mappings/validate  body: the mapping (YAML or JSON)         -> {valid, problems}
//	POST /v1/mappings/test      body: {"mapping": "<yaml/json text>", "samples": [<record>...]}
//	                                                                    -> per sample: rule, output or error
type Handler struct {
	mux  *http.ServeMux
	open bool
}

// New builds the handler. With open (auth off) the TLS identity is not checked.
func New(open bool) *Handler {
	h := &Handler{mux: http.NewServeMux(), open: open}
	h.mux.HandleFunc("GET /v1/schema/mapping", h.guard(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/schema+json")
		_, _ = w.Write(mapping.SchemaJSON)
	}))
	h.mux.HandleFunc("GET /v1/examples/mapping", h.guard(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(mapping.ExampleAcmePGW)
	}))
	h.mux.HandleFunc("POST /v1/mappings/validate", h.guard(h.validate))
	h.mux.HandleFunc("POST /v1/mappings/test", h.guard(h.test))
	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.open && tlsconf.PeerCommonName(r.TLS) == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"code": "unauthenticated", "message": "a verified client certificate is required"}})
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": map[string]string{"code": "too_large", "message": "request body too large"}})
		} else {
			badRequest(w, "unreadable", err.Error())
		}
		return nil, false
	}
	return b, true
}

type problemList struct {
	Valid    bool     `json:"valid"`
	Name     string   `json:"name,omitempty"`
	Problems []string `json:"problems"`
}

// check parses and compiles a draft, returning the mapper or the problems.
func check(doc []byte) (*mapping.Mapper, problemList) {
	spec, err := mapping.Parse(doc)
	if err != nil {
		return nil, problemList{Problems: []string{err.Error()}}
	}
	m, err := mapping.Compile(spec)
	var ve *mapping.ValidationError
	switch {
	case errors.As(err, &ve):
		return nil, problemList{Problems: ve.Problems}
	case err != nil:
		return nil, problemList{Problems: []string{err.Error()}}
	}
	return m, problemList{Valid: true, Name: m.Name(), Problems: []string{}}
}

func (h *Handler) validate(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	_, res := check(body)
	writeJSON(w, http.StatusOK, res)
}

type testRequest struct {
	Mapping string            `json:"mapping"`
	Samples []json.RawMessage `json:"samples"`
}

type sampleResult struct {
	Index  int             `json:"index"`
	RuleID string          `json:"ruleId,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	// Outcome: "translated", "no_match" (the record would be passed through
	// unchanged by the mediation pipeline) or "error".
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}

func (h *Handler) test(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var req testRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		badRequest(w, "invalid_json", "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Samples) == 0 || len(req.Samples) > maxSamples {
		badRequest(w, "invalid_samples", "between 1 and 100 samples are required")
		return
	}
	m, res := check([]byte(req.Mapping))
	if m == nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"mapping": res})
		return
	}
	out := make([]sampleResult, len(req.Samples))
	for i, s := range req.Samples {
		sr := sampleResult{Index: i}
		b, rule, err := m.Apply(s)
		switch {
		case err == nil:
			sr.Outcome, sr.RuleID, sr.Output = "translated", rule, b
		case errors.Is(err, mapping.ErrNoMatch):
			sr.Outcome = "no_match"
		default:
			sr.Outcome, sr.RuleID, sr.Error = "error", rule, err.Error()
		}
		out[i] = sr
	}
	writeJSON(w, http.StatusOK, map[string]any{"mapping": res, "results": out})
}
