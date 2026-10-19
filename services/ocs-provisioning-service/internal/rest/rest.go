// Package rest is the HTTP/JSON API for CRM and care systems.
//
//	POST   /v1/subscribers                              create a subscriber
//	POST   /v1/subscribers/{key}/top-ups                credit money
//	POST   /v1/subscribers/{key}/adjustments            correct a balance      (privileged)
//	POST   /v1/subscribers/{key}/bundles                add an allowance
//	PUT    /v1/subscribers/{key}/buckets/{bucket}       reload an allowance
//	DELETE /v1/subscribers/{key}/buckets/{bucket}       remove a bucket        (privileged)
//	POST   /v1/subscribers/{key}/lifecycle              change lifecycle state (privileged)
//	GET    /v1/commands/{id}                            outcome of a command
//	GET    /v1/subscribers/{key}                        profile and buckets
//
// Every write needs an Idempotency-Key header and answers 202 Accepted: the
// command is queued and applied asynchronously, in order per subscriber; read
// the outcome from /v1/commands/{id}. Repeating a request with the same key and
// the same content returns the original command; the same key with different
// content is 409. Bodies are decoded strictly: an unknown field is an error.
package rest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/tlsconf"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/auth"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

// MaxBody is the largest accepted request body.
const MaxBody = 64 << 10

// Handler serves the REST API.
type Handler struct {
	svc *service.Service
	az  *auth.Authorizer
	mux *http.ServeMux
}

// New builds the handler.
func New(svc *service.Service, az *auth.Authorizer) *Handler {
	h := &Handler{svc: svc, az: az, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/subscribers", handle(h, command.CreateSubscriber, true, fillCreate))
	h.mux.HandleFunc("POST /v1/subscribers/{key}/top-ups", handle(h, command.TopUp, true, fillTopUp))
	h.mux.HandleFunc("POST /v1/subscribers/{key}/adjustments", handle(h, command.AdjustBalance, true, fillAdjust))
	h.mux.HandleFunc("POST /v1/subscribers/{key}/bundles", handle(h, command.AddBundle, true, fillBundle))
	h.mux.HandleFunc("PUT /v1/subscribers/{key}/buckets/{bucket}", handle(h, command.ReloadBucket, true, fillReload))
	h.mux.HandleFunc("DELETE /v1/subscribers/{key}/buckets/{bucket}", handle(h, command.RemoveBucket, false, fillRemove))
	h.mux.HandleFunc("POST /v1/subscribers/{key}/lifecycle", handle(h, command.ChangeLifecycle, true, fillLifecycle))
	h.mux.HandleFunc("GET /v1/commands/{id}", h.getCommand)
	h.mux.HandleFunc("GET /v1/subscribers/{key}", h.getSubscriber)
	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// ---- request bodies --------------------------------------------------------

type createBody struct {
	SubscriberKey string `json:"subscriberKey"`
	PaymentType   string `json:"paymentType"`
	Identifiers   []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"identifiers"`
}

type topUpBody struct {
	AmountMinorUnits int64  `json:"amountMinorUnits"`
	Currency         string `json:"currency"`
	BucketID         string `json:"bucketId"`
}

type adjustBody struct {
	BucketID      string `json:"bucketId"`
	Delta         int64  `json:"delta"`
	Currency      string `json:"currency"`
	Reason        string `json:"reason"`
	AllowNegative bool   `json:"allowNegative"`
}

type bundleBody struct {
	BucketID  string `json:"bucketId"`
	Class     string `json:"class"`
	Unit      string `json:"unit"`
	Amount    int64  `json:"amount"`
	ExpiresAt string `json:"expiresAt"`
	Priority  *int32 `json:"priority"`
}

type reloadBody struct {
	Amount     int64  `json:"amount"`
	ExpiresAt  string `json:"expiresAt"`
	KeepExpiry bool   `json:"keepExpiry"`
	Priority   *int32 `json:"priority"`
}

type removeBody struct{}

type lifecycleBody struct {
	Target     string `json:"target"`
	GraceUntil string `json:"graceUntil"`
	Reason     string `json:"reason"`
}

func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, errors.New("timestamps must be RFC 3339 date-times")
	}
	return t.UnixMilli(), nil
}

func bucketOrCash(id string) string {
	if id == "" {
		return command.DefaultCashBucket
	}
	return id
}

func fillCreate(_ *http.Request, b *createBody, c *command.Command) error {
	c.Key, c.Payment = b.SubscriberKey, b.PaymentType
	for _, id := range b.Identifiers {
		c.Identifiers = append(c.Identifiers, command.Identifier{Type: id.Type, Value: id.Value})
	}
	return nil
}

func fillTopUp(_ *http.Request, b *topUpBody, c *command.Command) error {
	c.Amount, c.Currency, c.BucketID = b.AmountMinorUnits, b.Currency, bucketOrCash(b.BucketID)
	return nil
}

func fillAdjust(_ *http.Request, b *adjustBody, c *command.Command) error {
	c.BucketID, c.Amount, c.Currency, c.Reason, c.AllowNegative = bucketOrCash(b.BucketID), b.Delta, b.Currency, b.Reason, b.AllowNegative
	return nil
}

func fillBundle(_ *http.Request, b *bundleBody, c *command.Command) (err error) {
	c.BucketID, c.Class, c.Unit, c.Amount, c.Priority = b.BucketID, b.Class, b.Unit, b.Amount, b.Priority
	c.ExpiresAt, err = parseTime(b.ExpiresAt)
	return err
}

func fillReload(r *http.Request, b *reloadBody, c *command.Command) (err error) {
	c.BucketID, c.Amount, c.KeepExpiry, c.Priority = r.PathValue("bucket"), b.Amount, b.KeepExpiry, b.Priority
	c.ExpiresAt, err = parseTime(b.ExpiresAt)
	return err
}

func fillRemove(r *http.Request, _ *removeBody, c *command.Command) error {
	c.BucketID, c.Force = r.PathValue("bucket"), r.URL.Query().Get("force") == "true"
	return nil
}

func fillLifecycle(_ *http.Request, b *lifecycleBody, c *command.Command) (err error) {
	c.Target, c.Reason = b.Target, b.Reason
	c.GraceUntil, err = parseTime(b.GraceUntil)
	return err
}

// ---- handlers --------------------------------------------------------------

func (h *Handler) principal(r *http.Request) string {
	return h.az.Principal(tlsconf.PeerCommonName(r.TLS))
}

// handle builds the handler of one write endpoint.
func handle[B any](h *Handler, typ command.Type, hasBody bool, fill func(*http.Request, *B, *command.Command) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal := h.principal(r)
		if err := h.az.Allow(principal, typ); err != nil {
			fail(w, err)
			return
		}
		id := r.Header.Get("Idempotency-Key")
		if id == "" {
			fail(w, &command.ValidationError{Msg: "the Idempotency-Key header is required"})
			return
		}
		var body B
		if hasBody {
			if err := decode(w, r, &body); err != nil {
				fail(w, err)
				return
			}
		}
		c := &command.Command{ID: id, Type: typ, Key: r.PathValue("key"), Principal: principal}
		if err := fill(r, &body, c); err != nil {
			fail(w, &command.ValidationError{Msg: err.Error()})
			return
		}
		acc, err := h.svc.Submit(r.Context(), c)
		if err != nil {
			fail(w, err)
			return
		}
		w.Header().Set("Location", "/v1/commands/"+acc.CommandID)
		writeJSON(w, http.StatusAccepted, map[string]any{"commandId": acc.CommandID, "state": acc.State, "duplicate": acc.Duplicate})
	}
}

// decode reads one strict JSON object.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if ct := r.Header.Get("Content-Type"); len(ct) < 16 || !strings.EqualFold(ct[:16], "application/json") {
		return errUnsupportedMedia
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return errTooLarge
		}
		return &command.ValidationError{Msg: "invalid JSON body: " + err.Error()}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return &command.ValidationError{Msg: "invalid JSON body: unexpected data after the object"}
	}
	return nil
}

var (
	errUnsupportedMedia = errors.New("rest: unsupported media type")
	errTooLarge         = errors.New("rest: body too large")
)

func (h *Handler) getCommand(w http.ResponseWriter, r *http.Request) {
	if err := h.az.Read(h.principal(r)); err != nil {
		fail(w, err)
		return
	}
	rec, err := h.svc.GetCommand(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	out := map[string]any{"commandId": rec.ID, "type": rec.Type, "subscriberKey": rec.Key, "state": rec.State,
		"acceptedAt": rec.AcceptedAt.UTC().Format(time.RFC3339Nano)}
	if rec.Final() {
		out["outcome"], out["version"] = rec.Outcome, rec.Version
		out["completedAt"] = rec.CompletedAt.UTC().Format(time.RFC3339Nano)
		if rec.Message != "" {
			out["message"] = rec.Message
		}
		if rec.State == status.Applied {
			switch rec.Type {
			case command.ChangeLifecycle:
				out["from"], out["to"] = rec.From, rec.To
			case command.CreateSubscriber:
			default:
				out["balance"] = rec.Balance
				if rec.Type == command.ReloadBucket {
					out["oldBalance"] = rec.OldBalance
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) getSubscriber(w http.ResponseWriter, r *http.Request) {
	if err := h.az.Read(h.principal(r)); err != nil {
		fail(w, err)
		return
	}
	v, err := h.svc.GetSubscriber(r.Context(), r.PathValue("key"))
	if err != nil {
		fail(w, err)
		return
	}
	ts := func(t time.Time) any {
		if t.IsZero() {
			return nil
		}
		return t.UTC().Format(time.RFC3339)
	}
	buckets := make([]map[string]any, 0, len(v.Buckets))
	for _, b := range v.Buckets {
		buckets = append(buckets, map[string]any{"id": b.ID, "class": b.Class, "kind": b.Kind, "balance": b.Balance,
			"expiresAt": ts(b.ExpiresAt), "priority": b.Order})
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriberKey": v.Key, "paymentType": v.Profile.Payment, "status": v.Profile.Status,
		"version": v.Profile.Version, "createdAt": ts(v.Profile.CreatedAt), "statusAt": ts(v.Profile.StatusAt),
		"graceUntil": ts(v.Profile.GraceUntil), "buckets": buckets})
}

// ---- responses -------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// fail writes {"error":{"code","message"}} with a status derived from err.
func fail(w http.ResponseWriter, err error) {
	status, code, msg := http.StatusInternalServerError, "internal", "internal error"
	switch {
	case command.IsValidation(err):
		status, code, msg = http.StatusBadRequest, "invalid_request", err.Error()
	case errors.Is(err, errUnsupportedMedia):
		status, code, msg = http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json"
	case errors.Is(err, errTooLarge):
		status, code, msg = http.StatusRequestEntityTooLarge, "body_too_large", "the request body is too large"
	case errors.Is(err, auth.ErrUnauthenticated):
		status, code, msg = http.StatusUnauthorized, "unauthenticated", "a client certificate is required"
	case errors.Is(err, auth.ErrForbidden):
		status, code, msg = http.StatusForbidden, "forbidden", "this client may not perform this operation"
	case errors.Is(err, service.ErrNotFound):
		status, code, msg = http.StatusNotFound, "not_found", "not found"
	case errors.Is(err, service.ErrIdempotencyConflict):
		status, code, msg = http.StatusConflict, "idempotency_conflict", "this Idempotency-Key was already used for a different request"
	case errors.Is(err, service.ErrUnavailable):
		w.Header().Set("Retry-After", "1")
		status, code, msg = http.StatusServiceUnavailable, "unavailable", "temporarily unavailable, retry with the same Idempotency-Key"
	}
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}
