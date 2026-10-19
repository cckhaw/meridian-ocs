// Package cdr is the normalized charging data record the mediation pipeline
// works with, and the decoders that produce it from JSON and from ASN.1 BER.
package cdr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Container is usage for one rating group inside a record.
type Container struct {
	RatingGroup uint32 `json:"ratingGroup"`
	Uplink      int64  `json:"uplink,omitempty"`
	Downlink    int64  `json:"downlink,omitempty"`
	// Charge is filled by rating: minor currency units for this container.
	Charge    *int64 `json:"charge,omitempty"`
	RatedWith string `json:"ratedWith,omitempty"` // rule id that priced it, or the reason it did not
}

// Record is one normalized CDR.
type Record struct {
	Type       string      `json:"type"`                 // "PGW", "SMF", ... (record type)
	NodeID     string      `json:"nodeId"`               // the network function that produced it
	ChargingID uint64      `json:"chargingId"`           // PDP/PDU session charging identifier
	Sequence   int64       `json:"sequence"`             // record sequence number within the session
	IMSI       string      `json:"imsi,omitempty"`       // digits
	MSISDN     string      `json:"msisdn,omitempty"`     // digits, no '+'
	APN        string      `json:"apn,omitempty"`        //
	RATType    string      `json:"ratType,omitempty"`    // as the network reports it
	OpenTime   time.Time   `json:"openTime"`             // when the record was opened (UTC)
	Duration   int64       `json:"durationSeconds"`      //
	Cause      string      `json:"cause,omitempty"`      // cause for record closing
	Containers []Container `json:"containers,omitempty"` //

	// Set by the pipeline, not by the decoders.
	Source        string `json:"source,omitempty"`        // where the record came from (file#n or topic/partition@offset)
	SubscriberKey string `json:"subscriberKey,omitempty"` // internal subscriber key (enrichment)
	Charge        *int64 `json:"charge,omitempty"`        // total, minor units (rating)
	PlanID        string `json:"planId,omitempty"`
	CatalogVer    int64  `json:"catalogVersion,omitempty"`
}

// Totals returns the record's uplink and downlink bytes summed over its
// containers.
func (r *Record) Totals() (up, down int64) {
	for _, c := range r.Containers {
		up += c.Uplink
		down += c.Downlink
	}
	return
}

// ErrInvalid marks a record that cannot be used.
var ErrInvalid = errors.New("cdr: invalid record")

// MaxContainers bounds a record so a corrupt one cannot blow up memory.
const MaxContainers = 1024

// Validate checks the fields every later stage relies on.
func (r *Record) Validate() error {
	switch {
	case r.Type == "":
		return fmt.Errorf("%w: no record type", ErrInvalid)
	case r.NodeID == "":
		return fmt.Errorf("%w: no node id", ErrInvalid)
	case r.OpenTime.IsZero():
		return fmt.Errorf("%w: no opening time", ErrInvalid)
	case r.IMSI == "" && r.MSISDN == "":
		return fmt.Errorf("%w: neither IMSI nor MSISDN", ErrInvalid)
	case r.Duration < 0:
		return fmt.Errorf("%w: negative duration", ErrInvalid)
	case len(r.Containers) > MaxContainers:
		return fmt.Errorf("%w: more than %d containers", ErrInvalid, MaxContainers)
	}
	if !digits(r.IMSI) || !digits(r.MSISDN) {
		return fmt.Errorf("%w: identifiers must be digits", ErrInvalid)
	}
	for _, c := range r.Containers {
		if c.Uplink < 0 || c.Downlink < 0 {
			return fmt.Errorf("%w: negative volume", ErrInvalid)
		}
	}
	return nil
}

func digits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) <= 20
}

// Key is the record's identity for duplicate detection: the same session
// record re-sent by the network (a retransmitted file, a re-delivered message,
// a failover to a second collector) has the same key. It deliberately leaves
// out volumes and the source, which can differ between copies.
func (r *Record) Key() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%d|%d|%d|%s|%s", r.Type, r.NodeID, r.ChargingID, r.Sequence,
		r.OpenTime.UnixMilli(), r.IMSI, r.MSISDN)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// MaxJSONSize bounds one JSON record.
const MaxJSONSize = 1 << 20

// FromJSON decodes one normalized record strictly (unknown fields are an error,
// so a producer's typo does not silently drop a field) and validates it.
func FromJSON(b []byte) (Record, error) {
	var r Record
	if len(b) > MaxJSONSize {
		return r, fmt.Errorf("%w: larger than %d bytes", ErrInvalid, MaxJSONSize)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return r, fmt.Errorf("%w: data after the record", ErrInvalid)
	}
	r.OpenTime = r.OpenTime.UTC()
	return r, r.Validate()
}
