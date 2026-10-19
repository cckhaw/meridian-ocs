package nchf

import (
	"strings"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
)

// Request model: only the TS 32.291 ChargingDataRequest fields the adapter
// reads. Everything else is ignored by encoding/json, as the SBA requires.
// Pointers distinguish "absent" from zero.

type nfIdentification struct {
	NodeFunctionality string `json:"nodeFunctionality"`
	NFName            string `json:"nFName"`
	NFIPv4Address     string `json:"nFIPv4Address"`
	NFIPv6Address     string `json:"nFIPv6Address"`
}

type requestedUnit struct {
	Time                 *uint32 `json:"time"`
	TotalVolume          *uint64 `json:"totalVolume"`
	UplinkVolume         *uint64 `json:"uplinkVolume"`
	DownlinkVolume       *uint64 `json:"downlinkVolume"`
	ServiceSpecificUnits *uint64 `json:"serviceSpecificUnits"`
}

type usedUnitContainer struct {
	ServiceID            *uint32 `json:"serviceId"`
	Time                 *uint32 `json:"time"`
	TotalVolume          *uint64 `json:"totalVolume"`
	UplinkVolume         *uint64 `json:"uplinkVolume"`
	DownlinkVolume       *uint64 `json:"downlinkVolume"`
	ServiceSpecificUnits *uint64 `json:"serviceSpecificUnits"`
}

type multipleUnitUsage struct {
	RatingGroup       *uint32             `json:"ratingGroup"`
	RequestedUnit     *requestedUnit      `json:"requestedUnit"`
	UsedUnitContainer []usedUnitContainer `json:"usedUnitContainer"`
}

type snssai struct {
	SST *uint32 `json:"sst"`
	SD  string  `json:"sd"`
}

type pduSessionInformation struct {
	PDUSessionID       *uint32 `json:"pduSessionID"`
	RatType            string  `json:"ratType"`
	NetworkSlicingInfo *struct {
		SNSSAI *snssai `json:"sNSSAI"`
	} `json:"networkSlicingInfo"`
}

type pduSessionChargingInformation struct {
	PDUSessionInformation *pduSessionInformation `json:"pduSessionInformation"`
}

type chargingDataRequest struct {
	SubscriberIdentifier     string                         `json:"subscriberIdentifier"`
	NFConsumer               *nfIdentification              `json:"nfConsumerIdentification"`
	InvocationTimeStamp      string                         `json:"invocationTimeStamp"`
	InvocationSequenceNumber *uint32                        `json:"invocationSequenceNumber"`
	OneTimeEvent             bool                           `json:"oneTimeEvent"`
	OneTimeEventType         string                         `json:"oneTimeEventType"`
	MultipleUnitUsage        []multipleUnitUsage            `json:"multipleUnitUsage"`
	PDUSessionCharging       *pduSessionChargingInformation `json:"pDUSessionChargingInformation"`
}

// reset zeroes the request for reuse. json.Unmarshal reuses slice capacity
// without clearing the elements, so stale fields would leak between requests.
func (r *chargingDataRequest) reset() {
	mu := r.MultipleUnitUsage
	clear(mu[:cap(mu)])
	*r = chargingDataRequest{MultipleUnitUsage: mu[:0]}
}

// unitValue is the parsed content of a requested or used unit group.
type unitValue struct {
	kind  canonical.UnitKind
	value int64
	any   bool
}

func clamp(v uint64) int64 {
	if v > datalayer.MaxUnits {
		return datalayer.MaxUnits + 1 // refused downstream as out of range
	}
	return int64(v)
}

// units reduces the optional volume/time/event fields to one kind and value.
// totalVolume wins over uplink+downlink. ok is false when kinds are mixed.
func units(time *uint32, total, up, down, sse *uint64) (u unitValue, ok bool) {
	kinds := 0
	if total != nil || up != nil || down != nil {
		kinds++
		u.kind, u.any = canonical.UnitBytes, true
		if total != nil {
			u.value = clamp(*total)
		} else {
			var sum uint64
			if up != nil {
				sum += *up
			}
			if down != nil {
				if sum+*down < sum {
					sum = ^uint64(0)
				} else {
					sum += *down
				}
			}
			u.value = clamp(sum)
		}
	}
	if time != nil {
		kinds++
		u.kind, u.any, u.value = canonical.UnitSeconds, true, int64(*time)
	}
	if sse != nil {
		kinds++
		u.kind, u.any, u.value = canonical.UnitEvents, true, clamp(*sse)
	}
	return u, kinds <= 1
}

// parseSUPI turns a TS 29.571 Supi into an identity. Only IMSI and NAI based
// SUPIs are accepted.
func parseSUPI(s string) (identity.Identity, bool) {
	switch {
	case strings.HasPrefix(s, "imsi-") && len(s) > 5:
		return identity.Identity{Type: identity.IMSI, Value: s[5:]}, true
	case strings.HasPrefix(s, "nai-") && len(s) > 4:
		return identity.Identity{Type: identity.NAI, Value: s[4:]}, true
	}
	return identity.Identity{}, false
}
