package nchf

import (
	"math"
	"strconv"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
)

// unitResultCode maps a per-unit outcome to the TS 32.291 ResultCode enum.
func unitResultCode(r canonical.Result) string {
	switch r {
	case canonical.ResultGranted, canonical.ResultPartialFinal:
		return "SUCCESS"
	case canonical.ResultCreditLimitReached:
		return "QUOTA_LIMIT_REACHED"
	case canonical.ResultNoBucket:
		return "RATING_FAILED"
	case canonical.ResultUnavailable, canonical.ResultBarred:
		return "END_USER_SERVICE_DENIED"
	}
	return "END_USER_SERVICE_REJECTED"
}

// encodeResponse appends a ChargingDataResponse. Quota outcomes are carried
// per rating group in multipleUnitInformation.resultCode; the HTTP status
// only describes the request itself.
func encodeResponse(b []byte, now time.Time, seq uint32, req *canonical.Request, resp *canonical.Response) []byte {
	b = append(b, `{"invocationTimeStamp":"`...)
	b = now.UTC().AppendFormat(b, "2006-01-02T15:04:05.000Z")
	b = append(b, `","invocationSequenceNumber":`...)
	b = strconv.AppendUint(b, uint64(seq), 10)
	b = append(b, `,"multipleUnitInformation":[`...)
	first := true
	for i := range resp.Grants {
		if i >= len(req.Units) {
			break
		}
		g, u := &resp.Grants[i], &req.Units[i]
		if !first {
			b = append(b, ',')
		}
		first = false
		b = append(b, `{"ratingGroup":`...)
		b = strconv.AppendUint(b, uint64(g.RatingGroup), 10)
		b = append(b, `,"resultCode":"`...)
		b = append(b, unitResultCode(g.Result)...)
		b = append(b, '"')
		if g.Granted > 0 {
			b = append(b, `,"grantedUnit":{`...)
			switch u.Kind {
			case canonical.UnitSeconds:
				b = append(b, `"time":`...)
				b = strconv.AppendUint(b, uint64(min(g.Granted, math.MaxUint32)), 10)
			case canonical.UnitEvents:
				b = append(b, `"serviceSpecificUnits":`...)
				b = strconv.AppendInt(b, g.Granted, 10)
			default:
				b = append(b, `"totalVolume":`...)
				b = strconv.AppendInt(b, g.Granted, 10)
			}
			b = append(b, '}')
			if g.ValiditySeconds > 0 {
				b = append(b, `,"validityTime":`...)
				b = strconv.AppendUint(b, uint64(g.ValiditySeconds), 10)
			}
		}
		if g.FinalUnit {
			b = append(b, `,"finalUnitIndication":{"finalUnitAction":"TERMINATE"}`...)
		}
		b = append(b, '}')
	}
	return append(b, ']', '}')
}
