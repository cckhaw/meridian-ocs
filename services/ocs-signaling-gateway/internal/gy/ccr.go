package gy

import (
	"strconv"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
)

// Credit-Control AVP codes (RFC 4006) and the 3GPP ones used by Gy.
const (
	avpCCInputOctets        = 412
	avpCCNumber             = 415
	avpCCType               = 416
	avpCCSpecificUnits      = 417
	avpCCTime               = 420
	avpCCTotalOctets        = 421
	avpCCOutputOctets       = 414
	avpFinalUnitIndication  = 430
	avpRequestedAction      = 436
	avpGrantedServiceUnit   = 431
	avpRatingGroup          = 432
	avpRequestedServiceUnit = 437
	avpServiceIdentifier    = 439
	avpSubscriptionID       = 443
	avpSubscriptionIDData   = 444
	avpUsedServiceUnit      = 446
	avpValidityTime         = 448
	avpFinalUnitAction      = 449
	avpSubscriptionIDType   = 450
	avpMSCC                 = 456
	avpServiceInformation   = 873 // 3GPP
	avpPSInformation        = 874 // 3GPP
	avp3GPPRATType          = 21  // 3GPP
)

// CC-Request-Type values.
const (
	ccInitial   = 1
	ccUpdate    = 2
	ccTerminate = 3
	ccEvent     = 4
)

// Requested-Action values (RFC 4006 §8.41).
const actionDirectDebiting = 0

// Subscription-Id-Type values.
const (
	subE164 = 0
	subIMSI = 1
)

const ntpToUnix = 2208988800

// DecodeError is a protocol-level rejection with the Diameter Result-Code to
// answer with. Instances are shared values: never mutate them.
type DecodeError struct {
	Result uint32
	Msg    string
}

func (e *DecodeError) Error() string { return e.Msg }

var (
	errMissingSession = &DecodeError{diameter.ResultMissingAVP, "Session-Id missing"}
	errMissingType    = &DecodeError{diameter.ResultMissingAVP, "CC-Request-Type missing"}
	errMissingNumber  = &DecodeError{diameter.ResultMissingAVP, "CC-Request-Number missing"}
	errMissingSub     = &DecodeError{diameter.ResultMissingAVP, "Subscription-Id missing"}
	errMissingMSCC    = &DecodeError{diameter.ResultMissingAVP, "Multiple-Services-Credit-Control missing"}
	errBadType        = &DecodeError{diameter.ResultInvalidAVPValue, "invalid CC-Request-Type"}
	errBadLength      = &DecodeError{diameter.ResultInvalidAVPLength, "invalid AVP length"}
	errBadUnits       = &DecodeError{diameter.ResultInvalidAVPValue, "invalid or mixed service units"}
	errTooManyMSCC    = &DecodeError{diameter.ResultInvalidAVPValue, "too many MSCC"}
	errBadApp         = &DecodeError{diameter.ResultApplicationUnsup, "not the Credit-Control application"}
	errMissingAction  = &DecodeError{diameter.ResultMissingAVP, "Requested-Action missing"}
	errBadAction      = &DecodeError{diameter.ResultUnableToComply, "only DIRECT_DEBITING events are supported"}
)

// ccrInfo is what the answer needs from the request. Byte slices alias the
// request buffer, which stays valid until the answer is written.
type ccrInfo struct {
	h         diameter.Header
	sessionID []byte
	reqType   uint32
	reqNo     uint32
	haveType  bool
	haveNo    bool
	action    uint32
	haveAct   bool
}

var ratNames [256]string

func init() {
	for i := range ratNames {
		ratNames[i] = strconv.Itoa(i)
	}
}

// unitValue is the parsed content of a Granted/Requested/Used-Service-Unit.
type unitValue struct {
	kind  canonical.UnitKind
	value int64
	any   bool // at least one unit AVP was present
}

func clampUnits(v uint64) int64 {
	if v > datalayer.MaxUnits {
		return datalayer.MaxUnits + 1 // rejected downstream as out of range
	}
	return int64(v)
}

// parseUnits reads a *-Service-Unit grouped AVP. CC-Total-Octets wins over
// Input+Output when both are present.
func parseUnits(data []byte) (unitValue, *DecodeError) {
	var (
		total, in, out       uint64
		haveTotal, haveIO    bool
		secs, events         uint64
		haveSecs, haveEvents bool
	)
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			return unitValue{}, errBadLength
		}
		data = rest
		if a.Flags&diameter.AVPFlagVendor != 0 {
			continue
		}
		switch a.Code {
		case avpCCTotalOctets:
			v, err := a.Uint64()
			if err != nil {
				return unitValue{}, errBadLength
			}
			total, haveTotal = v, true
		case avpCCInputOctets:
			v, err := a.Uint64()
			if err != nil {
				return unitValue{}, errBadLength
			}
			in, haveIO = v, true
		case avpCCOutputOctets:
			v, err := a.Uint64()
			if err != nil {
				return unitValue{}, errBadLength
			}
			out, haveIO = v, true
		case avpCCTime:
			v, err := a.Uint32()
			if err != nil {
				return unitValue{}, errBadLength
			}
			secs, haveSecs = uint64(v), true
		case avpCCSpecificUnits:
			v, err := a.Uint64()
			if err != nil {
				return unitValue{}, errBadLength
			}
			events, haveEvents = v, true
		}
	}
	kinds := 0
	var u unitValue
	if haveTotal || haveIO {
		kinds++
		u.kind, u.any = canonical.UnitBytes, true
		if haveTotal {
			u.value = clampUnits(total)
		} else {
			sum := in + out
			if sum < in { // overflow
				sum = ^uint64(0)
			}
			u.value = clampUnits(sum)
		}
	}
	if haveSecs {
		kinds++
		u.kind, u.any, u.value = canonical.UnitSeconds, true, clampUnits(secs)
	}
	if haveEvents {
		kinds++
		u.kind, u.any, u.value = canonical.UnitEvents, true, clampUnits(events)
	}
	if kinds > 1 {
		return unitValue{}, errBadUnits
	}
	return u, nil
}

func (cfg *Config) defaultRequested(k canonical.UnitKind) int64 {
	switch k {
	case canonical.UnitSeconds:
		return cfg.DefaultRequestedSeconds
	case canonical.UnitEvents:
		return cfg.DefaultRequestedEvents
	}
	return cfg.DefaultRequestedBytes
}

func parseMSCC(data []byte, cfg *Config, u *canonical.UnitRequest) *DecodeError {
	var rsu, usu unitValue
	var haveRSU bool
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			return errBadLength
		}
		data = rest
		if a.Flags&diameter.AVPFlagVendor != 0 {
			continue
		}
		switch a.Code {
		case avpRatingGroup:
			v, err := a.Uint32()
			if err != nil {
				return errBadLength
			}
			u.RatingGroup = v
		case avpServiceIdentifier:
			v, err := a.Uint32()
			if err != nil {
				return errBadLength
			}
			u.ServiceID = v
		case avpRequestedServiceUnit:
			v, derr := parseUnits(a.Data)
			if derr != nil {
				return derr
			}
			rsu, haveRSU = v, true
		case avpUsedServiceUnit:
			// A session may report several USU (one per tariff change time).
			v, derr := parseUnits(a.Data)
			if derr != nil {
				return derr
			}
			if v.any {
				if usu.any && usu.kind != v.kind {
					return errBadUnits
				}
				sum := usu.value + v.value
				if sum > datalayer.MaxUnits {
					sum = datalayer.MaxUnits + 1
				}
				usu = unitValue{kind: v.kind, value: sum, any: true}
			}
		}
	}
	switch {
	case rsu.any && usu.any && rsu.kind != usu.kind:
		return errBadUnits
	case rsu.any:
		u.Kind = rsu.kind
	case usu.any:
		u.Kind = usu.kind
	default:
		u.Kind = canonical.UnitBytes
	}
	u.Used = usu.value
	switch {
	case rsu.any:
		u.Requested = rsu.value
	case haveRSU: // empty Requested-Service-Unit: the server picks the amount
		u.Requested = cfg.defaultRequested(u.Kind)
	}
	return nil
}

// decodeCCR fills req and info from a Credit-Control-Request. On error the
// partially filled info is still usable for building the error answer.
func decodeCCR(msg []byte, h diameter.Header, cfg *Config, req *canonical.Request, info *ccrInfo) *DecodeError {
	*info = ccrInfo{h: h}
	if h.AppID != diameter.AppCreditControl {
		return errBadApp
	}
	var e164, imsi, other []byte
	data := msg[diameter.HeaderLen:]
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			return errBadLength
		}
		data = rest
		if a.Flags&diameter.AVPFlagVendor != 0 {
			if a.Vendor == diameter.Vendor3GPP && a.Code == avpServiceInformation {
				decodeServiceInformation(a.Data, req)
			}
			continue
		}
		switch a.Code {
		case diameter.AVPSessionID:
			info.sessionID = a.Data
		case avpCCType:
			v, err := a.Uint32()
			if err != nil {
				return errBadLength
			}
			info.reqType, info.haveType = v, true
		case avpCCNumber:
			v, err := a.Uint32()
			if err != nil {
				return errBadLength
			}
			info.reqNo, info.haveNo = v, true
		case avpRequestedAction:
			v, err := a.Uint32()
			if err != nil {
				return errBadLength
			}
			info.action, info.haveAct = v, true
		case diameter.AVPEventTimestamp:
			if v, err := a.Uint32(); err == nil && v > ntpToUnix {
				req.EventTime = time.Unix(int64(v)-ntpToUnix, 0)
			}
		case avpSubscriptionID:
			var typ uint32 = 99
			var val []byte
			g := a.Data
			for len(g) > 0 {
				ia, irest, err := diameter.NextAVP(g)
				if err != nil {
					return errBadLength
				}
				g = irest
				switch ia.Code {
				case avpSubscriptionIDType:
					if v, err := ia.Uint32(); err == nil {
						typ = v
					}
				case avpSubscriptionIDData:
					val = ia.Data
				}
			}
			switch typ {
			case subE164:
				e164 = val
			case subIMSI:
				imsi = val
			default:
				if other == nil {
					other = val
				}
			}
		case avpMSCC:
			if len(req.Units) >= cfg.MaxMSCC {
				return errTooManyMSCC
			}
			req.Units = append(req.Units, canonical.UnitRequest{})
			if derr := parseMSCC(a.Data, cfg, &req.Units[len(req.Units)-1]); derr != nil {
				return derr
			}
		}
	}

	switch {
	case len(info.sessionID) == 0:
		return errMissingSession
	case !info.haveType:
		return errMissingType
	case !info.haveNo:
		return errMissingNumber
	}
	switch info.reqType {
	case ccInitial:
		req.Type = canonical.TypeInitial
	case ccUpdate:
		req.Type = canonical.TypeUpdate
	case ccTerminate:
		req.Type = canonical.TypeTerminate
	case ccEvent:
		req.Type = canonical.TypeEvent
	default:
		return errBadType
	}
	switch {
	case len(e164) > 0:
		req.SubscriberID = string(e164)
	case len(imsi) > 0:
		req.SubscriberID = string(imsi)
	case len(other) > 0:
		req.SubscriberID = string(other)
	default:
		return errMissingSub
	}
	if req.Type == canonical.TypeEvent {
		// Refunds, balance checks and price enquiries must never be rated as
		// a debit, so anything but DIRECT_DEBITING is refused.
		switch {
		case !info.haveAct:
			return errMissingAction
		case info.action != actionDirectDebiting:
			return errBadAction
		}
	}
	if req.Type != canonical.TypeTerminate && len(req.Units) == 0 {
		return errMissingMSCC
	}
	req.Source = canonical.SourceDiameterGy
	req.SessionID = string(info.sessionID)
	req.RequestNumber = info.reqNo
	if req.EventTime.IsZero() {
		req.EventTime = time.Now()
	}
	return nil
}

// decodeServiceInformation extracts the RAT type from
// Service-Information > PS-Information.
func decodeServiceInformation(data []byte, req *canonical.Request) {
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			return
		}
		data = rest
		if a.Vendor != diameter.Vendor3GPP || a.Code != avpPSInformation {
			continue
		}
		g := a.Data
		for len(g) > 0 {
			ia, irest, err := diameter.NextAVP(g)
			if err != nil {
				return
			}
			g = irest
			if ia.Vendor == diameter.Vendor3GPP && ia.Code == avp3GPPRATType && len(ia.Data) == 1 {
				req.RATType = ratNames[ia.Data[0]]
			}
		}
	}
}
