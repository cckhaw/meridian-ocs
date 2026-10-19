package gy

import (
	"math"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
)

// finalUnitActionTerminate is Final-Unit-Action TERMINATE (RFC 4006 §8.35).
const finalUnitActionTerminate = 0

// resultCode maps the canonical outcome to the command-level Result-Code.
func resultCode(r canonical.Result) uint32 {
	switch r {
	case canonical.ResultGranted, canonical.ResultPartialFinal:
		return diameter.ResultSuccess
	case canonical.ResultCreditLimitReached:
		return diameter.ResultCreditLimitReached
	case canonical.ResultNoBucket:
		return diameter.ResultRatingFailed
	case canonical.ResultUnknownSession:
		return diameter.ResultUnknownSessionID
	case canonical.ResultStaleRequest:
		return diameter.ResultUnableToComply
	case canonical.ResultUnavailable:
		return diameter.ResultEndUserDenied
	case canonical.ResultTooBusy:
		return diameter.ResultTooBusy
	}
	return diameter.ResultUnableToComply
}

func unitResultCode(r canonical.Result) uint32 {
	switch r {
	case canonical.ResultCreditLimitReached:
		return diameter.ResultCreditLimitReached
	case canonical.ResultNoBucket:
		return diameter.ResultRatingFailed
	}
	return diameter.ResultUnableToComply
}

func isProtocolError(result uint32) bool { return result >= 3000 && result < 4000 }

// beginAnswer writes the header and the AVPs common to every answer.
func (s *Server) beginAnswer(b *diameter.Builder, h diameter.Header, cmd uint32, result uint32) {
	flags := h.Flags & diameter.FlagProxiable
	if isProtocolError(result) {
		flags |= diameter.FlagError
	}
	b.StartMessage(flags, cmd, h.AppID, h.HopByHop, h.EndToEnd)
}

func (s *Server) originAVPs(b *diameter.Builder) {
	b.String(diameter.AVPOriginHost, diameter.AVPFlagMandatory, s.cfg.OriginHost)
	b.String(diameter.AVPOriginRealm, diameter.AVPFlagMandatory, s.cfg.OriginRealm)
}

// unitAVP appends the kind-specific unit AVP inside a Granted-Service-Unit.
func unitAVP(b *diameter.Builder, kind canonical.UnitKind, n int64) {
	switch kind {
	case canonical.UnitSeconds:
		if n > math.MaxUint32 {
			n = math.MaxUint32
		}
		b.Uint32(avpCCTime, diameter.AVPFlagMandatory, uint32(n))
	case canonical.UnitEvents:
		b.Uint64(avpCCSpecificUnits, diameter.AVPFlagMandatory, uint64(n))
	default:
		b.Uint64(avpCCTotalOctets, diameter.AVPFlagMandatory, uint64(n))
	}
}

// encodeCCA builds a Credit-Control-Answer. resp may be nil for answers that
// carry no per-unit data (errors, load shedding).
func (s *Server) encodeCCA(b *diameter.Builder, info *ccrInfo, req *canonical.Request, resp *canonical.Response, result uint32, errMsg string) ([]byte, error) {
	s.beginAnswer(b, info.h, diameter.CmdCCR, result)
	if len(info.sessionID) > 0 {
		b.Bytes(diameter.AVPSessionID, diameter.AVPFlagMandatory, info.sessionID)
	}
	b.Uint32(diameter.AVPResultCode, diameter.AVPFlagMandatory, result)
	s.originAVPs(b)
	b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, diameter.AppCreditControl)
	if info.haveType {
		b.Uint32(avpCCType, diameter.AVPFlagMandatory, info.reqType)
	}
	if info.haveNo {
		b.Uint32(avpCCNumber, diameter.AVPFlagMandatory, info.reqNo)
	}
	if errMsg != "" {
		b.String(diameter.AVPErrorMessage, 0, errMsg)
	}

	if resp != nil && result == diameter.ResultSuccess && info.reqType != ccTerminate {
		for i := range resp.Grants {
			if i >= len(req.Units) {
				break
			}
			g, u := &resp.Grants[i], &req.Units[i]
			m := b.BeginGrouped(avpMSCC, diameter.AVPFlagMandatory)
			if g.Granted > 0 {
				gs := b.BeginGrouped(avpGrantedServiceUnit, diameter.AVPFlagMandatory)
				unitAVP(b, u.Kind, g.Granted)
				b.EndGrouped(gs)
			}
			if u.ServiceID != 0 {
				b.Uint32(avpServiceIdentifier, diameter.AVPFlagMandatory, u.ServiceID)
			}
			if u.RatingGroup != 0 {
				b.Uint32(avpRatingGroup, diameter.AVPFlagMandatory, u.RatingGroup)
			}
			if g.Granted > 0 && g.ValiditySeconds > 0 {
				b.Uint32(avpValidityTime, diameter.AVPFlagMandatory, g.ValiditySeconds)
			}
			if g.FinalUnit {
				f := b.BeginGrouped(avpFinalUnitIndication, diameter.AVPFlagMandatory)
				b.Uint32(avpFinalUnitAction, diameter.AVPFlagMandatory, finalUnitActionTerminate)
				b.EndGrouped(f)
			}
			if g.Granted == 0 && g.Result != canonical.ResultGranted {
				b.Uint32(diameter.AVPResultCode, diameter.AVPFlagMandatory, unitResultCode(g.Result))
			}
			b.EndGrouped(m)
		}
	}
	return b.Finish()
}
