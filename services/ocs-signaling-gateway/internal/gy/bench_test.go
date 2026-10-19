package gy

import (
	"testing"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
)

// Decode a CCR-Update with two MSCC, fake a grant, encode the CCA: everything
// the adapter does per request except socket I/O and the rating call.
func BenchmarkDecodeEncode(b *testing.B) {
	msg := buildCCR(ccrOpts{session: "pgw.example.net;1700000000;42", typ: ccUpdate, no: 5, msisdn: "491701234567", imsi: "262011234567890",
		ratSet: true, rat: 6,
		mscc: []msccOpts{
			{rg: 100, hasRSU: true, rsu: []unitFn{totalOctets(1 << 20)}, usus: [][]unitFn{{totalOctets(500_000)}}},
			{rg: 200, hasRSU: true, rsu: []unitFn{ccTime(60)}, usus: [][]unitFn{{ccTime(10)}}},
		}})
	h, _ := diameter.ParseHeader(msg)
	s, _ := NewServer(*testCfg(), raterFunc(nil), nil)
	var bld diameter.Builder
	var info ccrInfo
	b.ReportAllocs()
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := canonical.AcquireRequest()
		resp := canonical.AcquireResponse()
		if err := decodeCCR(msg, h, &s.cfg, req, &info); err != nil {
			b.Fatal(err)
		}
		resp.Result = canonical.ResultGranted
		for _, u := range req.Units {
			resp.Grants = append(resp.Grants, canonical.UnitGrant{RatingGroup: u.RatingGroup, Result: canonical.ResultGranted, Granted: u.Requested, ValiditySeconds: 30})
		}
		if _, err := s.encodeCCA(&bld, &info, req, resp, diameter.ResultSuccess, ""); err != nil {
			b.Fatal(err)
		}
		canonical.ReleaseRequest(req)
		canonical.ReleaseResponse(resp)
	}
}
