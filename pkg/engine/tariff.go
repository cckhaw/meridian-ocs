package engine

import (
	"errors"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
)

// Tariff turns a unit request into per-bucket-class prices. The catalog
// service will provide the production implementation; it must not allocate
// beyond appending to dst.
type Tariff interface {
	Rates(req *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, error)
}

// ErrNoTariff is returned (wrapped) by a Tariff when a unit cannot be priced.
// The engine then sends the unit with no rates instead of failing the request.
var ErrNoTariff = errors.New("engine: no tariff for unit")

// Frac is a rate expressed as Num/Den cost units per unit.
type Frac struct{ Num, Den int64 }

// StaticTariff prices promo and bundle buckets 1:1 and cash per unit kind.
// A zero Frac means cash cannot pay for that kind.
type StaticTariff struct {
	Cash map[canonical.UnitKind]Frac
}

// Rates implements Tariff.
func (t StaticTariff) Rates(_ *canonical.Request, u *canonical.UnitRequest, dst []datalayer.Rate) ([]datalayer.Rate, error) {
	dst = append(dst,
		datalayer.Rate{Class: datalayer.ClassPromo, Num: 1, Den: 1},
		datalayer.Rate{Class: datalayer.ClassBundle, Num: 1, Den: 1},
	)
	if f, ok := t.Cash[u.Kind]; ok && f.Num > 0 && f.Den > 0 {
		dst = append(dst, datalayer.Rate{Class: datalayer.ClassCash, Num: f.Num, Den: f.Den})
	}
	return dst, nil
}
