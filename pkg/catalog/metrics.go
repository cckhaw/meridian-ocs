package catalog

import (
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/telemetry"
)

// SyncAge is how long ago the replicator last read the shared catalog
// successfully, or -1 if it never has. A node whose age keeps growing is
// pricing from a catalog that may no longer be the published one.
func (r *Replicator) SyncAge() time.Duration {
	at := r.lastOK.Load()
	if at == 0 {
		return -1
	}
	return time.Since(time.Unix(0, at))
}

// RegisterMetrics exposes the replicator's health and the catalog versions this
// node holds. Everything is read at scrape time.
func (r *Replicator) RegisterMetrics(reg *telemetry.Registry) {
	reg.GaugeFunc("ocs_catalog_latest_version", "Newest catalog version this node holds (may be future-dated).", nil,
		func() float64 { return float64(r.Store.LatestVersion()) })
	reg.GaugeFunc("ocs_catalog_version_in_effect", "Catalog version in effect now on this node, 0 if none.", nil,
		func() float64 {
			if sn := r.Store.Current(); sn != nil {
				return float64(sn.Version)
			}
			return 0
		})
	reg.GaugeFunc("ocs_catalog_sync_age_seconds", "Seconds since the shared catalog was last read successfully, -1 if never.", nil,
		func() float64 {
			if a := r.SyncAge(); a >= 0 {
				return a.Seconds()
			}
			return -1
		})
	reg.CounterFunc("ocs_catalog_sync_errors_total", "Background catalog replication errors (resync, subscribe, unreadable version).", nil,
		func() float64 { return float64(r.errs.Load()) })
}
