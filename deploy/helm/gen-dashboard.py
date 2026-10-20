#!/usr/bin/env python3
"""Regenerate meridian-ocs/files/dashboard.json (Python standard library only).

Every expression uses only metrics the services export; test.sh checks that. Edit
the PANELS below and run:  python3 gen-dashboard.py     (--check verifies it is current)
"""
import json, os, sys

here = os.path.dirname(os.path.abspath(__file__))
out = os.path.join(here, "meridian-ocs", "files", "dashboard.json")
DS = {"type": "prometheus", "uid": "${datasource}"}

# (title, unit, [(expr, legend)], thresholds-or-None, description)
ROWS = [
    ("Charging path", [
        ("Rating latency", "s", [
            ('histogram_quantile(0.50, sum by (le) (rate(ocs_engine_rate_duration_seconds_bucket[$__rate_interval])))', "P50"),
            ('histogram_quantile(0.99, sum by (le) (rate(ocs_engine_rate_duration_seconds_bucket[$__rate_interval])))', "P99"),
            ('histogram_quantile(0.999, sum by (le) (rate(ocs_engine_rate_duration_seconds_bucket[$__rate_interval])))', "P99.9")],
         0.010, "The target is P99 under 10 ms."),
        ("Requests per second", "reqps", [
            ('sum(rate(ocs_engine_requests_total[$__rate_interval]))', "rated"),
            ('sum(rate(ocs_admission_rejected_total[$__rate_interval]))', "rejected by admission")],
         None, "Charging requests processed and refused at the door."),
        ("Degraded, shed and unpriced", "ops", [
            ('sum(rate(ocs_engine_degraded_total[$__rate_interval]))', "answered by fail policy"),
            ('sum(rate(ocs_engine_overloaded_total[$__rate_interval]))', "queue full"),
            ('sum(rate(ocs_engine_unpriced_total[$__rate_interval]))', "units not priced"),
            ('sum(rate(ocs_engine_audit_dropped_total[$__rate_interval]))', "audit dropped")],
         None, "All of these should be zero in normal operation."),
        ("In flight", "short", [('sum by (pod) (ocs_admission_inflight)', "{{pod}}")], None, "Concurrent admitted requests per pod."),
    ]),
    ("Data tier and durability", [
        ("Circuit breaker", "short", [
            ('max(ocs_datatier_breaker_open)', "open"),
            ('max(ocs_datatier_breaker_forced)', "forced open")],
         None, "1 while open. Forced means an operator holds it."),
        ("Replica acknowledgement", "ops", [
            ('sum(rate(ocs_durability_waits_total[$__rate_interval]))', "commits that waited"),
            ('sum(rate(ocs_durability_short_total[$__rate_interval]))', "acknowledged by too few replicas"),
            ('sum(rate(ocs_durability_wait_errors_total[$__rate_interval]))', "wait failed"),
            ('sum(rate(ocs_durability_suspended_total[$__rate_interval]))', "wait suspended")],
         None, "Any 'too few replicas' means the no-double-spend promise is weakened."),
        ("Identity resolution", "ops", [
            ('sum(rate(ocs_identity_resolutions_total[$__rate_interval]))', "resolutions"),
            ('sum(rate(ocs_identity_unknown_total[$__rate_interval]))', "unknown"),
            ('sum(rate(ocs_identity_conflicts_total[$__rate_interval]))', "conflicts"),
            ('sum(rate(ocs_identity_errors_total[$__rate_interval]))', "directory errors")],
         None, "Unknown and conflicting identities are refused, never guessed."),
    ]),
    ("Catalog", [
        ("Version in effect per pod", "short", [('max by (pod) (ocs_catalog_version_in_effect)', "{{pod}}")], None, "Pods should agree; a future-dated version switches everywhere at its time."),
        ("Seconds since the catalog was last read", "s", [('max by (pod) (ocs_catalog_sync_age_seconds)', "{{pod}}")], 300, "-1 means never."),
        ("Publishes by result", "ops", [('sum by (result) (increase(ocs_catalog_publishes_total[$__rate_interval]))', "{{result}}")], None, "Catalog service."),
        ("Replication errors", "ops", [('sum by (pod) (rate(ocs_catalog_sync_errors_total[$__rate_interval]))', "{{pod}}")], None, "A node keeps serving its last good catalog."),
    ]),
    ("Provisioning", [
        ("Submissions by result", "ops", [('sum by (result) (rate(ocs_prov_submissions_total[$__rate_interval]))', "{{result}}")], None, "accepted, duplicate (idempotent replay), rejected, conflict, unavailable."),
        ("Commands finished", "ops", [
            ('sum by (outcome) (rate(ocs_prov_commands_total[$__rate_interval]))', "{{outcome}}"),
            ('sum(rate(ocs_prov_dead_lettered_total[$__rate_interval]))', "dead-lettered")],
         None, "'failed' is a business refusal recorded for the caller."),
        ("Command lag (accepted to outcome)", "s", [
            ('histogram_quantile(0.50, sum by (le) (rate(ocs_prov_command_lag_seconds_bucket[$__rate_interval])))', "P50"),
            ('histogram_quantile(0.99, sum by (le) (rate(ocs_prov_command_lag_seconds_bucket[$__rate_interval])))', "P99")],
         30, "A growing lag means the workers are behind."),
        ("API requests", "reqps", [('sum by (code) (rate(ocs_prov_http_requests_total[$__rate_interval]))', "{{code}}")], None, "By status class."),
        ("Worker retries", "ops", [('sum(rate(ocs_prov_worker_retries_total[$__rate_interval]))', "retries")], None, "Infrastructure failures while applying."),
    ]),
    ("Mediation", [
        ("Records by outcome", "ops", [('sum by (outcome) (rate(ocs_mediation_records_total[$__rate_interval]))', "{{outcome}}")], None, "received, output, suspense, rejected, duplicate, unrated."),
        ("Units committed", "ops", [
            ('sum(rate(ocs_mediation_units_total[$__rate_interval]))', "committed"),
            ('sum(rate(ocs_mediation_stale_units_total[$__rate_interval]))', "stale (already committed)")],
         None, "Files and partition slices."),
        ("Mapping reloads", "ops", [
            ('sum(increase(ocs_mapping_reloads_total[$__rate_interval]))', "loaded"),
            ('sum(increase(ocs_mapping_rejected_total[$__rate_interval]))', "rejected")],
         None, "A rejected mapping leaves the previous one in force."),
    ]),
    ("Process", [
        ("Goroutines", "short", [('sum by (pod) (go_goroutines)', "{{pod}}")], None, ""),
        ("Heap in use", "bytes", [('sum by (pod) (go_memstats_heap_alloc_bytes)', "{{pod}}")], None, ""),
        ("GC pause time", "s", [('sum by (pod) (rate(go_gc_pause_seconds_total[$__rate_interval]))', "{{pod}}")], None, "Seconds of stop-the-world pause per second."),
    ]),
]


def panel(pid, title, unit, targets, threshold, desc, x, y):
    p = {
        "id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"h": 8, "w": 8, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 8, "spanNulls": False}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS} for i, (e, l) in enumerate(targets)],
    }
    if threshold is not None:
        p["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"] = {"mode": "line"}
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "red", "value": threshold}]}
    return p


panels, pid, y = [], 1, 0
for title, row in ROWS:
    panels.append({"id": pid, "type": "row", "title": title, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    pid += 1
    y += 1
    for i, (t, unit, targets, thr, desc) in enumerate(row):
        panels.append(panel(pid, t, unit, targets, thr, desc, (i % 3) * 8, y + (i // 3) * 8))
        pid += 1
    y += ((len(row) + 2) // 3) * 8

dash = {
    "uid": "meridian-ocs", "title": "Meridian OCS", "tags": ["meridian-ocs"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "refresh": "30s", "time": {"from": "now-3h", "to": "now"},
    "templating": {"list": [{"name": "datasource", "label": "Data source", "type": "datasource", "query": "prometheus", "current": {}}]},
    "panels": panels,
}
text = json.dumps(dash, indent=1) + "\n"
if "--check" in sys.argv:
    if open(out).read() != text:
        sys.exit("files/dashboard.json is out of date: run deploy/helm/gen-dashboard.py")
else:
    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w").write(text)
