#!/usr/bin/env python3
"""Regenerate meridian-ocs/values.schema.json from values.yaml (needs PyYAML).

Every object in values.yaml becomes a closed object (a misspelt key is an error),
types follow the defaults, "" defaults are optional text-or-number scalars, and the
enums below pin the modes. Run it after editing values.yaml:  python3 gen-schema.py
"""
import json, os, sys
import yaml

here = os.path.dirname(os.path.abspath(__file__))
chart = os.path.join(here, "meridian-ocs")
v = yaml.safe_load(open(os.path.join(chart, "values.yaml")))

ENUMS = {
    "logLevel": ["debug", "info", "warn", "error"],
    "spread.whenUnsatisfiable": ["ScheduleAnyway", "DoNotSchedule"],
    "image.pullPolicy": ["Always", "IfNotPresent", "Never"],
    "gateway.tariff": ["static", "catalog"],
    "gateway.identity.mode": ["directory", "direct"],
    "gateway.identity.onConflict": ["", "reject", "prefer-first"],
    "gateway.nchf.oauth": ["", "required", "off"],
    "gateway.control.auth": ["", "mtls", "off"],
    "provisioning.auth": ["", "mtls", "off"],
    "catalog.auth": ["", "mtls", "off"],
    "aiops.auth": ["", "mtls", "off"],
    "mediation.rating": ["off", "catalog"],
}
SVC = ["ClusterIP", "NodePort", "LoadBalancer"]


def schema(x, path):
    if path in ENUMS:
        return {"type": "string", "enum": ENUMS[path]}
    if path.endswith(".service.type"):
        return {"type": "string", "enum": SVC}
    if isinstance(x, bool):
        return {"type": "boolean"}
    if isinstance(x, int):
        return {"type": "integer", "minimum": 0}
    if isinstance(x, float):
        return {"type": "number", "minimum": 0}
    if isinstance(x, str):
        return {"type": ["string", "integer", "number"]} if x == "" else {"type": "string"}
    if isinstance(x, list):
        return {"type": "array"}
    if isinstance(x, dict):
        if not x:
            return {"type": "object"}
        props = {k: schema(val, f"{path}.{k}" if path else k) for k, val in x.items()}
        return {"type": "object", "properties": props, "additionalProperties": False}
    return {}


s = schema(v, "")
s["$schema"] = "http://json-schema.org/draft-07/schema#"


def node(path):
    n = s
    for p in path.split("."):
        n = n["properties"][p]
    return n


for p in ["gateway.replicas", "provisioning.api.replicas", "provisioning.worker.replicas", "catalog.replicas", "aiops.replicas"]:
    node(p)["minimum"] = 1
for p in ["gateway.diameter.port", "gateway.nchf.port", "gateway.control.port", "provisioning.restPort",
          "provisioning.grpcPort", "catalog.port", "aiops.port"]:
    node(p).update(minimum=1, maximum=65535)

out = os.path.join(chart, "values.schema.json")
text = json.dumps(s, indent=2) + "\n"
if "--check" in sys.argv:
    if open(out).read() != text:
        sys.exit("values.schema.json is out of date: run deploy/helm/gen-schema.py")
else:
    open(out, "w").write(text)
