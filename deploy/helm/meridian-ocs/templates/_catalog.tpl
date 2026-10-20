{{- define "catalog.env" -}}
{{- $c := .Values.catalog -}}
- {name: OCS_REDIS_ADDR, value: {{ required "redis.addr is required" .Values.redis.addr | quote }}}
- {name: OCS_CATALOG_ADDR, value: {{ printf ":%d" (int $c.port) | quote }}}
- {name: OCS_CATALOG_AUTH, value: {{ $c.auth | quote }}}
{{- include "ocs.opt" (dict "name" "OCS_CATALOG_RETAIN" "value" $c.retain) }}
{{- include "ocs.opt" (dict "name" "OCS_CATALOG_RESYNC" "value" $c.resync) }}
{{- if eq $c.auth "mtls" }}
- {name: OCS_CATALOG_TLS_CERT, value: /tls/tls.crt}
- {name: OCS_CATALOG_TLS_KEY, value: /tls/tls.key}
- {name: OCS_CATALOG_CLIENT_CA, value: /tls/ca.crt}
- {name: OCS_CATALOG_PUBLISHERS, value: {{ join "," $c.publishers | quote }}}
{{- end }}
{{- end -}}
