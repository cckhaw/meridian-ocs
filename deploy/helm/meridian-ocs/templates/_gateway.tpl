{{- define "gateway.env" -}}
{{- $g := .Values.gateway -}}
{{ include "ocs.env.redis" . }}
- {name: OCS_LOG_LEVEL, value: {{ .Values.logLevel | quote }}}
- {name: OCS_GY_ADDR, value: {{ if $g.diameter.enabled }}{{ printf ":%d" (int $g.diameter.port) | quote }}{{ else }}"off"{{ end }}}
- {name: OCS_NCHF_ADDR, value: {{ if $g.nchf.enabled }}{{ printf ":%d" (int $g.nchf.port) | quote }}{{ else }}"off"{{ end }}}
- {name: OCS_ORIGIN_HOST, value: {{ printf "$(POD_NAME).%s" $g.diameter.originHostSuffix | quote }}}
- {name: OCS_ORIGIN_REALM, value: {{ $g.diameter.realm | quote }}}
- {name: OCS_HOST_IP, value: "$(POD_IP)"}
- {name: OCS_REGION, value: {{ $g.region | quote }}}
- {name: OCS_MAX_TPS, value: {{ $g.maxTps | quote }}}
- {name: OCS_MAX_INFLIGHT, value: {{ $g.maxInflight | quote }}}
- {name: OCS_UNSETTLED_SPOOL, value: {{ ternary "off" "/var/spool/ocs" (eq $g.unsettledSpool.mode "off") | quote }}}
{{- if $g.unsettledSpool.peerMayRepeat }}
- {name: OCS_UNSETTLED_PEER_REPEATS, value: "true"}
{{- end }}
- {name: OCS_TARIFF, value: {{ $g.tariff | quote }}}
- {name: OCS_IDENTITY_MODE, value: {{ $g.identity.mode | quote }}}
{{- include "ocs.opt" (dict "name" "OCS_DEFAULT_PLAN" "value" $g.defaultPlan) }}
{{- include "ocs.opt" (dict "name" "OCS_CASH_BYTES_RATE" "value" $g.cashBytesRate) }}
{{- include "ocs.opt" (dict "name" "OCS_PLAN_CACHE_TTL" "value" $g.planCacheTTL) }}
{{- include "ocs.opt" (dict "name" "OCS_IDENTITY_CACHE_TTL" "value" $g.identity.cacheTTL) }}
{{- include "ocs.opt" (dict "name" "OCS_IDENTITY_ON_CONFLICT" "value" $g.identity.onConflict) }}
{{- if $g.nchf.enabled }}
- {name: OCS_NCHF_OAUTH, value: {{ $g.nchf.oauth | quote }}}
{{- if $g.nchf.tls.secretName }}
- {name: OCS_NCHF_TLS_CERT, value: /tls/nchf/tls.crt}
- {name: OCS_NCHF_TLS_KEY, value: /tls/nchf/tls.key}
{{- end }}
{{- if $g.nchf.tls.requireClientCert }}
- {name: OCS_NCHF_CLIENT_CA, value: /tls/nchf/ca.crt}
{{- end }}
{{- if eq $g.nchf.oauth "required" }}
- {name: OCS_NCHF_OAUTH_KEYS, value: {{ printf "/oauth/%s" $g.nchf.oauthKeys.key | quote }}}
{{- end }}
{{- include "ocs.opt" (dict "name" "OCS_NCHF_OAUTH_AUDIENCE" "value" $g.nchf.audience) }}
{{- include "ocs.opt" (dict "name" "OCS_NCHF_OAUTH_ISSUERS" "value" $g.nchf.issuers) }}
{{- include "ocs.opt" (dict "name" "OCS_NCHF_OAUTH_SCOPE" "value" $g.nchf.scope) }}
{{- include "ocs.opt" (dict "name" "OCS_NF_INSTANCE_ID" "value" $g.nchf.nfInstanceId) }}
{{- end }}
{{- if $g.control.enabled }}
- {name: OCS_CONTROL_ADDR, value: {{ printf ":%d" (int $g.control.port) | quote }}}
- {name: OCS_CONTROL_AUTH, value: {{ $g.control.auth | quote }}}
{{- if eq $g.control.auth "mtls" }}
- {name: OCS_CONTROL_TLS_CERT, value: /tls/control/tls.crt}
- {name: OCS_CONTROL_TLS_KEY, value: /tls/control/tls.key}
- {name: OCS_CONTROL_CLIENT_CA, value: /tls/control/ca.crt}
{{- include "ocs.opt" (dict "name" "OCS_CONTROL_AGENTS" "value" (join "," $g.control.agents)) }}
{{- include "ocs.opt" (dict "name" "OCS_CONTROL_APPROVERS" "value" (join "," $g.control.approvers)) }}
{{- end }}
{{- end }}
{{- end -}}
