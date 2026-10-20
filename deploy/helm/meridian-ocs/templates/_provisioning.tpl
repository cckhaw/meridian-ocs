{{- define "provisioning.env" -}}
{{- $root := .root -}}
{{- $p := $root.Values.provisioning -}}
{{ include "ocs.env.redis" $root }}
- {name: OCS_PROV_MODE, value: {{ .mode | quote }}}
- {name: OCS_PROVISIONING_AUTH, value: {{ $p.auth | quote }}}
- {name: OCS_KAFKA_BROKERS, value: {{ $root.Values.kafka.brokers | quote }}}
- {name: OCS_CURRENCY, value: {{ $p.currency | quote }}}
- {name: OCS_PROV_REST_ADDR, value: {{ if ne .mode "worker" }}{{ printf ":%d" (int $p.restPort) | quote }}{{ else }}"off"{{ end }}}
- {name: OCS_PROV_GRPC_ADDR, value: {{ if ne .mode "worker" }}{{ printf ":%d" (int $p.grpcPort) | quote }}{{ else }}"off"{{ end }}}
{{- include "ocs.opt" (dict "name" "OCS_KAFKA_COMMANDS_TOPIC" "value" $root.Values.kafka.commandsTopic) }}
{{- include "ocs.opt" (dict "name" "OCS_KAFKA_EVENTS_TOPIC" "value" $root.Values.kafka.eventsTopic) }}
{{- include "ocs.opt" (dict "name" "OCS_KAFKA_DLQ_TOPIC" "value" $root.Values.kafka.dlqTopic) }}
{{- include "ocs.opt" (dict "name" "OCS_PROV_MAX_TOPUP" "value" $p.maxTopUp) }}
{{- include "ocs.opt" (dict "name" "OCS_PROV_MAX_ADJUST" "value" $p.maxAdjust) }}
{{- include "ocs.opt" (dict "name" "OCS_PROV_WORKERS" "value" $p.workers) }}
- {name: OCS_PROV_APPROVAL, value: {{ $p.approval.policy | quote }}}
{{- include "ocs.opt" (dict "name" "OCS_PROV_APPROVERS" "value" (join "," $p.approval.approvers)) }}
{{- include "ocs.opt" (dict "name" "OCS_PROV_APPROVAL_TTL" "value" $p.approval.ttl) }}
{{- if $p.approval.allowSelf }}
- {name: OCS_PROV_APPROVAL_SELF, value: "true"}
{{- end }}
{{- if eq $p.auth "mtls" }}
- {name: OCS_PROV_TLS_CERT, value: /tls/tls.crt}
- {name: OCS_PROV_TLS_KEY, value: /tls/tls.key}
- {name: OCS_PROV_CLIENT_CA, value: /tls/ca.crt}
{{- include "ocs.opt" (dict "name" "OCS_PROV_CLIENTS" "value" (join "," $p.clients)) }}
{{- include "ocs.opt" (dict "name" "OCS_PROV_PRIVILEGED" "value" (join "," $p.privileged)) }}
{{- end }}
{{- end -}}
