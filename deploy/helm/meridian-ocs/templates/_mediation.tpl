{{- define "mediation.env" -}}
{{- $m := .Values.mediation -}}
{{ include "ocs.env.redis" . }}
- {name: OCS_MED_OUT_DIR, value: /data/out}
{{- if $m.input.enabled }}
- {name: OCS_MED_IN_DIR, value: /data/in}
{{- end }}
{{- if $m.kafka.enabled }}
- {name: OCS_KAFKA_BROKERS, value: {{ .Values.kafka.brokers | quote }}}
{{- include "ocs.opt" (dict "name" "OCS_MED_KAFKA_TOPIC" "value" $m.kafka.topic) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_KAFKA_GROUP" "value" $m.kafka.group) }}
{{- end }}
- {name: OCS_MED_RATING, value: {{ $m.rating | quote }}}
{{- include "ocs.opt" (dict "name" "OCS_DEFAULT_PLAN" "value" $m.defaultPlan) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_SCAN_INTERVAL" "value" $m.scanInterval) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_SETTLE" "value" $m.settle) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_MAX_FILE_BYTES" "value" $m.maxFileBytes) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_DEDUP_WINDOW" "value" $m.dedupWindow) }}
{{- include "ocs.opt" (dict "name" "OCS_MED_MAPPING_RELOAD" "value" $m.mappingReload) }}
{{- if $m.mapping }}
- {name: OCS_MED_MAPPING, value: /config/mapping.yaml}
{{- end }}
{{- if $m.cdrMapping }}
- {name: OCS_CDR_MAPPING, value: /config/cdr-mapping.json}
{{- end }}
{{- end -}}
