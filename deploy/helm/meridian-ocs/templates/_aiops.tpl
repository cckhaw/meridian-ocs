{{- define "aiops.env" -}}
{{- $a := .Values.aiops -}}
- {name: OCS_AIOPS_ADDR, value: {{ printf ":%d" (int $a.port) | quote }}}
- {name: OCS_AIOPS_AUTH, value: {{ $a.auth | quote }}}
{{- if eq $a.auth "mtls" }}
- {name: OCS_AIOPS_TLS_CERT, value: /tls/tls.crt}
- {name: OCS_AIOPS_TLS_KEY, value: /tls/tls.key}
- {name: OCS_AIOPS_CLIENT_CA, value: /tls/ca.crt}
{{- end }}
{{- end -}}
