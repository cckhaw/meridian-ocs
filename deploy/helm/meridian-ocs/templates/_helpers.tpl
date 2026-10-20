{{/* Names and labels */}}
{{- define "ocs.name" -}}{{ printf "%s-%s" .root.Release.Name .name | trunc 63 | trimSuffix "-" }}{{- end -}}

{{- define "ocs.selector" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}

{{- define "ocs.labels" -}}
{{ include "ocs.selector" . }}
app.kubernetes.io/part-of: meridian-ocs
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end -}}

{{/* image <repository>/<service>:<tag>; a tag of the form sha256:... is a digest. */}}
{{- define "ocs.image" -}}
{{- $repo := required "image.repository is required" .root.Values.image.repository -}}
{{- $tag := required "image.tag is required" .root.Values.image.tag -}}
{{- if hasPrefix "sha256:" $tag -}}{{ printf "%s/%s@%s" $repo .service $tag }}{{- else -}}{{ printf "%s/%s:%s" $repo .service $tag }}{{- end -}}
{{- end -}}

{{/* A security mode must be chosen, and "off" needs allowInsecure. Usage: include "ocs.auth" (dict "root" $ "what" "provisioning.auth" "value" .Values.provisioning.auth) */}}
{{- define "ocs.auth" -}}
{{- if not (has .value (list "mtls" "off")) -}}{{ fail (printf "%s must be set to \"mtls\" or \"off\" (got %q); \"off\" is for throwaway development clusters only" .what .value) }}{{- end -}}
{{- if and (eq .value "off") (not .root.Values.allowInsecure) -}}{{ fail (printf "%s=off is refused unless allowInsecure=true (development clusters only)" .what) }}{{- end -}}
{{- .value -}}
{{- end -}}

{{/* Redis access control: refusals that mirror the services' own. */}}
{{- define "ocs.redis.check" -}}
{{- $r := .Values.redis -}}
{{- if and (not $r.password.secretName) (not .Values.allowInsecure) -}}{{ fail "redis.password.secretName is required: an open Redis is refused unless allowInsecure=true (development clusters only)" }}{{- end -}}
{{- if and $r.username (not $r.password.secretName) -}}{{ fail "redis.username needs redis.password.secretName" }}{{- end -}}
{{- if and $r.tls.enabled $r.tls.mesh -}}{{ fail "redis.tls.enabled and redis.tls.mesh are both set: choose one" }}{{- end -}}
{{- if and (not $r.tls.enabled) (not $r.tls.mesh) (not .Values.allowInsecure) -}}{{ fail "redis.tls.enabled=true is required (or redis.tls.mesh=true when a service mesh encrypts the connection): an unencrypted Redis connection is refused unless allowInsecure=true" }}{{- end -}}
{{- if and (not $r.tls.enabled) (or $r.tls.secretName $r.tls.serverName $r.tls.clientCert) -}}{{ fail "redis.tls.secretName, serverName and clientCert have no effect without redis.tls.enabled: refusing a configuration that looks encrypted but is not" }}{{- end -}}
{{- if and $r.tls.clientCert (not $r.tls.secretName) -}}{{ fail "redis.tls.clientCert needs redis.tls.secretName (tls.crt and tls.key are read from it)" }}{{- end -}}
{{- end -}}

{{/* How a component reaches Redis: address, credentials and TLS (files under /redis, see ocs.component). */}}
{{- define "ocs.env.redisAccess" -}}
{{- include "ocs.redis.check" . -}}
{{- $r := .Values.redis }}
- {name: OCS_REDIS_ADDR, value: {{ required "redis.addr is required" $r.addr | quote }}}
{{- if $r.username }}
- {name: OCS_REDIS_USERNAME, value: {{ $r.username | quote }}}
{{- end }}
{{- if $r.password.secretName }}
- {name: OCS_REDIS_PASSWORD_FILE, value: {{ printf "/redis/password/%s" $r.password.key | quote }}}
{{- end }}
{{- if $r.tls.enabled }}
- {name: OCS_REDIS_TLS, value: "on"}
{{- if $r.tls.secretName }}
- {name: OCS_REDIS_TLS_CA, value: /redis/tls/ca.crt}
{{- end }}
{{- if $r.tls.clientCert }}
- {name: OCS_REDIS_TLS_CERT, value: /redis/tls/tls.crt}
- {name: OCS_REDIS_TLS_KEY, value: /redis/tls/tls.key}
{{- end }}
{{- if $r.tls.serverName }}
- {name: OCS_REDIS_TLS_SERVER_NAME, value: {{ $r.tls.serverName | quote }}}
{{- end }}
{{- end }}
{{- end -}}

{{/* Settings every component that uses Redis for balances shares. */}}
{{- define "ocs.env.redis" -}}
{{ include "ocs.env.redisAccess" . }}
- {name: OCS_REDIS_REPLICAS, value: {{ .Values.redis.requiredReplicas | quote }}}
- {name: OCS_REDIS_WAIT_TIMEOUT, value: {{ .Values.redis.waitTimeout | quote }}}
{{- if .Values.redis.allowNoReplica }}
{{- if not .Values.allowInsecure }}{{ fail "redis.allowNoReplica=true needs allowInsecure=true: it starts without the replicas that make commits durable" }}{{- end }}
- {name: OCS_ALLOW_NO_REPLICA, value: "true"}
{{- end }}
- {name: OCS_IDENTITY_PREFIX, value: {{ .Values.identityPrefix | quote }}}
{{- end -}}

{{/* Only emit an env var when its value is set. Usage: include "ocs.opt" (dict "name" "OCS_X" "value" .Values.x) */}}
{{- define "ocs.opt" -}}
{{- if and (ne (toString .value) "") (ne (toString .value) "<nil>") }}
- {name: {{ .name }}, value: {{ .value | toString | quote }}}
{{- end }}
{{- end -}}

{{/* TLS secret volume. Usage: include "ocs.tlsVolume" (dict "name" "tls" "secret" .Values.x.tls.secretName) */}}
{{- define "ocs.tlsVolume" -}}
- name: {{ .name }}
  secret: {secretName: {{ .secret | quote }}, defaultMode: 0440}
{{- end -}}

{{/*
A component: ServiceAccount-less workload + admin Service + PDB (+ HPA).
Inputs: root, name, service (binary), kind (Deployment|StatefulSet), replicas,
resources, ports (list of {name, port}), env (yaml list text), volumes, mounts,
claims (StatefulSet), parallel (StatefulSet: start and replace pods together), pdb ({maxUnavailable}),
autoscaling (optional), checksum (optional).
*/}}
{{- define "ocs.component" -}}
{{- $root := .root -}}
{{- $full := include "ocs.name" . -}}
{{- $v := $root.Values -}}
---
apiVersion: apps/v1
kind: {{ .kind }}
metadata:
  name: {{ $full }}
  labels: {{- include "ocs.labels" . | nindent 4 }}
spec:
  {{- if eq .kind "StatefulSet" }}
  serviceName: {{ $full }}-admin
  {{- if .parallel }}
  podManagementPolicy: Parallel    # pods are independent: do not start or replace them one after the other
  {{- end }}
  {{- end }}
  {{- if not (and .autoscaling .autoscaling.enabled) }}
  replicas: {{ .replicas }}
  {{- end }}
  selector:
    matchLabels: {{- include "ocs.selector" . | nindent 6 }}
  {{- if eq .kind "Deployment" }}
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxUnavailable: 0, maxSurge: 1}
  {{- end }}
  template:
    metadata:
      labels: {{- include "ocs.labels" . | nindent 8 }}
      annotations:
        {{- if .checksum }}
        checksum/config: {{ .checksum }}
        {{- end }}
        {{- with $v.podAnnotations }}{{ toYaml . | nindent 8 }}{{- end }}
    spec:
      serviceAccountName: {{ $root.Release.Name }}
      automountServiceAccountToken: false
      securityContext: {{- toYaml $v.podSecurityContext | nindent 8 }}
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      {{- with $v.image.pullSecrets }}
      imagePullSecrets: {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $v.nodeSelector }}
      nodeSelector: {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $v.tolerations }}
      tolerations: {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if $v.spread.enabled }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: {{ $v.spread.whenUnsatisfiable }}
          labelSelector:
            matchLabels: {{- include "ocs.selector" . | nindent 14 }}
        - maxSkew: 1
          topologyKey: {{ $v.spread.zoneKey }}
          whenUnsatisfiable: {{ $v.spread.whenUnsatisfiable }}
          labelSelector:
            matchLabels: {{- include "ocs.selector" . | nindent 14 }}
      {{- end }}
      containers:
        - name: {{ .name }}
          image: {{ include "ocs.image" . | quote }}
          imagePullPolicy: {{ $v.image.pullPolicy }}
          securityContext: {{- toYaml $v.containerSecurityContext | nindent 12 }}
          ports:
            {{- range .ports }}
            - {name: {{ .name }}, containerPort: {{ .port }}, protocol: {{ .protocol | default "TCP" }}}
            {{- end }}
            - {name: admin, containerPort: 8080, protocol: TCP}
          env:
            - {name: OCS_ADMIN_ADDR, value: ":8080"}
            - name: POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: POD_IP
              valueFrom: {fieldRef: {fieldPath: status.podIP}}
            {{- .env | nindent 12 }}
          # /readyz is 503 until the service has connected to Redis (and, where
          # configured, Kafka) and bound its listeners, and again while it drains.
          startupProbe: {httpGet: {path: /readyz, port: admin}, periodSeconds: 2, failureThreshold: 60}
          readinessProbe: {httpGet: {path: /readyz, port: admin}, periodSeconds: 3, failureThreshold: 2}
          livenessProbe: {httpGet: {path: /healthz, port: admin}, periodSeconds: 10, failureThreshold: 3}
          resources: {{- toYaml .resources | nindent 12 }}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
            {{- if .mounts }}{{ .mounts | nindent 12 }}{{- end }}
            {{- if .redis }}
            {{- if $v.redis.password.secretName }}
            - {name: redis-password, mountPath: /redis/password, readOnly: true}
            {{- end }}
            {{- if and $v.redis.tls.enabled $v.redis.tls.secretName }}
            - {name: redis-tls, mountPath: /redis/tls, readOnly: true}
            {{- end }}
            {{- end }}
      volumes:
        - {name: tmp, emptyDir: {sizeLimit: 64Mi}}
        {{- if .volumes }}{{ .volumes | nindent 8 }}{{- end }}
        {{- if .redis }}
        {{- if $v.redis.password.secretName }}
        - name: redis-password
          secret:
            secretName: {{ $v.redis.password.secretName | quote }}
            defaultMode: 0440
            items: [{key: {{ $v.redis.password.key | quote }}, path: {{ $v.redis.password.key | quote }}}]
        {{- end }}
        {{- if and $v.redis.tls.enabled $v.redis.tls.secretName }}
        - name: redis-tls
          secret: {secretName: {{ $v.redis.tls.secretName | quote }}, defaultMode: 0440}
        {{- end }}
        {{- end }}
  {{- if .claims }}
  volumeClaimTemplates: {{- .claims | nindent 4 }}
  {{- end }}
---
# The admin port (health and metrics) is never exposed outside the cluster.
apiVersion: v1
kind: Service
metadata:
  name: {{ $full }}-admin
  labels: {{- include "ocs.labels" . | nindent 4 }}
    ocs.meridian/admin: "true"
spec:
  clusterIP: {{ if eq .kind "StatefulSet" }}None{{ else }}""{{ end }}
  selector: {{- include "ocs.selector" . | nindent 4 }}
  ports:
    - {name: admin, port: 8080, targetPort: admin}
{{- if .pdb }}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ $full }}
  labels: {{- include "ocs.labels" . | nindent 4 }}
spec:
  maxUnavailable: {{ .pdb.maxUnavailable }}
  selector:
    matchLabels: {{- include "ocs.selector" . | nindent 6 }}
{{- end }}
{{- if and .autoscaling .autoscaling.enabled }}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ $full }}
  labels: {{- include "ocs.labels" . | nindent 4 }}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: {{ .kind }}, name: {{ $full }}}
  minReplicas: {{ .autoscaling.minReplicas }}
  maxReplicas: {{ .autoscaling.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target: {type: Utilization, averageUtilization: {{ .autoscaling.targetCPUUtilizationPercentage }}}
  behavior:
    scaleDown: {stabilizationWindowSeconds: 600}   # connections and sessions are sticky; scale in slowly
{{- end }}
{{- end -}}

{{/* A client-facing Service. Inputs: root, name, svc (values: type, annotations, ...), ports, and optionally suffix (appended to the resource name). */}}
{{- define "ocs.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "ocs.name" . }}{{ if .suffix }}-{{ .suffix }}{{ end }}
  labels: {{- include "ocs.labels" . | nindent 4 }}
  {{- with .svc.annotations }}
  annotations: {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: {{ .svc.type | default "ClusterIP" }}
  {{- with .svc.externalTrafficPolicy }}
  externalTrafficPolicy: {{ . }}
  {{- end }}
  {{- with .svc.loadBalancerIP }}
  loadBalancerIP: {{ . }}
  {{- end }}
  selector: {{- include "ocs.selector" . | nindent 4 }}
  ports:
    {{- range .ports }}
    - {name: {{ .name }}, port: {{ .port }}, targetPort: {{ .name }}, protocol: {{ .protocol | default "TCP" }}}
    {{- end }}
{{- end -}}
