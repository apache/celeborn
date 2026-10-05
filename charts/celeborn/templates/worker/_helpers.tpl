{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Common labels for Celeborn worker resources
*/}}
{{- define "celeborn.worker.labels" -}}
{{ include "celeborn.labels" . }}
app.kubernetes.io/role: worker
{{- if .zone }}
celeborn.apache.org/zone: {{ .zone.name }}
{{- end }}
{{- end }}

{{/*
Selector labels for Celeborn worker pods. The zone label is only added when rendering
within a zone context, so that cluster-wide selectors (service, pod monitor) keep
matching the workers of every zone.
*/}}
{{- define "celeborn.worker.selectorLabels" -}}
{{ include "celeborn.selectorLabels" . }}
app.kubernetes.io/role: worker
{{- if .zone }}
celeborn.apache.org/zone: {{ .zone.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the worker service to use
*/}}
{{- define "celeborn.worker.service.name" -}}
{{ include "celeborn.fullname" . }}-worker-svc
{{- end }}

{{/*
Create worker Service http port params if metrics is enabled
*/}}
{{- define "celeborn.worker.service.port" -}}
{{- $metricsEnabled := true -}}
{{- $workerPort := 9096 -}}
{{- range $key, $val := .Values.celeborn }}
{{- if eq $key "celeborn.metrics.enabled" }}
{{- $metricsEnabled = $val -}}
{{- end }}
{{- if eq $key "celeborn.worker.http.port" }}
{{- $workerPort = $val -}}
{{- end }}
{{- end }}
{{- if eq (toString $metricsEnabled) "true" -}}
ports:
  - port: {{ $workerPort }}
    targetPort: {{ $workerPort }}
    protocol: TCP
    name: celeborn-worker-http
{{- end }}
{{- end }}

{{/*
Create the name of the worker priority class to use
*/}}
{{- define "celeborn.worker.priorityClass.name" -}}
{{- with .Values.worker.priorityClass.name -}}
{{ . }}
{{- else -}}
{{ include "celeborn.fullname" . }}-worker-priority-class
{{- end }}
{{- end }}

{{/*
Create the name of the worker statefulset to use
*/}}
{{- define "celeborn.worker.statefulSet.name" -}}
{{- if .zone -}}
{{ include "celeborn.fullname" . }}-worker-{{ .zone.name }}
{{- else -}}
{{ include "celeborn.fullname" . }}-worker
{{- end }}
{{- end }}

{{/*
Number of replicas for a worker statefulset. Without zone-aware replication this is
`worker.replicas` as-is; with it, `worker.replicas` is the total across all zones and each
zone gets `ceil(replicas / zones)` unless the zone overrides it.
*/}}
{{- define "celeborn.worker.replicas" -}}
{{- if .zone -}}
{{- /* Key presence, not truthiness: an explicit 0 parks a zone without removing it. */ -}}
{{- if and (hasKey .zone "replicas") (not (kindIs "invalid" .zone.replicas)) -}}
{{ .zone.replicas }}
{{- else -}}
{{ divf .Values.worker.replicas (len .Values.worker.zoneAwareReplication.zones) | ceil | int }}
{{- end }}
{{- else -}}
{{ .Values.worker.replicas }}
{{- end }}
{{- end }}

{{/*
Label selector used by the built-in autoscaling triggers. Scoped by pod name to this
statefulset's own workers, so releases sharing a namespace - or sharing a zone name - never
scale on each other's workers. The `zone` metric label is not enough on its own: it says
nothing about which release a worker belongs to.
*/}}
{{- define "celeborn.worker.autoscaling.selector" -}}
{{- /* Anchored on the ordinal, so statefulset `x-a` does not also match the pods of `x-a-y`. */ -}}
{{ printf "role=\"Worker\",namespace=\"%s\",pod=~\"%s-[0-9]+\"" .Release.Namespace (include "celeborn.worker.statefulSet.name" .) }}
{{- end }}

{{/*
Built-in autoscaling triggers, in front of any the user adds. Both exclude a decommissioning
worker, whose disk stays full and whose slots stay allocated while it drains - counting it
would have the fleet scale out to replace capacity it is still holding.
*/}}
{{- define "celeborn.worker.autoscaling.defaultTriggers" -}}
{{- $selector := include "celeborn.worker.autoscaling.selector" . -}}
{{- $live := printf "and on (instance) metrics_IsDecommissioningWorker_Value{%s} == 0" $selector -}}
{{- if .Values.worker.autoscaling.diskUsage.enabled }}
- type: prometheus
  {{- /* Value, not KEDA's AverageValue default: a ratio must not be divided by the replicas. */}}
  metricType: Value
  metadata:
    serverAddress: {{ required "worker.autoscaling.prometheusAddress is required by the built-in triggers" .Values.worker.autoscaling.prometheusAddress }}
    query: max((1 - metrics_DeviceCelebornFreeBytes_Value{{ printf "{%s}" $selector }} / metrics_DeviceCelebornTotalBytes_Value{{ printf "{%s}" $selector }}) {{ $live }})
    threshold: {{ .Values.worker.autoscaling.diskUsage.threshold | quote }}
{{- end }}
{{- if .Values.worker.autoscaling.memoryUsage.enabled }}
- type: prometheus
  {{- /* Value, not KEDA's AverageValue default: a ratio must not be divided by the replicas. */}}
  metricType: Value
  metadata:
    serverAddress: {{ required "worker.autoscaling.prometheusAddress is required by the built-in triggers" .Values.worker.autoscaling.prometheusAddress }}
    query: max(metrics_DirectMemoryUsageRatio_Value{{ printf "{%s}" $selector }} {{ $live }})
    threshold: {{ .Values.worker.autoscaling.memoryUsage.threshold | quote }}
{{- end }}
{{- end }}

{{/*
Create the name of the worker podmonitor to use
*/}}
{{- define "celeborn.worker.podMonitor.name" -}}
{{ include "celeborn.fullname" . }}-worker-podmonitor
{{- end }}

{{/*
Create worker annotations if metrics is enabled
*/}}
{{- define "celeborn.worker.metrics.annotations" -}}
{{- $metricsEnabled := true -}}
{{- $metricsPath := "/metrics/prometheus" -}}
{{- $workerPort := 9096 -}}
{{- range $key, $val := .Values.celeborn }}
{{- if eq $key "celeborn.metrics.enabled" }}
{{- $metricsEnabled = $val -}}
{{- end }}
{{- if eq $key "celeborn.metrics.prometheus.path" }}
{{- $metricsPath = $val -}}
{{- end }}
{{- if eq $key "celeborn.worker.http.port" }}
{{- $workerPort = $val -}}
{{- end }}
{{- end }}
{{- if eq (toString $metricsEnabled) "true" -}}
prometheus.io/path: {{ $metricsPath }}
prometheus.io/port: '{{ $workerPort }}'
prometheus.io/scheme: 'http'
prometheus.io/scrape: 'true'
{{- end }}
{{- end }}
