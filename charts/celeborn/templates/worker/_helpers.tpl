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
nothing about which release a worker belongs to. Neither says which Kubernetes cluster it runs
in, so a Prometheus holding several clusters needs `worker.autoscaling.extraSelector` as well.
*/}}
{{- define "celeborn.worker.autoscaling.selector" -}}
{{- /* Anchored on the ordinal, so statefulset `x-a` does not also match the pods of `x-a-y`. */ -}}
{{- $selector := printf "role=\"Worker\",namespace=\"%s\",pod=~\"%s-[0-9]+\"" .Release.Namespace (include "celeborn.worker.statefulSet.name" .) -}}
{{- with .Values.worker.autoscaling.extraSelector | toString | trim | trimPrefix "," | trimSuffix "," -}}
{{- $selector = printf "%s,%s" $selector . -}}
{{- end -}}
{{ $selector }}
{{- end }}

{{/*
One gauge as the built-in triggers read it: the instant value, or its average over
`worker.autoscaling.window`. Takes a dict with `metric`, `selector` and `window`.
*/}}
{{- define "celeborn.worker.autoscaling.gauge" -}}
{{- $series := printf "metrics_%s_Value{%s}" .metric .selector -}}
{{- if .window -}}
{{ printf "avg_over_time(%s[%s])" $series .window }}
{{- else -}}
{{ $series }}
{{- end -}}
{{- end }}

{{/*
The largest `capacity=` in `celeborn.worker.storage.dirs`, in bytes, or empty when a dir has
none (Celeborn then takes the whole disk). DeviceCelebornFreeBytes is capped by that capacity
but DeviceCelebornTotalBytes is the filesystem's size, so on a disk larger than its capacity
the disk trigger would read the unconfigured part as used.
*/}}
{{- define "celeborn.worker.autoscaling.diskCapacityBytes" -}}
{{- $units := dict "" 1 "b" 1 "k" 1024 "kb" 1024 "ki" 1024 "kib" 1024 "m" 1048576 "mb" 1048576 "mi" 1048576 "mib" 1048576 "g" 1073741824 "gb" 1073741824 "gi" 1073741824 "gib" 1073741824 "t" 1099511627776 "tb" 1099511627776 "ti" 1099511627776 "tib" 1099511627776 "p" 1125899906842624 "pb" 1125899906842624 "pi" 1125899906842624 "pib" 1125899906842624 -}}
{{- $dirs := index (.Values.celeborn | default dict) "celeborn.worker.storage.dirs" | default "" | toString | trim -}}
{{- $max := 0 -}}
{{- $complete := ne $dirs "" -}}
{{- range splitList "," $dirs -}}
{{- $capacity := regexFind "(?i):capacity=[0-9]+[a-z]*" . | lower | trimPrefix ":capacity=" -}}
{{- if not $capacity -}}
{{- $complete = false -}}
{{- else -}}
{{- $unit := regexFind "[a-z]+$" $capacity -}}
{{- if not (hasKey $units $unit) -}}
{{- fail (printf "celeborn.worker.storage.dirs: unknown capacity unit in %q" .) -}}
{{- end -}}
{{- $bytes := mul (trimSuffix $unit $capacity | int64) (get $units $unit) -}}
{{- if gt $bytes $max -}}
{{- $max = $bytes -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $complete -}}
{{ $max }}
{{- end -}}
{{- end }}

{{/*
Built-in autoscaling triggers, in front of any the user adds. All exclude a decommissioning
worker, whose disk stays full and whose slots stay allocated while it drains - counting it
would have the fleet scale out to replace capacity it is still holding. That check reads the
instant value even with a window, so a worker drops out as soon as its drain starts.
*/}}
{{- define "celeborn.worker.autoscaling.defaultTriggers" -}}
{{- $autoscaling := .Values.worker.autoscaling -}}
{{- $selector := include "celeborn.worker.autoscaling.selector" . -}}
{{- $window := $autoscaling.window | toString | trim -}}
{{- /* A PromQL range: units largest first, each at most once (1m30s), and not zero. */ -}}
{{- if and $window (not (and (regexMatch "^([0-9]+y)?([0-9]+w)?([0-9]+d)?([0-9]+h)?([0-9]+m)?([0-9]+s)?([0-9]+ms)?$" $window) (regexMatch "[1-9]" $window))) -}}
{{- fail (printf "worker.autoscaling.window must be a positive PromQL duration such as 5m or 1m30s, got %q" $window) -}}
{{- end -}}
{{- $gauge := dict "selector" $selector "window" $window -}}
{{- $live := printf "and on (instance) metrics_IsDecommissioningWorker_Value{%s} == 0" $selector -}}
{{- if $autoscaling.diskUsage.enabled }}
- type: prometheus
  {{- /* Value, not KEDA's AverageValue default: a ratio must not be divided by the replicas. */}}
  metricType: Value
  metadata:
    serverAddress: {{ required "worker.autoscaling.prometheusAddress is required by the built-in triggers" $autoscaling.prometheusAddress }}
    {{- $total := include "celeborn.worker.autoscaling.gauge" (merge (dict "metric" "DeviceCelebornTotalBytes") $gauge) }}
    {{- with include "celeborn.worker.autoscaling.diskCapacityBytes" . }}
    {{- $total = printf "clamp_max(%s, %s)" $total . }}
    {{- end }}
    query: max((1 - {{ include "celeborn.worker.autoscaling.gauge" (merge (dict "metric" "DeviceCelebornFreeBytes") $gauge) }} / {{ $total }}) {{ $live }})
    threshold: {{ $autoscaling.diskUsage.threshold | quote }}
{{- end }}
{{- if $autoscaling.memoryUsage.enabled }}
- type: prometheus
  {{- /* Value, not KEDA's AverageValue default: a ratio must not be divided by the replicas. */}}
  metricType: Value
  metadata:
    serverAddress: {{ required "worker.autoscaling.prometheusAddress is required by the built-in triggers" $autoscaling.prometheusAddress }}
    query: max({{ include "celeborn.worker.autoscaling.gauge" (merge (dict "metric" "DirectMemoryUsageRatio") $gauge) }} {{ $live }})
    threshold: {{ $autoscaling.memoryUsage.threshold | quote }}
{{- end }}
{{- if $autoscaling.activeSlots.enabled }}
- type: prometheus
  {{- /* AverageValue: a count, so KEDA divides the total by the replicas and compares it per worker. */}}
  metricType: AverageValue
  metadata:
    serverAddress: {{ required "worker.autoscaling.prometheusAddress is required by the built-in triggers" $autoscaling.prometheusAddress }}
    query: sum({{ include "celeborn.worker.autoscaling.gauge" (merge (dict "metric" "ActiveSlotsCount") $gauge) }} {{ $live }})
    threshold: {{ required "worker.autoscaling.activeSlots.threshold is required when the active slots trigger is enabled" $autoscaling.activeSlots.threshold | toString | quote }}
{{- end }}
{{- end }}

{{/*
HPA behavior for the scaled object. With `worker.drain.enabled` every scale-in waits for a
drain and the statefulset cannot grow meanwhile, so unless `behavior.scaleDown` is set it is
paced as slowly as the HPA allows: the highest recommendation of the past hour, then at most
one worker per 30 minutes.
*/}}
{{- define "celeborn.worker.autoscaling.behavior" -}}
{{- $behavior := deepCopy (.Values.worker.autoscaling.behavior | default dict) -}}
{{- if and .Values.worker.drain.enabled (not (hasKey $behavior "scaleDown")) -}}
{{- $_ := set $behavior "scaleDown" (dict "stabilizationWindowSeconds" 3600 "policies" (list (dict "type" "Pods" "value" 1 "periodSeconds" 1800))) -}}
{{- end -}}
{{- with $behavior }}{{ toYaml . }}{{ end -}}
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
