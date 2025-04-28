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
Common labels for Celeborn master resources
*/}}
{{- define "celeborn.worker.labels" -}}
{{ include "celeborn.labels" . }}
app.kubernetes.io/role: worker
{{- end }}

{{/*
Selector labels for Celeborn master pods
*/}}
{{- define "celeborn.worker.selectorLabels" -}}
{{ include "celeborn.selectorLabels" . }}
app.kubernetes.io/role: worker
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
{{ include "celeborn.fullname" . }}-worker
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
