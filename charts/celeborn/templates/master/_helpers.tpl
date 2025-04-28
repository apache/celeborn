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
{{- define "celeborn.master.labels" -}}
{{ include "celeborn.labels" . }}
app.kubernetes.io/role: master
{{- end }}

{{/*
Selector labels for Celeborn master pods
*/}}
{{- define "celeborn.master.selectorLabels" -}}
{{ include "celeborn.selectorLabels" . }}
app.kubernetes.io/role: master
{{- end }}

{{/*
Create the name of the master service to use
*/}}
{{- define "celeborn.master.service.name" -}}
{{ include "celeborn.fullname" . }}-master-svc
{{- end }}


{{/*
Create the name of the master priority class to use
*/}}
{{- define "celeborn.master.priorityClass.name" -}}
{{- with .Values.master.priorityClass.name -}}
{{ . }}
{{- else -}}
{{ include "celeborn.fullname" . }}-master-priority-class
{{- end }}
{{- end }}

{{/*
Create the name of the master statefulset to use
*/}}
{{- define "celeborn.master.statefulSet.name" -}}
{{ include "celeborn.fullname" . }}-master
{{- end }}

{{/*
Create the name of the master podmonitor to use
*/}}
{{- define "celeborn.master.podMonitor.name" -}}
{{ include "celeborn.fullname" . }}-master-podmonitor
{{- end }}

{{/*
Create master annotations if metrics enables
*/}}
{{- define "celeborn.master.metrics.annotations" -}}
{{- $metricsEnabled := true -}}
{{- $metricsPath := "/metrics/prometheus" -}}
{{- $masterPort := 9098 -}}
{{- range $key, $val := .Values.celeborn }}
{{- if eq $key "celeborn.metrics.enabled" }}
{{- $metricsEnabled = $val -}}
{{- end }}
{{- if eq $key "celeborn.metrics.prometheus.path" }}
{{- $metricsPath = $val -}}
{{- end }}
{{- if eq $key "celeborn.master.http.port" }}
{{- $masterPort = $val -}}
{{- end }}
{{- end }}
{{- if eq (toString $metricsEnabled) "true" -}}
prometheus.io/path: {{ $metricsPath }}
prometheus.io/port: '{{ $masterPort }}'
prometheus.io/scheme: 'http'
prometheus.io/scrape: 'true'
{{- end }}
{{- end }}
