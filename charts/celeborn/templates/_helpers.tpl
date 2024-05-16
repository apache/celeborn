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
Expand the name of the chart.
*/}}
{{- define "celeborn.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "celeborn.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "celeborn.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "celeborn.labels" -}}
helm.sh/chart: {{ include "celeborn.chart" . }}
{{ include "celeborn.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "celeborn.selectorLabels" -}}
app.kubernetes.io/name: {{ include "celeborn.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "celeborn.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "celeborn.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of configmap to use
*/}}
{{- define "celeborn.configMapName" -}}
{{ include "celeborn.fullname" . }}-conf
{{- end -}}

{{/*
Create the name of the master service to use
*/}}
{{- define "celeborn.masterServiceName" -}}
{{ include "celeborn.fullname" . }}-master-svc
{{- end }}

{{/*
Create the name of the worker service to use
*/}}
{{- define "celeborn.workerServiceName" -}}
{{ include "celeborn.fullname" . }}-worker-svc
{{- end }}

{{/*
Create the name of the master priority class to use
*/}}
{{- define "celeborn.masterPriorityClassName" -}}
{{- if .Values.priorityClass.master.name -}}
{{ .Values.priorityClass.master.name }}
{{- else -}}
{{ include "celeborn.fullname" . }}-master-priority-class
{{- end }}
{{- end }}

{{/*
Create the name of the worker priority class to use
*/}}
{{- define "celeborn.workerPriorityClassName" -}}
{{- if .Values.priorityClass.worker.name -}}
{{ .Values.priorityClass.worker.name }}
{{- else -}}
{{ include "celeborn.fullname" . }}-worker-priority-class
{{- end }}
{{- end }}

{{/*
Create the name of the celeborn image to use
*/}}
{{- define "celeborn.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Create the name of the master statefulset to use
*/}}
{{- define "celeborn.masterStatefulSetName" -}}
{{ include "celeborn.fullname" . }}-master
{{- end }}

{{/*
Create the name of the worker statefulset to use
*/}}
{{- define "celeborn.workerStatefulSetName" -}}
{{ include "celeborn.fullname" . }}-worker
{{- end }}

{{/*
Create the name of the master podmonitor to use
*/}}
{{- define "celeborn.masterPodMonitorName" -}}
{{ include "celeborn.fullname" . }}-master-podmonitor
{{- end }}

{{/*
Create the name of the worker podmonitor to use
*/}}
{{- define "celeborn.workerPodMonitorName" -}}
{{ include "celeborn.fullname" . }}-worker-podmonitor
{{- end }}
