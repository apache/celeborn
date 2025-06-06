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

{{/* Create the name of the service account to use. */}}
{{- define "celeborn.serviceAccount.name" -}}
{{- if .Values.serviceAccount.create }}
{{- .Values.serviceAccount.name | default (include "celeborn.fullname" .) }}
{{- else }}
{{- .Values.serviceAccount.name | default "default" }}
{{- end }}
{{- end }}

{{/* Create the name of the role to use. */}}
{{- define "celeborn.role.name" -}}
{{- if .Values.rbac.create }}
{{- .Values.rbac.roleName | default (include "celeborn.fullname" .) }}
{{- else }}
{{- .Values.rbac.roleName | default "default" }}
{{- end }}
{{- end }}

{{/*
Create the name of the roleBinding to use
*/}}
{{- define "celeborn.roleBinding.name" -}}
{{- if .Values.rbac.create }}
{{- .Values.rbac.roleBindingName | default (include "celeborn.fullname" .) }}
{{- else }}
{{- .Values.rbac.roleBindingName | default "default" }}
{{- end }}
{{- end }}

{{/* Create the name of configmap to use. */}}
{{- define "celeborn.configMap.name" -}}
{{ include "celeborn.fullname" . }}-conf
{{- end -}}

{{/*
Create the name of the celeborn image to use
*/}}
{{- define "celeborn.image" -}}
{{- $imageRegistry := .Values.image.registry | default "docker.io" }}
{{- $imageRepository := .Values.image.repository | default "apache/celeborn" }}
{{- $imageTag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s/%s:%s" $imageRegistry $imageRepository $imageTag }}
{{- end }}
