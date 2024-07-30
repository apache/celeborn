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
Common labels for Celeborn master resoruces
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
{{- define "celeborn.worker.serviceName" -}}
{{ include "celeborn.fullname" . }}-worker-svc
{{- end }}


{{/*
Create the name of the worker priority class to use
*/}}
{{- define "celeborn.worker.priorityClassName" -}}
{{- with .Values.worker.priorityClass.name -}}
{{ . }}
{{- else -}}
{{ include "celeborn.fullname" . }}-worker-priority-class
{{- end }}
{{- end }}

{{/*
Create the name of the worker statefulset to use
*/}}
{{- define "celeborn.worker.statefulSetName" -}}
{{ include "celeborn.fullname" . }}-worker
{{- end }}


{{/*
Create the name of the worker podmonitor to use
*/}}
{{- define "celeborn.worker.podMonitorName" -}}
{{ include "celeborn.fullname" . }}-worker-podmonitor
{{- end }}
