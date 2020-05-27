{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "schema-registry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "schema-registry.configmap" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s-configmap" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "schema-registry.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "auth-volume.name" -}}
{{- printf "auth-passwd-secret" -}}
{{- end -}}

{{- define "auth-volume.path" -}}
{{- printf "/etc/auth-passwd-volume" -}}
{{- end -}}

{{- define "tls-volume.name" -}}
{{- printf "tls-secret" -}}
{{- end -}}

{{- define "tls-volume.path" -}}
{{- printf "/etc/secret-volume" -}}
{{- end -}}
