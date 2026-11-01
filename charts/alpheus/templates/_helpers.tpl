{{/* The registry images are pulled from (SPEC 14). */}}
{{- define "alpheus.registry" -}}
{{- .Values.registry | default "registry.alpheus-dev.org" | trimSuffix "/" -}}
{{- end -}}

{{/* An image reference from a values block: pinned by digest when one is set. */}}
{{- define "alpheus.image" -}}
{{- $ref := printf "%s/%s" (include "alpheus.registry" .root) .image.repository -}}
{{- if .image.digest -}}
{{- printf "%s@%s" $ref .image.digest -}}
{{- else -}}
{{- printf "%s:%s" $ref (.image.tag | default .root.Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{- define "alpheus.labels" -}}
app.kubernetes.io/name: alpheus-controller
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
