{{- define "k8s-agents.name" -}}
{{- default "session-hub" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "k8s-agents.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "k8s-agents.name" . -}}
{{- end -}}
{{- end -}}

{{- define "k8s-agents.labels" -}}
app.kubernetes.io/name: {{ include "k8s-agents.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: k8s-agents
{{- end -}}

{{- define "k8s-agents.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k8s-agents.name" . }}
{{- end -}}

{{- define "k8s-agents.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "k8s-agents.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
