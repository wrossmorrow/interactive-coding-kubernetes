package piconfig

import "strings"

var scrubbedExact = map[string]bool{
	"SESSION_HUB_TOKEN":              true,
	"SESSION_SINK_TOKEN":             true,
	"AGENT_GATEWAY_TOKEN":            true,
	"MOTHERDUCK_TOKEN":               true,
	"GOOGLE_APPLICATION_CREDENTIALS": true,
	"PI_SESSIOND_TOKEN":              true,
}

var scrubbedPrefixes = []string{
	"AWS_",
	"WEB_AWS_",
	"GCP_",
	"CLOUDSDK_",
	"INFISICAL_",
}

// ScrubEnv returns a copy of env with hub tokens, gateway tokens, and cloud
// credentials removed so tool calls executed by pi cannot read them.
func ScrubEnv(env []string, extra map[string]string) []string {
	out := make([]string, 0, len(env)+len(extra))
	seen := make(map[string]bool, len(extra))
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if shouldScrub(k) {
			continue
		}
		if _, override := extra[k]; override {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range extra {
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k+"="+v)
	}
	return out
}

func shouldScrub(key string) bool {
	if scrubbedExact[key] {
		return true
	}
	for _, p := range scrubbedPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}
