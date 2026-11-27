package piconfig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteConfigDirWritesModelsAndAgents(t *testing.T) {
	dir := t.TempDir()
	cat := DefaultCatalog()
	err := WriteConfigDir(dir, WriteOptions{
		Catalog:      cat,
		GatewayToken: "gw-secret",
		AgentsMD:     "# Rules\nBe concise.",
		Attribution: Attribution{
			SessionID:   "sess-123",
			SessionName: "fix-bug",
			SessionKind: "interactive",
			GitHubRepo:  "org/repo",
			GitHubPR:    42,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "models.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc piModelsFile
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	p, ok := doc.Providers["gateway"]
	if !ok {
		t.Fatalf("missing gateway provider: %+v", doc.Providers)
	}
	if p.BaseURL != cat.GatewayEndpoint {
		t.Fatalf("baseUrl = %q, want %q", p.BaseURL, cat.GatewayEndpoint)
	}
	if len(p.Models) != 2 {
		t.Fatalf("models len = %d, want 2", len(p.Models))
	}
	if p.Headers["x-session-id"] != "sess-123" || p.Headers["x-session-kind"] != "interactive" || p.Headers["x-github-pr"] != "42" {
		t.Fatalf("unexpected headers: %+v", p.Headers)
	}
	if p.Headers["Authorization"] != "Bearer gw-secret" {
		t.Fatalf("missing auth header")
	}

	agents, err := os.ReadFile(filepath.Join(dir, "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(agents), "Be concise.") {
		t.Fatalf("unexpected AGENTS.md: %q", string(agents))
	}
}

func TestScrubEnvRemovesSecrets(t *testing.T) {
	in := []string{
		"PATH=/usr/bin",
		"HOME=/home/node",
		"SESSION_HUB_TOKEN=super-secret",
		"AGENT_GATEWAY_TOKEN=gw-secret",
		"AWS_SECRET_ACCESS_KEY=aws-secret",
		"GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp.json",
		"MOTHERDUCK_TOKEN=md-secret",
	}
	out := ScrubEnv(in, map[string]string{
		"PI_CODING_AGENT_DIR": "/tmp/pi",
		"SESSION_KIND":        "interactive",
	})
	joined := strings.Join(out, "\n")
	for _, forbidden := range []string{"SESSION_HUB_TOKEN", "AGENT_GATEWAY_TOKEN", "AWS_SECRET_ACCESS_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "MOTHERDUCK_TOKEN"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("env still contains %s: %v", forbidden, out)
		}
	}
	if !strings.Contains(joined, "PATH=/usr/bin") || !strings.Contains(joined, "PI_CODING_AGENT_DIR=/tmp/pi") {
		t.Fatalf("expected safe/extra env vars preserved: %v", out)
	}
}
