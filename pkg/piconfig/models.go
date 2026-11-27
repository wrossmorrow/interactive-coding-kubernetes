// Package piconfig writes model-agnostic Agent Router models.json catalogs and
// AGENTS.md files for pi, and scrubs sensitive credentials from the pi child
// process environment.
package piconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ModelSpec describes a single model exposed through an OpenAI-compatible
// Agent Router endpoint.
type ModelSpec struct {
	Provider      string   `json:"provider"`
	ID            string   `json:"id"`
	Name          string   `json:"name,omitempty"`
	Reasoning     bool     `json:"reasoning,omitempty"`
	Input         []string `json:"input,omitempty"`
	ContextWindow int      `json:"contextWindow,omitempty"`
	MaxTokens     int      `json:"maxTokens,omitempty"`
}

// Catalog holds the set of models available for dynamic /model switching.
type Catalog struct {
	GatewayEndpoint string      `json:"gatewayEndpoint"`
	DefaultProvider string      `json:"defaultProvider,omitempty"`
	DefaultModel    string      `json:"defaultModel,omitempty"`
	Models          []ModelSpec `json:"models"`
}

// DefaultCatalog returns a neutral development catalog used when no explicit
// catalog file is configured.
func DefaultCatalog() Catalog {
	return Catalog{
		GatewayEndpoint: "http://agent-router.ai-gateway.svc.cluster.local:8080/v1",
		DefaultProvider: "gateway",
		DefaultModel:    "model-alpha",
		Models: []ModelSpec{
			{
				Provider:      "gateway",
				ID:            "model-alpha",
				Name:          "Alpha 27B",
				Reasoning:     true,
				Input:         []string{"text"},
				ContextWindow: 131072,
				MaxTokens:     32768,
			},
			{
				Provider:      "gateway",
				ID:            "model-beta",
				Name:          "Beta Mini",
				Reasoning:     false,
				Input:         []string{"text", "image"},
				ContextWindow: 131072,
				MaxTokens:     16384,
			},
		},
	}
}

// LoadCatalog reads a JSON model catalog from path.
func LoadCatalog(path string) (Catalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Catalog{}, fmt.Errorf("read model catalog %s: %w", path, err)
	}
	var cat Catalog
	if err := json.Unmarshal(b, &cat); err != nil {
		return Catalog{}, fmt.Errorf("parse model catalog %s: %w", path, err)
	}
	return cat, nil
}

// Attribution carries per-session headers stamped onto models.json providers.
type Attribution struct {
	SessionID   string
	SessionName string
	SessionKind string
	GitHubRepo  string
	GitHubPR    int
}

// WriteOptions configures WriteConfigDir.
type WriteOptions struct {
	Catalog      Catalog
	GatewayToken string
	AgentsMD     string
	Attribution  Attribution
}

type piModelsFile struct {
	Providers map[string]piProviderConfig `json:"providers"`
}

type piProviderConfig struct {
	BaseURL    string            `json:"baseUrl"`
	API        string            `json:"api"`
	APIKey     string            `json:"apiKey"`
	AuthHeader bool              `json:"authHeader,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Models     []piModelConfig   `json:"models"`
}

type piModelConfig struct {
	ID            string        `json:"id"`
	Name          string        `json:"name,omitempty"`
	Reasoning     bool          `json:"reasoning"`
	Input         []string      `json:"input,omitempty"`
	ContextWindow int           `json:"contextWindow,omitempty"`
	MaxTokens     int           `json:"maxTokens,omitempty"`
	Cost          piModelCost   `json:"cost"`
	Compat        piModelCompat `json:"compat"`
}

type piModelCost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

type piModelCompat struct {
	SupportsStore         bool   `json:"supportsStore"`
	SupportsDeveloperRole bool   `json:"supportsDeveloperRole"`
	SupportsStrictMode    bool   `json:"supportsStrictMode"`
	MaxTokensField        string `json:"maxTokensField,omitempty"`
}

// WriteConfigDir populates dir (intended as $PI_CODING_AGENT_DIR) with
// models.json and optional AGENTS.md. All models route through the OpenAI-
// compatible Agent Router endpoint with mode 0600 permissions.
func WriteConfigDir(dir string, opts WriteOptions) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create pi config dir: %w", err)
	}

	if strings.TrimSpace(opts.AgentsMD) != "" {
		if err := os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte(strings.TrimRight(opts.AgentsMD, "\n")+"\n"), 0o600); err != nil {
			return fmt.Errorf("write AGENTS.md: %w", err)
		}
	}

	if opts.Catalog.GatewayEndpoint == "" || len(opts.Catalog.Models) == 0 {
		return nil
	}

	token := strings.TrimSpace(opts.GatewayToken)
	apiKey := "local"
	authHeader := false
	baseHeaders := map[string]string{}
	if token != "" {
		apiKey = token
		authHeader = true
		baseHeaders["Authorization"] = "Bearer " + token
	}
	if opts.Attribution.SessionID != "" {
		baseHeaders["x-session-id"] = opts.Attribution.SessionID
	}
	if opts.Attribution.SessionName != "" {
		baseHeaders["x-session-name"] = opts.Attribution.SessionName
	}
	if opts.Attribution.SessionKind != "" {
		baseHeaders["x-session-kind"] = opts.Attribution.SessionKind
	}
	if opts.Attribution.GitHubRepo != "" {
		baseHeaders["x-github-repo"] = opts.Attribution.GitHubRepo
	}
	if opts.Attribution.GitHubPR > 0 {
		baseHeaders["x-github-pr"] = strconv.Itoa(opts.Attribution.GitHubPR)
	}

	providers := map[string]piProviderConfig{}
	for _, m := range opts.Catalog.Models {
		pname := strings.TrimSpace(m.Provider)
		if pname == "" {
			pname = "gateway"
		}
		p, exists := providers[pname]
		if !exists {
			hdrs := make(map[string]string, len(baseHeaders))
			for k, v := range baseHeaders {
				hdrs[k] = v
			}
			p = piProviderConfig{
				BaseURL:    opts.Catalog.GatewayEndpoint,
				API:        "openai-completions",
				APIKey:     apiKey,
				AuthHeader: authHeader,
				Headers:    hdrs,
			}
		}
		input := m.Input
		if len(input) == 0 {
			input = []string{"text"}
		}
		name := m.Name
		if name == "" {
			name = m.ID
		}
		p.Models = append(p.Models, piModelConfig{
			ID:            m.ID,
			Name:          name,
			Reasoning:     m.Reasoning,
			Input:         input,
			ContextWindow: m.ContextWindow,
			MaxTokens:     m.MaxTokens,
			Compat: piModelCompat{
				SupportsStore:         false,
				SupportsDeveloperRole: false,
				SupportsStrictMode:    false,
				MaxTokensField:        "max_tokens",
			},
		})
		providers[pname] = p
	}

	raw, err := json.MarshalIndent(piModelsFile{Providers: providers}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal models.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "models.json"), append(raw, '\n'), 0o600); err != nil {
		return fmt.Errorf("write models.json: %w", err)
	}
	return nil
}
