package sessions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPStore implements Store for pi-sessiond by shipping objects over HTTP to
// session-hub's /internal/sessions/{id}/objects/* and /compose endpoints using
// a per-session scoped bearer token. Sandbox pods therefore hold zero cloud
// storage credentials.
type HTTPStore struct {
	baseURL    string
	sessionID  string
	token      string
	httpClient *http.Client
}

// NewHTTPStore returns an HTTPStore scoped to sessionID against hubBaseURL.
func NewHTTPStore(hubBaseURL, sessionID, token string) *HTTPStore {
	return &HTTPStore{
		baseURL:   strings.TrimRight(hubBaseURL, "/"),
		sessionID: sessionID,
		token:     token,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (h *HTTPStore) relPath(key string) (string, error) {
	prefix := "sessions/" + h.sessionID + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("key %q outside scoped session %q", key, h.sessionID)
	}
	return strings.TrimPrefix(key, prefix), nil
}

func (h *HTTPStore) objectURL(rel string) string {
	return fmt.Sprintf("%s/internal/sessions/%s/objects/%s", h.baseURL, h.sessionID, rel)
}

func (h *HTTPStore) Put(ctx context.Context, key string, data []byte, contentType string) error {
	rel, err := h.relPath(key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, h.objectURL(rel), bytes.NewReader(data))
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http store put %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http store put %s: status %d: %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (h *HTTPStore) Get(ctx context.Context, key string) ([]byte, error) {
	rel, err := h.relPath(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.objectURL(rel), nil)
	if err != nil {
		return nil, err
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http store get %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("http store get %s: status %d: %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func (h *HTTPStore) List(ctx context.Context, prefix string) ([]string, error) {
	u := fmt.Sprintf("%s/internal/sessions/%s/objects", h.baseURL, h.sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http store list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("http store list: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(payload.Keys))
	for _, k := range payload.Keys {
		full := k
		if !strings.HasPrefix(full, "sessions/") {
			full = "sessions/" + h.sessionID + "/" + strings.TrimPrefix(k, "/")
		}
		if prefix == "" || strings.HasPrefix(full, prefix) {
			out = append(out, full)
		}
	}
	return out, nil
}

func (h *HTTPStore) Delete(ctx context.Context, key string) error {
	rel, err := h.relPath(key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, h.objectURL(rel), nil)
	if err != nil {
		return err
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("http store delete %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (h *HTTPStore) Compose(ctx context.Context, _ string, _ []string, _ string) error {
	u := fmt.Sprintf("%s/internal/sessions/%s/compose", h.baseURL, h.sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http store compose: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http store compose: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
