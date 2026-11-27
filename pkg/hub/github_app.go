package hub

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// TokenProvider returns a bearer token for GitHub API calls.
type TokenProvider interface {
	Token(ctx context.Context) (string, error)
}

// GitHubAppTokenProvider mints and caches short-lived GitHub App installation
// tokens using RS256 JWTs signed with the App's RSA private key PEM.
type GitHubAppTokenProvider struct {
	appID          string
	installationID string
	key            *rsa.PrivateKey
	baseURL        string
	httpClient     *http.Client

	mu        sync.Mutex
	cached    string
	expiresAt time.Time
}

// NewGitHubAppTokenProvider parses privateKeyPEM (PKCS#1 or PKCS#8 RSA) and
// returns a GitHubAppTokenProvider.
func NewGitHubAppTokenProvider(appID, installationID string, privateKeyPEM []byte, baseURL string) (*GitHubAppTokenProvider, error) {
	if strings.TrimSpace(appID) == "" || strings.TrimSpace(installationID) == "" {
		return nil, fmt.Errorf("github app id and installation id are required")
	}
	block, _ := pem.Decode(privateKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("decode github app private key pem: invalid PEM block")
	}
	var rsaKey *rsa.PrivateKey
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		rsaKey = k
	} else if anyKey, err2 := x509.ParsePKCS8PrivateKey(block.Bytes); err2 == nil {
		var ok bool
		rsaKey, ok = anyKey.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("github app private key is not RSA")
		}
	} else {
		return nil, fmt.Errorf("parse github app private key: %w", err)
	}
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	return &GitHubAppTokenProvider{
		appID:          strings.TrimSpace(appID),
		installationID: strings.TrimSpace(installationID),
		key:            rsaKey,
		baseURL:        strings.TrimRight(baseURL, "/"),
		httpClient:     &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// NewGitHubAppTokenProviderFromFile reads the private key PEM from keyPath.
func NewGitHubAppTokenProviderFromFile(appID, installationID, keyPath, baseURL string) (*GitHubAppTokenProvider, error) {
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read github app key %s: %w", keyPath, err)
	}
	return NewGitHubAppTokenProvider(appID, installationID, pemBytes, baseURL)
}

// Token returns a cached installation token or mints a fresh one when within 5
// minutes of expiry.
func (p *GitHubAppTokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached != "" && time.Now().Add(5*time.Minute).Before(p.expiresAt) {
		return p.cached, nil
	}

	jwtToken, err := p.signJWT(time.Now())
	if err != nil {
		return "", err
	}
	u := fmt.Sprintf("%s/app/installations/%s/access_tokens", p.baseURL, p.installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader([]byte("{}")))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "k8s-agents-session-hub")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("mint github app token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("mint github app token: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode github app token response: %w", err)
	}
	if out.Token == "" {
		return "", fmt.Errorf("github app token response missing token")
	}
	if out.ExpiresAt.IsZero() {
		out.ExpiresAt = time.Now().Add(55 * time.Minute)
	}
	p.cached = out.Token
	p.expiresAt = out.ExpiresAt
	return p.cached, nil
}

func (p *GitHubAppTokenProvider) signJWT(now time.Time) (string, error) {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(540 * time.Second).Unix(),
		"iss": p.appID,
	})
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := hdr + "." + payload
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign github app jwt: %w", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
