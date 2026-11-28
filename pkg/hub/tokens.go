package hub

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"strings"
	"sync"
)

// TokenManager issues and verifies per-session scoped bearer tokens so each
// pi-sessiond sandbox can only access its own session's objects on session-hub.
// It also manages separate read-only analyst tokens for /internal/analyst/*.
type TokenManager struct {
	mu            sync.RWMutex
	tokens        map[string]string // sessionID -> write-scoped session token
	analystTokens map[string]string // sessionID -> read-only analyst token
	lookup        func(sessionID string) (string, bool)
	analystLookup func(sessionID string) (string, bool)
}

func NewTokenManager() *TokenManager {
	return &TokenManager{
		tokens:        map[string]string{},
		analystTokens: map[string]string{},
	}
}

// SetLookup configures an optional durable fallback (e.g. reading a per-session
// Kubernetes Secret) so session-hub restarts seamlessly recover tokens for
// already-running sandbox pods.
func (m *TokenManager) SetLookup(fn func(sessionID string) (string, bool)) {
	m.mu.Lock()
	m.lookup = fn
	m.mu.Unlock()
}

// SetAnalystLookup configures an optional durable fallback (e.g. direct Get of
// the per-session Kubernetes Secret by sessionID) for verifying read-only
// analyst tokens after a session-hub restart without requiring Secret list RBAC.
func (m *TokenManager) SetAnalystLookup(fn func(sessionID string) (string, bool)) {
	m.mu.Lock()
	m.analystLookup = fn
	m.mu.Unlock()
}

// IssueAnalyst returns a freshly generated read-only bearer token for an
// analyst session in the format "an_<sessionID>.<hex>". This token is distinct
// from the per-session write token and is accepted only on /internal/analyst/*
// read endpoints.
func (m *TokenManager) IssueAnalyst(sessionID string) string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	tok := "an_" + sessionID + "." + hex.EncodeToString(b)
	m.mu.Lock()
	m.analystTokens[sessionID] = tok
	m.mu.Unlock()
	return tok
}

func parseAnalystTokenSessionID(token string) (string, bool) {
	rest, ok := strings.CutPrefix(token, "an_")
	if !ok {
		return "", false
	}
	sessionID, secret, ok := strings.Cut(rest, ".")
	if !ok || sessionID == "" || secret == "" {
		return "", false
	}
	return sessionID, true
}

// VerifyAnalyst reports whether token is a valid read-only analyst token.
func (m *TokenManager) VerifyAnalyst(token string) bool {
	if token == "" {
		return false
	}
	m.mu.RLock()
	for _, want := range m.analystTokens {
		if subtle.ConstantTimeCompare([]byte(want), []byte(token)) == 1 {
			m.mu.RUnlock()
			return true
		}
	}
	lookup := m.analystLookup
	m.mu.RUnlock()
	if lookup != nil {
		if sessionID, ok := parseAnalystTokenSessionID(token); ok {
			if recovered, found := lookup(sessionID); found && recovered != "" {
				if subtle.ConstantTimeCompare([]byte(recovered), []byte(token)) == 1 {
					m.mu.Lock()
					m.analystTokens[sessionID] = recovered
					m.mu.Unlock()
					return true
				}
			}
		}
	}
	return false
}

// Issue returns a freshly generated bearer token bound exclusively to sessionID.
func (m *TokenManager) Issue(sessionID string) string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	tok := hex.EncodeToString(b)
	m.mu.Lock()
	m.tokens[sessionID] = tok
	m.mu.Unlock()
	return tok
}

// Get returns the current token for sessionID, falling back to the durable
// lookup (if configured) on cache miss.
func (m *TokenManager) Get(sessionID string) (string, bool) {
	m.mu.RLock()
	tok, ok := m.tokens[sessionID]
	lookup := m.lookup
	m.mu.RUnlock()
	if ok && tok != "" {
		return tok, true
	}
	if lookup != nil {
		if recovered, found := lookup(sessionID); found && recovered != "" {
			m.mu.Lock()
			m.tokens[sessionID] = recovered
			m.mu.Unlock()
			return recovered, true
		}
	}
	return "", false
}

// Verify checks whether token is valid for sessionID.
func (m *TokenManager) Verify(sessionID, token string) bool {
	if token == "" {
		return false
	}
	want, ok := m.Get(sessionID)
	if !ok || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(token)) == 1
}

// Reconcile syncs the in-memory token for sessionID to whatever the durable
// lookup (e.g. the per-session Kubernetes Secret) currently holds, so that
// after a concurrent Spawn reused or re-created the shared token secret,
// the hub verifies against the token the sandbox pod was actually started
// with. It returns false when no durable lookup is configured or holds a
// token.
func (m *TokenManager) Reconcile(sessionID string) bool {
	m.mu.RLock()
	lookup := m.lookup
	m.mu.RUnlock()
	if lookup == nil {
		return false
	}
	recovered, found := lookup(sessionID)
	if !found || recovered == "" {
		return false
	}
	m.mu.Lock()
	m.tokens[sessionID] = recovered
	m.mu.Unlock()
	return true
}

// Revoke removes the active tokens for sessionID.
func (m *TokenManager) Revoke(sessionID string) {
	m.mu.Lock()
	delete(m.tokens, sessionID)
	delete(m.analystTokens, sessionID)
	m.mu.Unlock()
}
