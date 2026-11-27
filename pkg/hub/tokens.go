package hub

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"sync"
)

// TokenManager issues and verifies per-session scoped bearer tokens so each
// pi-sessiond sandbox can only access its own session's objects on session-hub.
type TokenManager struct {
	mu     sync.RWMutex
	tokens map[string]string // sessionID -> token
	lookup func(sessionID string) (string, bool)
}

func NewTokenManager() *TokenManager {
	return &TokenManager{tokens: map[string]string{}}
}

// SetLookup configures an optional durable fallback (e.g. reading a per-session
// Kubernetes Secret) so session-hub restarts seamlessly recover tokens for
// already-running sandbox pods.
func (m *TokenManager) SetLookup(fn func(sessionID string) (string, bool)) {
	m.mu.Lock()
	m.lookup = fn
	m.mu.Unlock()
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

// Revoke removes the active token for sessionID.
func (m *TokenManager) Revoke(sessionID string) {
	m.mu.Lock()
	delete(m.tokens, sessionID)
	m.mu.Unlock()
}
