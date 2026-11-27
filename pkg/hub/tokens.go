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
}

func NewTokenManager() *TokenManager {
	return &TokenManager{tokens: map[string]string{}}
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

// Get returns the current token for sessionID, if any.
func (m *TokenManager) Get(sessionID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tok, ok := m.tokens[sessionID]
	return tok, ok
}

// Verify checks whether token is valid for sessionID.
func (m *TokenManager) Verify(sessionID, token string) bool {
	if token == "" {
		return false
	}
	m.mu.RLock()
	want, ok := m.tokens[sessionID]
	m.mu.RUnlock()
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
