package sessiond

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wrossmorrow/k8s-agents/pkg/pirpc"
)

func TestBearerAuthMiddleware(t *testing.T) {
	s := NewServer(pirpc.NewClient("", nil, nil, ""), Config{
		SessionID: "sess-1",
		AuthToken: "secret-tok",
	})
	h := s.Routes()

	// Health does not require auth (503 because fake rpc isn't started, not 401).
	rHealth := httptest.NewRequest("GET", "/api/health", nil)
	wHealth := httptest.NewRecorder()
	h.ServeHTTP(wHealth, rHealth)
	if wHealth.Code == http.StatusUnauthorized {
		t.Fatalf("health should not require auth, got 401")
	}

	// Info without token -> 401.
	rNoAuth := httptest.NewRequest("GET", "/api/info", nil)
	wNoAuth := httptest.NewRecorder()
	h.ServeHTTP(wNoAuth, rNoAuth)
	if wNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("info without auth = %d, want 401", wNoAuth.Code)
	}

	// Info with token -> 200.
	rAuth := httptest.NewRequest("GET", "/api/info", nil)
	rAuth.Header.Set("Authorization", "Bearer secret-tok")
	wAuth := httptest.NewRecorder()
	h.ServeHTTP(wAuth, rAuth)
	if wAuth.Code != http.StatusOK {
		t.Fatalf("info with auth = %d, want 200 (%s)", wAuth.Code, wAuth.Body.String())
	}
}

func TestCommandAllowlistRejectsExportHTML(t *testing.T) {
	s := NewServer(pirpc.NewClient("", nil, nil, ""), Config{SessionID: "sess-1"})
	r := httptest.NewRequest("POST", "/api/cmd", strings.NewReader(`{"type":"export_html","outputPath":"/tmp/x"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Routes().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "not allowed") {
		t.Fatalf("got %d %s", w.Code, w.Body.String())
	}
}
