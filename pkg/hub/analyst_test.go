package hub_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

func TestAnalystReadOnlyAPIAndSecurityBoundaries(t *testing.T) {
	store, err := sessions.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spawner := &fakeSpawner{endpoint: "http://127.0.0.1:9999", running: map[string]bool{}}

	var issuedAnalystTok string
	var issuedInteractiveTok string
	srv := hub.NewServer(hub.Config{
		Store:   store,
		Spawner: spawner,
		TokenLookup: func(id string) (string, bool) {
			if id == "sess-coding-1" {
				return issuedInteractiveTok, issuedInteractiveTok != ""
			}
			return "", false
		},
		AnalystTokenLookup: func(id string) (string, bool) {
			if id == "sess-analyst-1" {
				return issuedAnalystTok, issuedAnalystTok != ""
			}
			return "", false
		},
	})
	issuedInteractiveTok = "interactive-write-tok"
	issuedAnalystTok = "an_sess-analyst-1.readonlytok"

	// Seed one interactive session and one archived CI review session.
	m1 := sessions.Metadata{
		SessionID:   "sess-coding-1",
		SessionName: "fix-auth",
		SessionKind: sessions.KindInteractive,
		Status:      sessions.StatusPaused,
		Repo:        "wrossmorrow/k8s-agents",
		PRNumber:    7,
		ModelConfig: map[string]any{"provider": "gateway", "modelId": "Qwen/Qwen3.8-27B"},
		UpdatedAt:   "2026-11-27T10:00:00Z",
	}
	b1, _ := json.Marshal(m1)
	_ = store.Put(context.Background(), sessions.MetadataKey(m1.SessionID), b1, sessions.JSONContentType)
	gz1, _ := sessions.GzipBytes([]byte(`{"type":"message","message":{"role":"user","content":[{"type":"text","text":"please fix auth retry"}]}}` + "\n"))
	_ = store.Put(context.Background(), sessions.PartKey(m1.SessionID, 0), gz1, sessions.PartContentType)

	m2 := sessions.Metadata{
		SessionID:   "sess-review-2",
		SessionName: "wrossmorrow/k8s-agents#7",
		SessionKind: sessions.KindReview,
		Status:      sessions.StatusClosed,
		Repo:        "wrossmorrow/k8s-agents",
		PRNumber:    7,
		Verdict:     "REQUEST_CHANGES",
		ModelConfig: map[string]any{"provider": "gateway", "modelId": "gemini-3.8-flash"},
		UpdatedAt:   "2026-11-27T11:00:00Z",
	}
	b2, _ := json.Marshal(m2)
	_ = store.Put(context.Background(), sessions.MetadataKey(m2.SessionID), b2, sessions.JSONContentType)
	gz2, _ := sessions.GzipBytes([]byte(`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"Found TOCTOU lock bug in handleUpdateSettings"}]}}` + "\n"))
	_ = store.Put(context.Background(), sessions.CompactedKey(m2.SessionID), gz2, sessions.PartContentType)

	h := srv.Routes()

	// 1. Security: interactive session token MUST NOT access /internal/analyst/*.
	rUnauth := httptest.NewRequest(http.MethodGet, "/internal/analyst/sessions", nil)
	rUnauth.Header.Set("Authorization", "Bearer "+issuedInteractiveTok)
	wUnauth := httptest.NewRecorder()
	h.ServeHTTP(wUnauth, rUnauth)
	if wUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("interactive token on analyst API = %d, want 401", wUnauth.Code)
	}

	// 2. Security: analyst token MUST NOT write to another session's objects.
	rWrite := httptest.NewRequest(http.MethodPut, "/internal/sessions/sess-coding-1/objects/metadata.json", strings.NewReader(`{"status":"closed"}`))
	rWrite.Header.Set("Authorization", "Bearer "+issuedAnalystTok)
	wWrite := httptest.NewRecorder()
	h.ServeHTTP(wWrite, rWrite)
	if wWrite.Code != http.StatusUnauthorized {
		t.Fatalf("analyst token on session write = %d, want 401", wWrite.Code)
	}

	// 3. Analyst list with filters (kind=review, pr=7, verdict=REQUEST_CHANGES).
	rList := httptest.NewRequest(http.MethodGet, "/internal/analyst/sessions?kind=review&pr=7&verdict=REQUEST_CHANGES", nil)
	rList.Header.Set("Authorization", "Bearer "+issuedAnalystTok)
	wList := httptest.NewRecorder()
	h.ServeHTTP(wList, rList)
	if wList.Code != http.StatusOK {
		t.Fatalf("analyst list status = %d: %s", wList.Code, wList.Body.String())
	}
	var listed struct {
		Sessions []sessions.Metadata `json:"sessions"`
	}
	_ = json.Unmarshal(wList.Body.Bytes(), &listed)
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "sess-review-2" {
		t.Fatalf("unexpected analyst list: %+v", listed.Sessions)
	}

	// 4. Analyst transcript fetch (parsed + raw JSONL).
	rTr := httptest.NewRequest(http.MethodGet, "/internal/analyst/sessions/sess-review-2/transcript", nil)
	rTr.Header.Set("Authorization", "Bearer "+issuedAnalystTok)
	wTr := httptest.NewRecorder()
	h.ServeHTTP(wTr, rTr)
	if wTr.Code != http.StatusOK || !strings.Contains(wTr.Body.String(), "TOCTOU lock bug") {
		t.Fatalf("analyst transcript = %d: %s", wTr.Code, wTr.Body.String())
	}

	rRaw := httptest.NewRequest(http.MethodGet, "/internal/analyst/sessions/sess-review-2/transcript?format=raw", nil)
	rRaw.Header.Set("Authorization", "Bearer "+issuedAnalystTok)
	wRaw := httptest.NewRecorder()
	h.ServeHTTP(wRaw, rRaw)
	if wRaw.Code != http.StatusOK || !strings.Contains(wRaw.Body.String(), `"role":"assistant"`) {
		t.Fatalf("analyst raw transcript = %d: %s", wRaw.Code, wRaw.Body.String())
	}

	// 5. Analyst search across transcripts.
	rSearch := httptest.NewRequest(http.MethodGet, "/internal/analyst/search?q=toctou", nil)
	rSearch.Header.Set("Authorization", "Bearer "+issuedAnalystTok)
	wSearch := httptest.NewRecorder()
	h.ServeHTTP(wSearch, rSearch)
	if wSearch.Code != http.StatusOK {
		t.Fatalf("analyst search status = %d: %s", wSearch.Code, wSearch.Body.String())
	}
	var sr struct {
		Hits []struct {
			SessionID string   `json:"session_id"`
			Snippets  []string `json:"snippets"`
		} `json:"hits"`
	}
	_ = json.Unmarshal(wSearch.Body.Bytes(), &sr)
	if len(sr.Hits) != 1 || sr.Hits[0].SessionID != "sess-review-2" || len(sr.Hits[0].Snippets) == 0 {
		t.Fatalf("unexpected search hits: %+v", sr.Hits)
	}

	// 6. Rejoining an already-running analyst session must not rotate/break its token.
	createBody := `{"kind":"analyst","name":"audit-run","toolchain":"agent"}`
	rCreate := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(createBody))
	rCreate.Host = "127.0.0.1:8787"
	rCreate.Header.Set("Content-Type", "application/json")
	wCreate := httptest.NewRecorder()
	h.ServeHTTP(wCreate, rCreate)
	if wCreate.Code != http.StatusCreated {
		t.Fatalf("create analyst session = %d: %s", wCreate.Code, wCreate.Body.String())
	}
	var created sessions.Metadata
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)
	origTok := spawner.lastSpec.AnalystToken
	if origTok == "" {
		t.Fatal("expected analyst token on initial Spawn")
	}

	rRejoin := httptest.NewRequest(http.MethodPost, "/api/sessions/"+created.SessionID+"/rejoin", nil)
	rRejoin.Host = "127.0.0.1:8787"
	rRejoin.Header.Set("Content-Type", "application/json")
	wRejoin := httptest.NewRecorder()
	h.ServeHTTP(wRejoin, rRejoin)
	if wRejoin.Code != http.StatusOK {
		t.Fatalf("rejoin running analyst session = %d: %s", wRejoin.Code, wRejoin.Body.String())
	}

	// Original analyst token must still be accepted after rejoin-while-running.
	rCheck := httptest.NewRequest(http.MethodGet, "/internal/analyst/sessions", nil)
	rCheck.Header.Set("Authorization", "Bearer "+origTok)
	wCheck := httptest.NewRecorder()
	h.ServeHTTP(wCheck, rCheck)
	if wCheck.Code != http.StatusOK {
		t.Fatalf("original analyst token rejected after rejoin-while-running: %d %s", wCheck.Code, wCheck.Body.String())
	}
}
