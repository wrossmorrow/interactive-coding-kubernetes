package hub_test

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

func buildAgentrunnerArchiveRequest(t *testing.T, token string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	metaHeader := make(textproto.MIMEHeader)
	metaHeader.Set("Content-Disposition", `form-data; name="metadata"; filename="metadata.json"`)
	metaHeader.Set("Content-Type", "application/json")
	mp, err := mw.CreatePart(metaHeader)
	if err != nil {
		t.Fatal(err)
	}
	metaJSON := map[string]any{
		"session_id":   "ar-review-1234",
		"session_kind": "review",
		"session_name": "wrossmorrow/k8s-agents#42",
		"repo":         "wrossmorrow/k8s-agents",
		"pr_number":    42,
		"run_id":       "987654321",
		"head_sha":     "deadbeef",
		"provider":     "gateway",
		"model":        "Qwen/Qwen3.8-27B",
		"mode":         "read-only",
		"output":       "pr-review",
		"verdict":      "REQUEST_CHANGES",
		"findings":     2,
		"turns":        4,
		"end_reason":   "closed",
		"started_at":   time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339),
		"ended_at":     time.Now().UTC().Format(time.RFC3339),
		"stats": map[string]any{
			"tokens":    map[string]any{"input": 1000, "output": 250},
			"toolCalls": 3,
		},
	}
	b, _ := json.Marshal(metaJSON)
	_, _ = mp.Write(b)

	jsonl := `{"type":"message","message":{"role":"user","content":[{"type":"text","text":"review PR #42"}]}}` + "\n" +
		`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"Found 2 issues."}]}}` + "\n"
	gz, err := sessions.GzipBytes([]byte(jsonl))
	if err != nil {
		t.Fatal(err)
	}
	trHeader := make(textproto.MIMEHeader)
	trHeader.Set("Content-Disposition", `form-data; name="transcript"; filename="transcript.jsonl.gz"`)
	trHeader.Set("Content-Type", "application/gzip")
	tp, err := mw.CreatePart(trHeader)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tp.Write(gz)
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/internal/archive", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-Session-Id", "ar-review-1234")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func TestAgentrunnerMultipartArchiveIngestAndReadOnlyEnforcement(t *testing.T) {
	store, err := sessions.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := hub.NewServer(hub.Config{
		Store:            store,
		ArchiveSinkToken: "sink-secret-token",
	})
	h := srv.Routes()

	// 1. Unauthorized upload -> 401.
	wBad := httptest.NewRecorder()
	h.ServeHTTP(wBad, buildAgentrunnerArchiveRequest(t, "wrong-token"))
	if wBad.Code != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d, want 401", wBad.Code)
	}

	// 2. Authorized upload -> 200.
	wOK := httptest.NewRecorder()
	h.ServeHTTP(wOK, buildAgentrunnerArchiveRequest(t, "sink-secret-token"))
	if wOK.Code != http.StatusOK {
		t.Fatalf("archive status = %d, want 200 (%s)", wOK.Code, wOK.Body.String())
	}

	// 3. Transcript is browsable with 0 running daemons.
	rTr := httptest.NewRequest(http.MethodGet, "/api/sessions/ar-review-1234/transcript", nil)
	rTr.Host = "localhost"
	wTr := httptest.NewRecorder()
	h.ServeHTTP(wTr, rTr)
	if wTr.Code != http.StatusOK {
		t.Fatalf("transcript status = %d: %s", wTr.Code, wTr.Body.String())
	}
	var tr struct {
		Status   sessions.Status   `json:"status"`
		Metadata sessions.Metadata `json:"metadata"`
		Messages []map[string]any  `json:"messages"`
	}
	if err := json.Unmarshal(wTr.Body.Bytes(), &tr); err != nil {
		t.Fatal(err)
	}
	if tr.Status != sessions.StatusClosed || tr.Metadata.SessionKind != sessions.KindReview || tr.Metadata.Verdict != "REQUEST_CHANGES" || tr.Metadata.Findings != 2 {
		t.Fatalf("unexpected archived metadata: %+v", tr.Metadata)
	}
	if len(tr.Messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(tr.Messages))
	}
	if !tr.Metadata.SessionKind.IsAutomated() {
		t.Fatal("review kind must be treated as automated")
	}

	// 4. Attempting to rejoin an automated CI session -> 409 Conflict.
	rRejoin := httptest.NewRequest(http.MethodPost, "/api/sessions/ar-review-1234/rejoin", bytes.NewReader([]byte("{}")))
	rRejoin.Host = "localhost"
	rRejoin.Header.Set("Content-Type", "application/json")
	wRejoin := httptest.NewRecorder()
	h.ServeHTTP(wRejoin, rRejoin)
	if wRejoin.Code != http.StatusConflict {
		t.Fatalf("rejoin automated session status = %d, want 409", wRejoin.Code)
	}
}
