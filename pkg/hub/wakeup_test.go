package hub_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

type fakeReviewFetcher struct {
	mu      sync.Mutex
	calls   int
	etags   []string
	reviews []hub.PullReview
	etag    string
}

func (f *fakeReviewFetcher) ListReviews(_ context.Context, _ string, _ int, etag string) ([]hub.PullReview, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.etags = append(f.etags, etag)
	if etag != "" && etag == f.etag {
		return nil, f.etag, true, nil
	}
	return f.reviews, f.etag, false, nil
}

type fakeSpawner struct {
	mu       sync.Mutex
	endpoint string
	running  map[string]bool
	spawns   int
}

func (f *fakeSpawner) Spawn(_ context.Context, spec hub.SpawnSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spawns++
	f.running[spec.SessionID] = true
	return f.endpoint, nil
}

func (f *fakeSpawner) Endpoint(id string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running[id] {
		return f.endpoint, true
	}
	return "", false
}

func (f *fakeSpawner) Running(id string) bool {
	_, ok := f.Endpoint(id)
	return ok
}

func (f *fakeSpawner) WaitExit(_ context.Context, _ string) {}
func (f *fakeSpawner) Stop(_ context.Context, id string) error {
	f.mu.Lock()
	delete(f.running, id)
	f.mu.Unlock()
	return nil
}
func (f *fakeSpawner) CloseAll() error { return nil }

func TestParseAgentrunnerFooterAndReviewFiltering(t *testing.T) {
	body := "Please fix the race condition.\n\n<!-- agentrunner-session: 019dbbc7-aaaa-bbbb-cccc-111122223333 -->"
	if got := hub.ParseAgentrunnerSessionID(body); got != "019dbbc7-aaaa-bbbb-cccc-111122223333" {
		t.Fatalf("ParseAgentrunnerSessionID = %q", got)
	}

	// Clean APPROVE with only the footer must NOT wake.
	cleanApprove := hub.PullReview{ID: 10, State: "APPROVED", Body: "<!-- agentrunner-session: abc -->", CommitID: "sha1"}
	if hub.ShouldWakeForReview(cleanApprove, "sha1", 0) {
		t.Fatal("clean APPROVE should not trigger wake-up")
	}

	// REQUEST_CHANGES on matching SHA must wake.
	reqChanges := hub.PullReview{ID: 11, State: "CHANGES_REQUESTED", Body: body, CommitID: "sha1"}
	if !hub.ShouldWakeForReview(reqChanges, "sha1", 10) {
		t.Fatal("CHANGES_REQUESTED should trigger wake-up")
	}
	// Duplicate ID must not wake again.
	if hub.ShouldWakeForReview(reqChanges, "sha1", 11) {
		t.Fatal("already-processed review ID must not wake again")
	}
}

func TestWakeWatcherETagRejoinAndAntiPingPongCap(t *testing.T) {
	store, err := sessions.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Mock daemon recording injected wake-up prompts.
	var injected []string
	var injMu sync.Mutex
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var cmd map[string]any
		_ = json.NewDecoder(r.Body).Decode(&cmd)
		if msg, ok := cmd["message"].(string); ok {
			injMu.Lock()
			injected = append(injected, msg)
			injMu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer daemon.Close()

	spawner := &fakeSpawner{endpoint: daemon.URL, running: map[string]bool{}}
	srv := hub.NewServer(hub.Config{
		Store:   store,
		Spawner: spawner,
	})

	// Seed a paused interactive session linked to PR #42.
	meta := sessions.Metadata{
		SessionID:              "sess-wake-1",
		SessionName:            "fix-bug",
		SessionKind:            sessions.KindInteractive,
		Status:                 sessions.StatusPaused,
		Repo:                   "wrossmorrow/k8s-agents",
		PRNumber:               42,
		HeadSHA:                "abc123",
		WakePolicy:             sessions.WakePolicyEvaluateAndStage,
		MaxWakeupsPerHumanTurn: 1,
	}
	mb, _ := json.Marshal(meta)
	if err := store.Put(context.Background(), sessions.MetadataKey(meta.SessionID), mb, sessions.JSONContentType); err != nil {
		t.Fatal(err)
	}

	fetcher := &fakeReviewFetcher{
		etag: `"etag-1"`,
		reviews: []hub.PullReview{
			{
				ID:       101,
				State:    "CHANGES_REQUESTED",
				Body:     "Check error handling.\n<!-- agentrunner-session: ar-rev-99 -->",
				CommitID: "abc123",
			},
		},
	}
	watcher := hub.NewWakeWatcher(srv, fetcher, 0)

	// Poll 1: wakes paused session, injects evaluate-and-stage prompt, sets needs-attention + review link.
	watcher.PollOnce(context.Background())

	if spawner.spawns != 1 {
		t.Fatalf("spawns = %d, want 1", spawner.spawns)
	}
	injMu.Lock()
	gotCount := len(injected)
	gotPrompt := ""
	if gotCount > 0 {
		gotPrompt = injected[0]
	}
	injMu.Unlock()
	if gotCount != 1 || !strings.Contains(gotPrompt, "evaluate-and-stage") {
		t.Fatalf("unexpected injected prompts: %v", injected)
	}

	rawMeta, err := store.Get(context.Background(), sessions.MetadataKey(meta.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	var updated sessions.Metadata
	_ = json.Unmarshal(rawMeta, &updated)
	if updated.Status != sessions.StatusNeedsAttention || updated.WakeupsSinceHumanTurn != 1 || updated.LastReviewSessionID != "ar-rev-99" {
		t.Fatalf("unexpected updated metadata: %+v", updated)
	}

	// Poll 2 with another review ID: blocked by maxWakeupsPerHumanTurn (1/1) anti-ping-pong cap.
	fetcher.etag = `"etag-2"`
	fetcher.reviews = append(fetcher.reviews, hub.PullReview{
		ID:       102,
		State:    "CHANGES_REQUESTED",
		Body:     "Second review",
		CommitID: "abc123",
	})
	watcher.PollOnce(context.Background())

	injMu.Lock()
	if len(injected) != 1 {
		t.Fatalf("anti-ping-pong cap failed: injected %d prompts, want 1", len(injected))
	}
	injMu.Unlock()
}

func TestGitHubAppTokenProviderMintsAndCachesToken(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})

	var calls int
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/app/installations/999/access_tokens" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Fatalf("missing JWT bearer header")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "ghs_minted_123",
			"expires_at": time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339),
		})
	}))
	defer gh.Close()

	tp, err := hub.NewGitHubAppTokenProvider("12345", "999", pemBytes, gh.URL)
	if err != nil {
		t.Fatal(err)
	}
	tok1, err := tp.Token(context.Background())
	if err != nil || tok1 != "ghs_minted_123" {
		t.Fatalf("tok1 = (%q, %v)", tok1, err)
	}
	tok2, err := tp.Token(context.Background())
	if err != nil || tok2 != "ghs_minted_123" {
		t.Fatalf("tok2 = (%q, %v)", tok2, err)
	}
	if calls != 1 {
		t.Fatalf("expected cached token (1 HTTP call), got %d calls", calls)
	}
}

func TestDaemonMetadataFlushPreservesHubWakeupFields(t *testing.T) {
	store, err := sessions.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spawner := &fakeSpawner{endpoint: "http://127.0.0.1:9999", running: map[string]bool{"sess-merge-1": true}}
	srv := hub.NewServer(hub.Config{
		Store:   store,
		Spawner: spawner,
		TokenLookup: func(id string) (string, bool) {
			if id == "sess-merge-1" {
				return "tok-merge", true
			}
			return "", false
		},
	})

	// Seed hub metadata after a PR wake-up (needs-attention + counter=1 + review link).
	hubMeta := sessions.Metadata{
		SessionID:              "sess-merge-1",
		SessionKind:            sessions.KindInteractive,
		Status:                 sessions.StatusNeedsAttention,
		Repo:                   "wrossmorrow/k8s-agents",
		PRNumber:               42,
		WakePolicy:             sessions.WakePolicyEvaluateAndStage,
		MaxWakeupsPerHumanTurn: 1,
		WakeupsSinceHumanTurn:  1,
		LastProcessedReviewID:  555,
		LastReviewSessionID:    "ar-rev-555",
		WakeSummary:            "Woken by CHANGES_REQUESTED",
	}
	hb, _ := json.Marshal(hubMeta)
	if err := store.Put(context.Background(), sessions.MetadataKey("sess-merge-1"), hb, sessions.JSONContentType); err != nil {
		t.Fatal(err)
	}

	// Simulate pi-sessiond periodic flush sending stale status="active" and zeroed wake fields.
	daemonFlush := sessions.Metadata{
		SessionID:  "sess-merge-1",
		Status:     sessions.StatusActive,
		LastOffset: 2048,
		PartCount:  3,
	}
	db, _ := json.Marshal(daemonFlush)
	req := httptest.NewRequest(http.MethodPut, "/internal/sessions/sess-merge-1/objects/metadata.json", strings.NewReader(string(db)))
	req.Header.Set("Authorization", "Bearer tok-merge")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("daemon metadata PUT status = %d: %s", w.Code, w.Body.String())
	}

	gotBytes, err := store.Get(context.Background(), sessions.MetadataKey("sess-merge-1"))
	if err != nil {
		t.Fatal(err)
	}
	var got sessions.Metadata
	_ = json.Unmarshal(gotBytes, &got)
	if got.Status != sessions.StatusNeedsAttention || got.WakeupsSinceHumanTurn != 1 || got.LastProcessedReviewID != 555 || got.LastReviewSessionID != "ar-rev-555" || got.PRNumber != 42 {
		t.Fatalf("hub wake-up fields were clobbered by daemon flush: %+v", got)
	}
	if got.LastOffset != 2048 || got.PartCount != 3 {
		t.Fatalf("daemon upload progress fields were lost: %+v", got)
	}

	// Also verify negative max_wakeups_per_human_turn returns 400.
	badReq := httptest.NewRequest(http.MethodPost, "/api/sessions/sess-merge-1/settings", strings.NewReader(`{"max_wakeups_per_human_turn":-1}`))
	badReq.Host = "localhost"
	badReq.Header.Set("Content-Type", "application/json")
	badW := httptest.NewRecorder()
	srv.Routes().ServeHTTP(badW, badReq)
	if badW.Code != http.StatusBadRequest {
		t.Fatalf("negative max_wakeups_per_human_turn status = %d, want 400", badW.Code)
	}
}
