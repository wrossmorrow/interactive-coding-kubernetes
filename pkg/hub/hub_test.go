package hub_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/piconfig"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
	"github.com/wrossmorrow/k8s-agents/ui"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func buildSessiondBin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pi-sessiond")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/pi-sessiond")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pi-sessiond: %v\n%s", err, string(out))
	}
	return bin
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

// TestEndToEndCreateLeavePauseBrowseRejoinCloseFork exercises the full
// Milestone 1 lifecycle locally with session-hub + LocalSpawner + pi-sessiond +
// fake-pi.mjs + DirStore.
func TestEndToEndCreateLeavePauseBrowseRejoinCloseFork(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available in PATH")
	}
	t.Setenv("FAKE_PI_SPEED", "0")

	sessiondBin := buildSessiondBin(t)
	store, err := sessions.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workRoot := t.TempDir()
	fakePi := filepath.Join(repoRoot(t), "testdata", "fake-pi.mjs")

	var hubSrv *hub.Server
	spawner := hub.NewLocalSpawner(hub.LocalSpawnerConfig{
		SessiondBin:      sessiondBin,
		WorkRoot:         workRoot,
		PiBin:            "node",
		PiArgs:           fakePi,
		FlushIntervalSec: 60,
		OnExit: func(id string) {
			if hubSrv != nil {
				hubSrv.NotifyExit(id)
			}
		},
	})
	defer func() { _ = spawner.CloseAll() }()

	hubSrv = hub.NewServer(hub.Config{
		Store:   store,
		Spawner: spawner,
		Catalog: piconfig.DefaultCatalog(),
		UIFS:    ui.FS(),
	})
	ts := httptest.NewServer(hubSrv.Routes())
	defer ts.Close()
	hubSrv.SetHubBaseURL(ts.URL)

	// 1. CREATE a new interactive session.
	resp := postJSON(t, ts.URL+"/api/sessions", map[string]any{
		"name":      "e2e-session",
		"repo":      "wrossmorrow/k8s-agents",
		"toolchain": "golang",
	})
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status = %d: %s", resp.StatusCode, string(b))
	}
	var created sessions.Metadata
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	id := created.SessionID
	if id == "" || created.Status != sessions.StatusActive || !spawner.Running(id) {
		t.Fatalf("unexpected created session: %+v", created)
	}

	// 2. ATTACH SSE, send prompt, wait for agent_settled, then LEAVE (disconnect SSE).
	sseCtx, sseCancel := context.WithTimeout(context.Background(), 15*time.Second)
	sseReq, _ := http.NewRequestWithContext(sseCtx, http.MethodGet, ts.URL+"/s/"+id+"/api/events", nil)
	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatal(err)
	}
	settledCh := make(chan struct{})
	go func() {
		defer func() { _ = sseResp.Body.Close() }()
		sc := bufio.NewScanner(sseResp.Body)
		for sc.Scan() {
			if strings.Contains(sc.Text(), `"agent_settled"`) {
				close(settledCh)
				return
			}
		}
	}()

	cmdResp := postJSON(t, ts.URL+"/s/"+id+"/api/cmd", map[string]any{"type": "prompt", "message": "hello first turn"})
	if cmdResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(cmdResp.Body)
		t.Fatalf("prompt 1 status = %d: %s", cmdResp.StatusCode, string(b))
	}
	_ = cmdResp.Body.Close()

	select {
	case <-settledCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for agent_settled on turn 1")
	}
	// Leave (disconnect SSE).
	sseCancel()

	// Create an uncommitted file in the session's scratch git workspace to verify workspace.bundle save/restore.
	dirtyFile := filepath.Join(workRoot, id, "uncommitted.txt")
	if err := os.WriteFile(dirtyFile, []byte("saved across pause\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 3. PAUSE the session -> flushes part 0, saves workspace.bundle, exits daemon (0 running daemons).
	pauseResp := postJSON(t, ts.URL+"/api/sessions/"+id+"/pause", map[string]any{})
	if pauseResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(pauseResp.Body)
		t.Fatalf("pause status = %d: %s", pauseResp.StatusCode, string(b))
	}
	var paused sessions.Metadata
	if err := json.NewDecoder(pauseResp.Body).Decode(&paused); err != nil {
		t.Fatal(err)
	}
	_ = pauseResp.Body.Close()
	if paused.Status != sessions.StatusPaused || spawner.Running(id) {
		t.Fatalf("expected paused with 0 running daemons, got status=%q running=%v", paused.Status, spawner.Running(id))
	}
	if _, err := store.Get(context.Background(), sessions.ClosedKey(id)); err != sessions.ErrNotFound {
		t.Fatalf("pause must NOT write _closed, got err=%v", err)
	}
	if _, err := store.Get(context.Background(), sessions.WorkspaceBundleKey(id)); err != nil {
		t.Fatalf("expected workspace.bundle saved on pause: %v", err)
	}

	// 4. BROWSE read-only transcript with 0 running daemons.
	trResp, err := http.Get(ts.URL + "/api/sessions/" + id + "/transcript")
	if err != nil {
		t.Fatal(err)
	}
	var tr struct {
		Status   sessions.Status  `json:"status"`
		Messages []map[string]any `json:"messages"`
	}
	if err := json.NewDecoder(trResp.Body).Decode(&tr); err != nil {
		t.Fatal(err)
	}
	_ = trResp.Body.Close()
	if tr.Status != sessions.StatusPaused || len(tr.Messages) < 2 {
		t.Fatalf("unexpected paused transcript: status=%q messages=%d", tr.Status, len(tr.Messages))
	}

	// Delete the local uncommitted file to prove Rejoin restores it from workspace.bundle.
	if err := os.Remove(dirtyFile); err != nil {
		t.Fatal(err)
	}

	// 5. REJOIN -> spawns fresh pi-sessiond, restores workspace.bundle + JSONL + lastOffset.
	rejoinResp := postJSON(t, ts.URL+"/api/sessions/"+id+"/rejoin", map[string]any{})
	if rejoinResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(rejoinResp.Body)
		t.Fatalf("rejoin status = %d: %s", rejoinResp.StatusCode, string(b))
	}
	_ = rejoinResp.Body.Close()

	restoredBytes, err := os.ReadFile(dirtyFile)
	if err != nil || string(restoredBytes) != "saved across pause\n" {
		t.Fatalf("workspace.bundle did not restore uncommitted.txt: %q (err=%v)", string(restoredBytes), err)
	}

	// Send turn 2 and verify a second part is appended without overwriting part-000000000000.
	sseCtx2, sseCancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer sseCancel2()
	sseReq2, _ := http.NewRequestWithContext(sseCtx2, http.MethodGet, ts.URL+"/s/"+id+"/api/events", nil)
	sseResp2, err := http.DefaultClient.Do(sseReq2)
	if err != nil {
		t.Fatal(err)
	}
	settledCh2 := make(chan struct{})
	go func() {
		defer func() { _ = sseResp2.Body.Close() }()
		sc := bufio.NewScanner(sseResp2.Body)
		for sc.Scan() {
			if strings.Contains(sc.Text(), `"agent_settled"`) {
				close(settledCh2)
				return
			}
		}
	}()

	cmdResp2 := postJSON(t, ts.URL+"/s/"+id+"/api/cmd", map[string]any{"type": "prompt", "message": "hello second turn"})
	if cmdResp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(cmdResp2.Body)
		t.Fatalf("prompt 2 status = %d: %s", cmdResp2.StatusCode, string(b))
	}
	_ = cmdResp2.Body.Close()
	select {
	case <-settledCh2:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for agent_settled on turn 2")
	}

	// 6. CLOSE -> writes _closed + composes compacted.jsonl.gz with both turns.
	closeResp := postJSON(t, ts.URL+"/api/sessions/"+id+"/close", map[string]any{})
	if closeResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(closeResp.Body)
		t.Fatalf("close status = %d: %s", closeResp.StatusCode, string(b))
	}
	_ = closeResp.Body.Close()

	parts, err := store.List(context.Background(), sessions.PartsPrefix(id))
	if err != nil || len(parts) < 2 {
		t.Fatalf("expected >= 2 distinct parts after pause+rejoin, got %v (err=%v)", parts, err)
	}
	if _, err := store.Get(context.Background(), sessions.CompactedKey(id)); err != nil {
		t.Fatalf("expected compacted.jsonl.gz on close: %v", err)
	}

	// Rejoining a closed session must fail with 409 Conflict.
	rejoinClosed := postJSON(t, ts.URL+"/api/sessions/"+id+"/rejoin", map[string]any{})
	_ = rejoinClosed.Body.Close()
	if rejoinClosed.StatusCode != http.StatusConflict {
		t.Fatalf("rejoin closed status = %d, want 409", rejoinClosed.StatusCode)
	}

	// 7. FORK the closed session into a new active session.
	forkResp := postJSON(t, ts.URL+"/api/sessions/"+id+"/fork", map[string]any{})
	if forkResp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(forkResp.Body)
		t.Fatalf("fork status = %d: %s", forkResp.StatusCode, string(b))
	}
	var forked sessions.Metadata
	if err := json.NewDecoder(forkResp.Body).Decode(&forked); err != nil {
		t.Fatal(err)
	}
	_ = forkResp.Body.Close()
	if forked.ForkedFrom != id || forked.Status != sessions.StatusActive {
		t.Fatalf("unexpected forked metadata: %+v", forked)
	}
}
