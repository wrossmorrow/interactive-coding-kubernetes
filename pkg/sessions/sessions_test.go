package sessions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/workspace"
)

const testSessionID = "019dbbc7-1111-2222-3333-444455556666"

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSessionIDFromFileAndPartKey(t *testing.T) {
	if got := SessionIDFromFile("/home/node/.pi/agent/sessions/2026-05-06T12-00-00-000Z_" + testSessionID + ".jsonl"); got != testSessionID {
		t.Fatalf("SessionIDFromFile = %q, want %q", got, testSessionID)
	}
	if got := PartKey(testSessionID, 42); got != "sessions/"+testSessionID+"/parts/part-000000000042.jsonl.gz" {
		t.Fatalf("PartKey = %q", got)
	}
	if off, ok := ParsePartOffset(PartKey(testSessionID, 42)); !ok || off != 42 {
		t.Fatalf("ParsePartOffset = (%d, %v), want (42, true)", off, ok)
	}
}

func TestPauseResumeOffsetAndCloseCompose(t *testing.T) {
	ctx := context.Background()
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	line1 := `{"type":"message","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}` + "\n"
	path := writeFile(t, workDir, testSessionID+".jsonl", line1)

	u1 := NewUploader(store, time.Hour, DefaultMaxPartBytes, nil)
	ref := &Ref{
		SessionID: testSessionID,
		Path:      path,
		State:     map[string]any{"sessionName": "first-run"},
		Git:       workspace.GitState{Repo: "org/repo", Branch: "feat/a", HeadSHA: "abc123"},
	}

	// Flush part 0, then Pause (must NOT write _closed).
	st, err := u1.Flush(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if st.PartCount != 1 || st.LastOffset != int64(len(line1)) {
		t.Fatalf("after flush: %+v", st)
	}
	pst, err := u1.Pause(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if pst.Status != StatusPaused {
		t.Fatalf("after pause status = %q, want paused", pst.Status)
	}
	if _, err := store.Get(ctx, ClosedKey(testSessionID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pause must NOT write _closed, got err=%v", err)
	}

	// Simulate a new daemon process rejoining the same session.
	line2 := `{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"world"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line1+line2), 0o644); err != nil {
		t.Fatal(err)
	}
	u2 := NewUploader(store, time.Hour, DefaultMaxPartBytes, nil)
	hst, err := u2.Hydrate(ctx, testSessionID, path)
	if err != nil {
		t.Fatal(err)
	}
	if hst.LastOffset != int64(len(line1)) || hst.PartCount != 1 {
		t.Fatalf("hydrated state = %+v, want lastOffset=%d partCount=1", hst, len(line1))
	}

	fst, err := u2.Flush(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if fst.PartCount != 2 || fst.LastOffset != int64(len(line1)+len(line2)) {
		t.Fatalf("after resumed flush: %+v", fst)
	}
	// Verify both part-000000000000 and part-<len(line1)> exist intact.
	if _, err := store.Get(ctx, PartKey(testSessionID, 0)); err != nil {
		t.Fatalf("part 0 missing: %v", err)
	}
	if _, err := store.Get(ctx, PartKey(testSessionID, int64(len(line1)))); err != nil {
		t.Fatalf("resumed part missing: %v", err)
	}

	// Close writes _closed and composes compacted.jsonl.gz.
	cst, err := u2.CloseSession(ctx, ref, "closed")
	if err != nil {
		t.Fatal(err)
	}
	if cst.Status != StatusClosed || !u2.IsClosed(testSessionID) {
		t.Fatalf("expected closed state, got %+v", cst)
	}
	cb, err := store.Get(ctx, ClosedKey(testSessionID))
	if err != nil {
		t.Fatalf("_closed missing: %v", err)
	}
	var cr ClosedReport
	if err := json.Unmarshal(cb, &cr); err != nil || cr.Status != "ok" || cr.PartCount != 2 {
		t.Fatalf("unexpected _closed report: %+v (err=%v)", cr, err)
	}
	if _, err := store.Get(ctx, CompactedKey(testSessionID)); err != nil {
		t.Fatalf("compacted.jsonl.gz missing: %v", err)
	}

	// ReadTranscript from compacted.jsonl.gz returns both messages.
	msgs, err := ReadTranscript(ctx, store, testSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[0]["role"] != "user" || msgs[1]["role"] != "assistant" {
		t.Fatalf("unexpected transcript messages: %+v", msgs)
	}
}

func TestUploaderSplitsLargeParts(t *testing.T) {
	ctx := context.Background()
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := "{\"a\":1}\n{\"b\":22}\n{\"c\":333}\n"
	path := writeFile(t, t.TempDir(), testSessionID+".jsonl", content)
	u := NewUploader(store, time.Hour, 20, nil)
	st, err := u.Flush(ctx, &Ref{SessionID: testSessionID, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if st.PartCount != 2 || st.LastOffset != int64(len(content)) {
		t.Fatalf("got partCount=%d lastOffset=%d, want 2 and %d", st.PartCount, st.LastOffset, len(content))
	}
}
