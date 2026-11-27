package workspace

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
}

func TestSaveAndRestoreBundle(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	runGit(t, src, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, src, "add", "hello.txt")
	runGit(t, src, "commit", "-m", "initial")

	// Create a feature branch with an unpushed commit, a staged edit, an unstaged edit, and an untracked file.
	runGit(t, src, "checkout", "-b", "feat/test")
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, src, "commit", "-am", "feat commit")

	if err := os.WriteFile(filepath.Join(src, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, src, "add", "staged.txt")

	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("v3-unstaged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "notes.md"), []byte("untracked note\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	bundle, st, err := SaveBundle(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle) == 0 {
		t.Fatal("expected non-empty bundle")
	}
	if st.Branch != "feat/test" || !st.Dirty || st.HeadSHA == "" {
		t.Fatalf("unexpected git state: %+v", st)
	}

	// Restore into a fresh empty git repo.
	dst := t.TempDir()
	runGit(t, dst, "init", "-b", "main")
	if err := RestoreBundle(ctx, dst, bundle); err != nil {
		t.Fatal(err)
	}

	gotHello, err := os.ReadFile(filepath.Join(dst, "hello.txt"))
	if err != nil || string(gotHello) != "v3-unstaged\n" {
		t.Fatalf("hello.txt = %q (err=%v), want v3-unstaged", string(gotHello), err)
	}
	gotStaged, err := os.ReadFile(filepath.Join(dst, "staged.txt"))
	if err != nil || string(gotStaged) != "staged\n" {
		t.Fatalf("staged.txt = %q (err=%v), want staged", string(gotStaged), err)
	}
	gotUntracked, err := os.ReadFile(filepath.Join(dst, "notes.md"))
	if err != nil || string(gotUntracked) != "untracked note\n" {
		t.Fatalf("notes.md = %q (err=%v), want untracked note", string(gotUntracked), err)
	}
	dstState := Inspect(ctx, dst)
	if dstState.Branch != "feat/test" || dstState.HeadSHA != st.HeadSHA {
		t.Fatalf("restored git state = %+v, want branch=%s sha=%s", dstState, st.Branch, st.HeadSHA)
	}
}
