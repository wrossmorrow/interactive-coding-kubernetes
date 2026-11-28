package workspace

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// EnsureCloned clones repo (owner/name) into dir when dir does not yet contain
// a .git directory. It prefers `gh repo clone` (which automatically picks up
// the github-auth sidecar token via the gh shim) and configures git auth via
// `gh auth setup-git` so subsequent `git push` / `git fetch` calls work out of
// the box. If branch is non-empty and exists on origin, it checks it out.
func EnsureCloned(ctx context.Context, dir, repo, branch string) error {
	repo = strings.TrimSpace(repo)
	if dir == "" || repo == "" {
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create workdir %s: %w", dir, err)
	}

	if ghPath, err := exec.LookPath("gh"); err == nil {
		cmd := exec.CommandContext(ctx, ghPath, "repo", "clone", repo, dir)
		if out, cerr := cmd.CombinedOutput(); cerr != nil {
			return fmt.Errorf("gh repo clone %s: %w: %s", repo, cerr, strings.TrimSpace(string(out)))
		}
		// Configure repo-local git credential helper to invoke the gh wrapper on PATH
		// (e.g. /usr/local/bin/gh) rather than `gh auth setup-git` (which resolves
		// os.Executable() to /usr/local/lib/gh/gh and bypasses the /tokens/token shim)
		// without mutating the caller's global ~/.gitconfig in local dev mode.
		helper := "!" + ghPath + " auth git-credential"
		if err := gitRun(ctx, dir, "config", "--local", "--add", "credential.https://github.com.helper", ""); err != nil {
			return fmt.Errorf("reset git credential helper: %w", err)
		}
		if err := gitRun(ctx, dir, "config", "--local", "--add", "credential.https://github.com.helper", helper); err != nil {
			return fmt.Errorf("configure git credential helper: %w", err)
		}
	} else {
		url := repo
		if !strings.Contains(repo, "://") && !strings.HasPrefix(repo, "git@") {
			url = "https://github.com/" + strings.TrimPrefix(repo, "/") + ".git"
		}
		cmd := exec.CommandContext(ctx, "git", "clone", url, dir)
		if out, cerr := cmd.CombinedOutput(); cerr != nil {
			return fmt.Errorf("git clone %s: %w: %s", url, cerr, strings.TrimSpace(string(out)))
		}
	}

	branch = strings.TrimSpace(branch)
	if branch != "" {
		if err := gitRun(ctx, dir, "checkout", branch); err != nil {
			// Branch may not exist on origin yet (e.g. only in workspace.bundle); ignore.
			_ = err
		}
	}
	return nil
}
