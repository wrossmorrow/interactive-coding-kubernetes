// Package workspace saves and restores compact git workspace bundles so
// interactive sessions can pause to zero and rehydrate uncommitted or unpushed
// changes cleanly.
package workspace

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GitState summarizes the git repository metadata captured alongside a bundle.
type GitState struct {
	Repo    string `json:"repo,omitempty"`
	Branch  string `json:"branch,omitempty"`
	HeadSHA string `json:"head_sha,omitempty"`
	Dirty   bool   `json:"dirty,omitempty"`
}

type bundleManifest struct {
	Branch      string   `json:"branch"`
	HeadSHA     string   `json:"head_sha"`
	HasGitPack  bool     `json:"has_git_pack"`
	HasStaged   bool     `json:"has_staged"`
	HasUnstaged bool     `json:"has_unstaged"`
	Untracked   []string `json:"untracked,omitempty"`
}

// Inspect returns the current git repo, branch, HEAD SHA, and dirty flag for dir.
func Inspect(ctx context.Context, dir string) GitState {
	st := GitState{}
	if dir == "" || !isGitRepo(ctx, dir) {
		return st
	}
	if out, err := gitOut(ctx, dir, "remote", "get-url", "origin"); err == nil {
		st.Repo = parseRepoName(out)
	}
	if out, err := gitOut(ctx, dir, "rev-parse", "--abbrev-ref", "HEAD"); err == nil && out != "" && out != "HEAD" {
		st.Branch = out
	}
	if out, err := gitOut(ctx, dir, "rev-parse", "HEAD"); err == nil {
		st.HeadSHA = out
	}
	if out, err := gitOut(ctx, dir, "status", "--porcelain"); err == nil && out != "" {
		st.Dirty = true
	}
	return st
}

// SaveBundle packages unpushed commits (via `git bundle create`), staged and
// unstaged diffs, and untracked non-ignored files into a gzipped tar archive.
// If dir is not a git repo, or has no commits/changes to preserve, it returns
// (nil, state, nil).
func SaveBundle(ctx context.Context, dir string) ([]byte, GitState, error) {
	st := Inspect(ctx, dir)
	if dir == "" || !isGitRepo(ctx, dir) {
		return nil, st, nil
	}

	var (
		gitBundleBytes []byte
		stagedPatch    []byte
		unstagedPatch  []byte
		untrackedFiles []string
	)

	// Capture commits on HEAD (since upstream if configured, else full HEAD)
	// when HEAD exists.
	if st.HeadSHA != "" {
		revRange := "HEAD"
		if upstream, err := gitOut(ctx, dir, "rev-parse", "--abbrev-ref", "@{upstream}"); err == nil && upstream != "" {
			if ahead, err := gitOut(ctx, dir, "rev-list", "--count", upstream+"..HEAD"); err == nil && ahead != "0" {
				revRange = upstream + "..HEAD"
			} else {
				revRange = ""
			}
		}
		if revRange != "" {
			tmpBundle := filepath.Join(os.TempDir(), fmt.Sprintf("k8s-agents-%d.bundle", os.Getpid()))
			defer func() { _ = os.Remove(tmpBundle) }()
			if err := gitRun(ctx, dir, "bundle", "create", tmpBundle, revRange); err == nil {
				if b, rerr := os.ReadFile(tmpBundle); rerr == nil && len(b) > 0 {
					gitBundleBytes = b
				}
			}
		}
	}

	if out, err := gitRawOut(ctx, dir, "diff", "--cached", "--binary"); err == nil && len(out) > 0 {
		stagedPatch = out
	}
	if out, err := gitRawOut(ctx, dir, "diff", "--binary"); err == nil && len(out) > 0 {
		unstagedPatch = out
	}
	if out, err := gitOut(ctx, dir, "ls-files", "--others", "--exclude-standard"); err == nil && out != "" {
		for _, line := range strings.Split(out, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				untrackedFiles = append(untrackedFiles, line)
			}
		}
	}

	if len(gitBundleBytes) == 0 && len(stagedPatch) == 0 && len(unstagedPatch) == 0 && len(untrackedFiles) == 0 {
		return nil, st, nil
	}

	manifest := bundleManifest{
		Branch:      st.Branch,
		HeadSHA:     st.HeadSHA,
		HasGitPack:  len(gitBundleBytes) > 0,
		HasStaged:   len(stagedPatch) > 0,
		HasUnstaged: len(unstagedPatch) > 0,
		Untracked:   untrackedFiles,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return nil, st, err
	}

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	if err := writeTarBytes(tw, "manifest.json", manifestJSON, 0o644); err != nil {
		return nil, st, err
	}
	if len(gitBundleBytes) > 0 {
		if err := writeTarBytes(tw, "commits.bundle", gitBundleBytes, 0o644); err != nil {
			return nil, st, err
		}
	}
	if len(stagedPatch) > 0 {
		if err := writeTarBytes(tw, "staged.patch", stagedPatch, 0o644); err != nil {
			return nil, st, err
		}
	}
	if len(unstagedPatch) > 0 {
		if err := writeTarBytes(tw, "unstaged.patch", unstagedPatch, 0o644); err != nil {
			return nil, st, err
		}
	}
	for _, rel := range untrackedFiles {
		full := filepath.Join(dir, rel)
		info, err := os.Stat(full)
		if err != nil || info.IsDir() {
			continue
		}
		data, err := os.ReadFile(full)
		if err != nil {
			return nil, st, fmt.Errorf("read untracked %s: %w", rel, err)
		}
		if err := writeTarBytes(tw, "untracked/"+filepath.ToSlash(rel), data, int64(info.Mode().Perm())); err != nil {
			return nil, st, err
		}
	}

	if err := tw.Close(); err != nil {
		return nil, st, err
	}
	if err := gw.Close(); err != nil {
		return nil, st, err
	}
	return buf.Bytes(), st, nil
}

// RestoreBundle unpacks a workspace.bundle archive into dir, fetching commits
// (if any), applying staged/unstaged patches, and restoring untracked files.
func RestoreBundle(ctx context.Context, dir string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("open workspace bundle gzip: %w", err)
	}
	defer func() { _ = gr.Close() }()

	tr := tar.NewReader(gr)
	var (
		manifest      bundleManifest
		gitBundle     []byte
		stagedPatch   []byte
		unstagedPatch []byte
		untracked     = map[string][]byte{}
	)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read workspace bundle tar: %w", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		switch {
		case hdr.Name == "manifest.json":
			if err := json.Unmarshal(body, &manifest); err != nil {
				return fmt.Errorf("parse workspace manifest: %w", err)
			}
		case hdr.Name == "commits.bundle":
			gitBundle = body
		case hdr.Name == "staged.patch":
			stagedPatch = body
		case hdr.Name == "unstaged.patch":
			unstagedPatch = body
		case strings.HasPrefix(hdr.Name, "untracked/"):
			rel := strings.TrimPrefix(hdr.Name, "untracked/")
			clean := filepath.Clean(rel)
			if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
				return fmt.Errorf("unsafe untracked path %q", rel)
			}
			untracked[clean] = body
		}
	}

	if len(gitBundle) > 0 && isGitRepo(ctx, dir) {
		tmpBundle, err := os.CreateTemp("", "k8s-agents-restore-*.bundle")
		if err != nil {
			return err
		}
		tmpPath := tmpBundle.Name()
		defer func() { _ = os.Remove(tmpPath) }()
		if _, err := tmpBundle.Write(gitBundle); err != nil {
			_ = tmpBundle.Close()
			return err
		}
		_ = tmpBundle.Close()

		branch := manifest.Branch
		if branch == "" {
			branch = "restored"
		}
		if err := gitRun(ctx, dir, "fetch", tmpPath, "HEAD:refs/heads/"+branch); err == nil {
			_ = gitRun(ctx, dir, "checkout", branch)
		} else if manifest.HeadSHA != "" {
			if err := gitRun(ctx, dir, "fetch", tmpPath, "HEAD"); err == nil {
				_ = gitRun(ctx, dir, "checkout", "-B", branch, "FETCH_HEAD")
			}
		}
	}

	if len(stagedPatch) > 0 && isGitRepo(ctx, dir) {
		if err := applyPatch(ctx, dir, stagedPatch, "--index"); err != nil {
			return fmt.Errorf("apply staged patch: %w", err)
		}
	}
	if len(unstagedPatch) > 0 && isGitRepo(ctx, dir) {
		if err := applyPatch(ctx, dir, unstagedPatch); err != nil {
			return fmt.Errorf("apply unstaged patch: %w", err)
		}
	}
	for rel, content := range untracked {
		dst := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, content, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func applyPatch(ctx context.Context, dir string, patch []byte, extraArgs ...string) error {
	args := append([]string{"-C", dir, "apply"}, extraArgs...)
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Stdin = bytes.NewReader(patch)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func writeTarBytes(tw *tar.Writer, name string, data []byte, mode int64) error {
	hdr := &tar.Header{
		Name: name,
		Mode: mode,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func isGitRepo(ctx context.Context, dir string) bool {
	out, err := gitOut(ctx, dir, "rev-parse", "--is-inside-work-tree")
	return err == nil && out == "true"
}

func gitOut(ctx context.Context, dir string, args ...string) (string, error) {
	b, err := gitRawOut(ctx, dir, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func gitRawOut(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	return cmd.Output()
}

func gitRun(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func parseRepoName(raw string) string {
	u := strings.TrimSpace(raw)
	u = strings.TrimSuffix(u, ".git")
	if i := strings.Index(u, "github.com:"); i >= 0 {
		return u[i+len("github.com:"):]
	}
	if i := strings.Index(u, "github.com/"); i >= 0 {
		return u[i+len("github.com/"):]
	}
	if i := strings.LastIndex(u, "/"); i >= 0 {
		return u[i+1:]
	}
	return u
}
