package hub

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

// SpawnSpec describes the runtime parameters for launching a pi-sessiond instance.
type SpawnSpec struct {
	SessionID   string
	SessionName string
	SessionKind sessions.Kind
	Repo        string
	Branch      string
	Toolchain   string
	Provider    string
	Model       string
	HubURL      string
	HubToken    string
}

// Spawner abstracts sandbox lifecycle management (LocalSpawner in Milestone 1,
// K8sPodSpawner in Milestone 2).
type Spawner interface {
	Spawn(ctx context.Context, spec SpawnSpec) (endpoint string, err error)
	Endpoint(sessionID string) (string, bool)
	Running(sessionID string) bool
	WaitExit(ctx context.Context, sessionID string)
	Stop(ctx context.Context, sessionID string) error
	CloseAll() error
}

// LocalSpawnerConfig configures LocalSpawner for local dev and E2E tests.
type LocalSpawnerConfig struct {
	SessiondBin      string
	WorkRoot         string
	PiBin            string
	PiArgs           string
	AgentsMDPath     string
	ModelCatalogPath string
	GatewayURL       string
	GatewayToken     string
	FlushIntervalSec int
	IdlePauseSec     int
	OnExit           func(sessionID string)
}

type localProc struct {
	endpoint string
	cmd      *exec.Cmd
	done     chan struct{}
}

// LocalSpawner runs pi-sessiond instances as local child processes on ephemeral
// loopback ports, each with its own scratch git repository under WorkRoot.
type LocalSpawner struct {
	cfg LocalSpawnerConfig

	mu    sync.RWMutex
	procs map[string]*localProc
}

func NewLocalSpawner(cfg LocalSpawnerConfig) *LocalSpawner {
	return &LocalSpawner{
		cfg:   cfg,
		procs: map[string]*localProc{},
	}
}

func (l *LocalSpawner) Spawn(ctx context.Context, spec SpawnSpec) (string, error) {
	l.mu.Lock()
	if existing, ok := l.procs[spec.SessionID]; ok {
		l.mu.Unlock()
		return existing.endpoint, nil
	}
	l.mu.Unlock()

	port, err := allocFreePort()
	if err != nil {
		return "", fmt.Errorf("alloc port: %w", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	endpoint := "http://" + addr

	workRoot, err := filepath.Abs(l.cfg.WorkRoot)
	if err != nil {
		return "", fmt.Errorf("resolve work root: %w", err)
	}
	workDir := filepath.Join(workRoot, spec.SessionID)
	if err := ensureScratchGitRepo(ctx, workDir); err != nil {
		return "", fmt.Errorf("prepare workdir: %w", err)
	}
	agentDir := filepath.Join(workRoot, ".agent-"+spec.SessionID)

	env := append([]string(nil), os.Environ()...)
	env = append(env,
		"SESSION_ID="+spec.SessionID,
		"SESSION_NAME="+spec.SessionName,
		"SESSION_KIND="+string(spec.SessionKind),
		"SESSION_HUB_URL="+spec.HubURL,
		"SESSION_HUB_TOKEN="+spec.HubToken,
		"PI_SESSIOND_TOKEN="+spec.HubToken,
		"PI_SESSIOND_ADDR="+addr,
		"PI_WORK_DIR="+workDir,
		"PI_AGENT_DIR="+agentDir,
		"TOOLCHAIN="+spec.Toolchain,
	)
	if l.cfg.PiBin != "" {
		env = append(env, "PI_BIN="+l.cfg.PiBin)
	}
	if l.cfg.PiArgs != "" {
		env = append(env, "PI_ARGS="+l.cfg.PiArgs)
	}
	if spec.Provider != "" {
		env = append(env, "PI_PROVIDER="+spec.Provider)
	}
	if spec.Model != "" {
		env = append(env, "PI_MODEL="+spec.Model)
	}
	if l.cfg.AgentsMDPath != "" {
		env = append(env, "PI_AGENTS_MD="+l.cfg.AgentsMDPath)
	}
	if l.cfg.ModelCatalogPath != "" {
		env = append(env, "PI_MODEL_CATALOG="+l.cfg.ModelCatalogPath)
	}
	if l.cfg.GatewayURL != "" {
		env = append(env, "AGENT_GATEWAY_URL="+l.cfg.GatewayURL)
	}
	if l.cfg.GatewayToken != "" {
		env = append(env, "AGENT_GATEWAY_TOKEN="+l.cfg.GatewayToken)
	}
	if l.cfg.FlushIntervalSec > 0 {
		env = append(env, fmt.Sprintf("PI_FLUSH_INTERVAL_SECONDS=%d", l.cfg.FlushIntervalSec))
	}
	if l.cfg.IdlePauseSec > 0 {
		env = append(env, fmt.Sprintf("PI_IDLE_PAUSE_SECONDS=%d", l.cfg.IdlePauseSec))
	}

	cmd := exec.Command(l.cfg.SessiondBin)
	cmd.Dir = workDir
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start pi-sessiond: %w", err)
	}

	proc := &localProc{
		endpoint: endpoint,
		cmd:      cmd,
		done:     make(chan struct{}),
	}
	l.mu.Lock()
	l.procs[spec.SessionID] = proc
	l.mu.Unlock()

	go func() {
		defer close(proc.done)
		if err := cmd.Wait(); err != nil {
			log.Printf("pi-sessiond (%s) exited: %v", spec.SessionID, err)
		}
		l.mu.Lock()
		if cur, ok := l.procs[spec.SessionID]; ok && cur == proc {
			delete(l.procs, spec.SessionID)
		}
		l.mu.Unlock()
		if l.cfg.OnExit != nil {
			l.cfg.OnExit(spec.SessionID)
		}
	}()

	if err := waitHealthy(ctx, endpoint+"/api/health", proc.done); err != nil {
		_ = l.Stop(context.Background(), spec.SessionID)
		return "", fmt.Errorf("pi-sessiond health check failed: %w", err)
	}
	return endpoint, nil
}

func (l *LocalSpawner) Endpoint(sessionID string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	p, ok := l.procs[sessionID]
	if !ok {
		return "", false
	}
	return p.endpoint, true
}

func (l *LocalSpawner) Running(sessionID string) bool {
	_, ok := l.Endpoint(sessionID)
	return ok
}

func (l *LocalSpawner) WaitExit(ctx context.Context, sessionID string) {
	l.mu.RLock()
	p, ok := l.procs[sessionID]
	l.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case <-p.done:
	case <-ctx.Done():
	}
}

func (l *LocalSpawner) Stop(ctx context.Context, sessionID string) error {
	l.mu.RLock()
	p, ok := l.procs[sessionID]
	l.mu.RUnlock()
	if !ok || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
		return nil
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		return nil
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
		return ctx.Err()
	}
}

func (l *LocalSpawner) CloseAll() error {
	l.mu.RLock()
	ids := make([]string, 0, len(l.procs))
	for id := range l.procs {
		ids = append(ids, id)
	}
	l.mu.RUnlock()
	for _, id := range ids {
		_ = l.Stop(context.Background(), id)
	}
	return nil
}

func allocFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitHealthy(ctx context.Context, url string, done <-chan struct{}) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-done:
			return fmt.Errorf("process exited before becoming healthy")
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for %s", url)
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func ensureScratchGitRepo(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		return nil
	}
	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Dev",
			"GIT_AUTHOR_EMAIL=dev@local",
			"GIT_COMMITTER_NAME=Dev",
			"GIT_COMMITTER_EMAIL=dev@local",
		)
		return cmd.Run()
	}
	if err := run("init", "-b", "main"); err != nil {
		return err
	}
	readme := filepath.Join(dir, "README.md")
	if _, err := os.Stat(readme); os.IsNotExist(err) {
		if err := os.WriteFile(readme, []byte("# Scratch Workspace\n"), 0o644); err != nil {
			return err
		}
	}
	if err := run("add", "README.md"); err != nil {
		return err
	}
	return run("commit", "-m", "initial scratch commit")
}
