// Command pi-sessiond runs the headless per-sandbox daemon managing a single
// `pi --mode rpc` session, writing model-agnostic Agent Router models.json,
// shipping JSONL parts and workspace.bundle to session-hub over HTTP, and
// pausing cleanly on /api/pause, idle timeout, or SIGTERM.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/piconfig"
	"github.com/wrossmorrow/k8s-agents/pkg/pirpc"
	"github.com/wrossmorrow/k8s-agents/pkg/sessiond"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
	"github.com/wrossmorrow/k8s-agents/pkg/workspace"
)

func main() {
	sessionID := envOr("SESSION_ID", sessions.NewID())
	sessionName := os.Getenv("SESSION_NAME")
	sessionKind := sessions.Kind(envOr("SESSION_KIND", string(sessions.KindInteractive)))
	addr := envOr("PI_SESSIOND_ADDR", "127.0.0.1:8787")
	workDir := envOr("PI_WORK_DIR", mustGetwd())
	agentDir := envOr("PI_AGENT_DIR", filepath.Join(os.TempDir(), "pi-agent-"+sessionID))
	hubURL := strings.TrimSpace(os.Getenv("SESSION_HUB_URL"))
	hubToken := strings.TrimSpace(os.Getenv("SESSION_HUB_TOKEN"))
	authToken := envOr("PI_SESSIOND_TOKEN", hubToken)
	toolchain := os.Getenv("TOOLCHAIN")
	flushInterval := time.Duration(envIntOr("PI_FLUSH_INTERVAL_SECONDS", 30)) * time.Second
	idleTimeout := time.Duration(envIntOr("PI_IDLE_PAUSE_SECONDS", 0)) * time.Second

	var store sessions.Store
	if hubURL != "" {
		store = sessions.NewHTTPStore(hubURL, sessionID, hubToken)
	}

	// Prepare agent config dir (models.json + AGENTS.md) and session file path.
	sessionsDir := filepath.Join(agentDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		log.Fatalf("create sessions dir: %v", err)
	}
	sessionFile := filepath.Join(sessionsDir, sessionID+".jsonl")

	cat := piconfig.DefaultCatalog()
	if catPath := os.Getenv("PI_MODEL_CATALOG"); catPath != "" {
		loaded, err := piconfig.LoadCatalog(catPath)
		if err != nil {
			log.Fatalf("load model catalog: %v", err)
		}
		cat = loaded
	}
	if gw := strings.TrimSpace(os.Getenv("AGENT_GATEWAY_URL")); gw != "" {
		cat.GatewayEndpoint = gw
	}
	var agentsMD string
	if agentsPath := os.Getenv("PI_AGENTS_MD"); agentsPath != "" {
		if b, err := os.ReadFile(agentsPath); err == nil {
			agentsMD = string(b)
		}
	}
	if err := piconfig.WriteConfigDir(agentDir, piconfig.WriteOptions{
		Catalog:      cat,
		GatewayToken: os.Getenv("AGENT_GATEWAY_TOKEN"),
		AgentsMD:     agentsMD,
		Attribution: piconfig.Attribution{
			SessionID:   sessionID,
			SessionName: sessionName,
			SessionKind: string(sessionKind),
			GitHubRepo:  os.Getenv("GITHUB_REPO"),
			GitHubPR:    envIntOr("GITHUB_PR", 0),
		},
	}); err != nil {
		log.Fatalf("write pi config dir: %v", err)
	}

	uploader := sessions.NewUploader(store, flushInterval, sessions.DefaultMaxPartBytes, nil)

	// Clone the target GitHub repo (if configured and not yet present) before
	// rehydrating workspace.bundle so unpushed commits and dirty edits apply
	// cleanly on top of the cloned repository. Analyst sessions have zero repo access.
	if sessionKind != sessions.KindAnalyst {
		if repo := strings.TrimSpace(os.Getenv("GITHUB_REPO")); repo != "" {
			cloneCtx, cloneCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if err := workspace.EnsureCloned(cloneCtx, workDir, repo, os.Getenv("GITHUB_BRANCH")); err != nil {
				log.Printf("clone repo %s warning: %v", repo, err)
			}
			cloneCancel()
		}
	}

	// Rehydrate existing transcript + workspace.bundle + upload offset if this
	// session is being rejoined or forked.
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if raw, err := sessions.ReadRawJSONL(ctx, store, sessionID); err == nil && len(raw) > 0 {
			if err := os.WriteFile(sessionFile, raw, 0o600); err != nil {
				log.Printf("rehydrate session file warning: %v", err)
			}
		}
		if bundle, err := store.Get(ctx, sessions.WorkspaceBundleKey(sessionID)); err == nil && len(bundle) > 0 {
			if err := workspace.RestoreBundle(ctx, workDir, bundle); err != nil {
				log.Printf("restore workspace.bundle warning: %v", err)
			}
		}
		if _, err := uploader.Hydrate(ctx, sessionID, sessionFile); err != nil {
			log.Printf("hydrate uploader warning: %v", err)
		}
		cancel()
	}

	piBin := envOr("PI_BIN", "pi")
	var piArgs []string
	if rawArgs := strings.TrimSpace(os.Getenv("PI_ARGS")); rawArgs != "" {
		piArgs = strings.Fields(rawArgs)
	} else {
		piArgs = []string{"--mode", "rpc"}
		if extPath := envOr("PI_EXTENSION_PATH", ""); extPath != "" {
			piArgs = append(piArgs, "--extension", extPath)
		}
		if provider := envOr("PI_PROVIDER", cat.DefaultProvider); provider != "" {
			piArgs = append(piArgs, "--provider", provider)
		}
		if model := envOr("PI_MODEL", cat.DefaultModel); model != "" {
			piArgs = append(piArgs, "--model", model)
		}
	}
	piArgs = append(piArgs, "--session", sessionFile)
	if sessionName != "" {
		piArgs = append(piArgs, "--name", sessionName)
	}

	// Scrub hub/gateway/cloud tokens before exec'ing pi so bash tool calls
	// inside the sandbox cannot read them. Analyst sessions receive ONLY their
	// separate read-only SESSION_ANALYST_URL / SESSION_ANALYST_TOKEN.
	extraEnv := map[string]string{
		"PI_CODING_AGENT_DIR": agentDir,
		"SESSION_ID":          sessionID,
		"SESSION_NAME":        sessionName,
		"SESSION_KIND":        string(sessionKind),
	}
	if sessionKind == sessions.KindAnalyst {
		if u := strings.TrimSpace(os.Getenv("SESSION_ANALYST_URL")); u != "" {
			extraEnv["SESSION_ANALYST_URL"] = u
		}
		if t := strings.TrimSpace(os.Getenv("SESSION_ANALYST_TOKEN")); t != "" {
			extraEnv["SESSION_ANALYST_TOKEN"] = t
		}
	}
	piEnv := piconfig.ScrubEnv(os.Environ(), extraEnv)

	rpc := pirpc.NewClient(piBin, piArgs, piEnv, workDir)
	if err := rpc.Start(); err != nil {
		log.Fatalf("start pi rpc: %v", err)
	}

	var srv *http.Server
	var shutdownOnce sync.Once
	triggerShutdown := func() {
		shutdownOnce.Do(func() {
			go func() {
				time.Sleep(150 * time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if srv != nil {
					_ = srv.Shutdown(ctx)
				}
				_ = rpc.Stop()
			}()
		})
	}

	d := sessiond.NewServer(rpc, sessiond.Config{
		SessionID:   sessionID,
		SessionKind: sessionKind,
		SessionFile: sessionFile,
		WorkDir:     workDir,
		Toolchain:   toolchain,
		AuthToken:   authToken,
		BuildSHA:    os.Getenv("PI_BUILD_SHA"),
		Store:       store,
		Uploader:    uploader,
		OnShutdown:  triggerShutdown,
	})

	if uploader.Enabled() {
		uploader.Start(context.Background())
	}
	stopIdle := d.StartIdleWatcher(context.Background(), idleTimeout)
	defer stopIdle()

	srv = &http.Server{
		Addr:    addr,
		Handler: d.Routes(),
	}

	// Handle SIGTERM/SIGINT -> Pause (resumable, NEVER writes _closed).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("pi-sessiond received %v; pausing session %s", sig, sessionID)
		pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := d.Pause(pctx); err != nil {
			log.Printf("pause on signal error: %v", err)
		}
		pcancel()
		triggerShutdown()
	}()

	// If pi exits unexpectedly, pause the session so work isn't locked or lost.
	go func() {
		err := rpc.Wait()
		if err != nil {
			log.Printf("pi rpc exited: %v", err)
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 20*time.Second)
		if _, perr := d.Pause(pctx); perr != nil {
			log.Printf("pause on child exit error: %v", perr)
		}
		pcancel()
		triggerShutdown()
	}()

	log.Printf("pi-sessiond listening on http://%s (session=%s)", addr, sessionID)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}
