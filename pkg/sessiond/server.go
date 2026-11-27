// Package sessiond implements the headless per-sandbox HTTP/SSE daemon that
// supervises a single `pi --mode rpc` process, ships JSONL parts and
// workspace.bundle archives to session-hub, and handles pause vs. close.
package sessiond

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/pirpc"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
	"github.com/wrossmorrow/k8s-agents/pkg/workspace"
)

var allowedCommands = setOf(
	"prompt", "steer", "follow_up", "abort",
	"get_state", "get_messages", "get_session_stats", "get_commands",
	"get_available_models", "set_model", "cycle_model",
	"get_available_thinking_levels", "set_thinking_level", "cycle_thinking_level",
	"set_steering_mode", "set_follow_up_mode",
	"compact", "set_auto_compaction", "set_auto_retry", "abort_retry",
	"bash", "abort_bash",
	"set_session_name", "get_last_assistant_text",
	"get_fork_messages", "fork", "clone",
)

var longCommands = setOf("bash", "compact")

func setOf(items ...string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, it := range items {
		m[it] = true
	}
	return m
}

// Config configures a headless sessiond Server.
type Config struct {
	SessionID   string
	SessionKind sessions.Kind
	SessionFile string
	WorkDir     string
	Toolchain   string
	AuthToken   string
	BuildSHA    string
	Store       sessions.Store
	Uploader    *sessions.Uploader
	OnShutdown  func()
}

// Server serves the headless per-sandbox HTTP/SSE API consumed by session-hub.
type Server struct {
	rpc        *pirpc.Client
	cfg        Config
	startedAt  time.Time
	onShutdown func()

	lifecycleMu sync.Mutex
	finalized   bool
}

// NewServer constructs a headless sessiond Server.
func NewServer(rpc *pirpc.Client, cfg Config) *Server {
	if cfg.SessionKind == "" {
		cfg.SessionKind = sessions.KindInteractive
	}
	if cfg.Uploader == nil {
		cfg.Uploader = sessions.NewUploader(cfg.Store, time.Hour, sessions.DefaultMaxPartBytes, nil)
	}
	s := &Server{
		rpc:        rpc,
		cfg:        cfg,
		startedAt:  time.Now(),
		onShutdown: cfg.OnShutdown,
	}
	cfg.Uploader.SetSource(s.SessionRef)
	return s
}

// SessionRef builds a live sessions.Ref from the running pi RPC process and workspace.
func (s *Server) SessionRef(ctx context.Context) (*sessions.Ref, error) {
	var state map[string]any
	var stats map[string]any
	path := s.cfg.SessionFile
	if s.rpc != nil && !s.rpc.IsDead() {
		if st, err := s.rpc.State(ctx); err == nil && st != nil {
			state = st
			if p, _ := st["sessionFile"].(string); p != "" {
				path = p
			}
		}
		if ss, err := s.rpc.Stats(ctx); err == nil {
			stats = ss
		}
	}
	id := s.cfg.SessionID
	if id == "" && path != "" {
		id = sessions.SessionIDFromFile(path)
	}
	return &sessions.Ref{
		SessionID:   id,
		SessionKind: s.cfg.SessionKind,
		Path:        path,
		State:       state,
		Stats:       stats,
		Git:         workspace.Inspect(ctx, s.cfg.WorkDir),
		Toolchain:   s.cfg.Toolchain,
		ClientHost:  clientHost(),
	}, nil
}

// Routes returns the headless daemon HTTP handler (no embedded UI).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/info", s.handleInfo)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/state", s.passthrough("get_state"))
	mux.HandleFunc("GET /api/stats", s.passthrough("get_session_stats"))
	mux.HandleFunc("POST /api/cmd", s.handleCommand)
	mux.HandleFunc("POST /api/extension-ui/response", s.handleExtensionUIResponse)
	mux.HandleFunc("POST /api/flush", s.handleFlush)
	mux.HandleFunc("POST /api/pause", s.handlePause)
	mux.HandleFunc("POST /api/close", s.handleClose)
	return s.authMiddleware(mux)
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" || s.cfg.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.AuthToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	status := http.StatusOK
	if s.rpc == nil || s.rpc.IsDead() {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": status == http.StatusOK})
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	gitSt := workspace.Inspect(r.Context(), s.cfg.WorkDir)
	info := map[string]any{
		"sessionId":     s.cfg.SessionID,
		"sessionName":   "",
		"sessionKind":   s.cfg.SessionKind,
		"status":        s.cfg.Uploader.StatusOf(s.cfg.SessionID),
		"cwd":           s.cfg.WorkDir,
		"repo":          gitSt.Repo,
		"branch":        gitSt.Branch,
		"headSha":       gitSt.HeadSHA,
		"buildSha":      s.cfg.BuildSHA,
		"startedAt":     s.startedAt.UTC().Format(time.RFC3339),
		"uptimeSeconds": int64(time.Since(s.startedAt) / time.Second),
		"subscribers":   s.rpc.SubscriberCount(),
		"closed":        s.cfg.Uploader.IsClosed(s.cfg.SessionID),
	}
	if ref, err := s.SessionRef(r.Context()); err == nil && ref != nil {
		if ref.SessionID != "" {
			info["sessionId"] = ref.SessionID
		}
		info["sessionName"] = sessions.SessionNameFromState(ref.State)
		info["status"] = s.cfg.Uploader.StatusOf(ref.SessionID)
		info["closed"] = s.cfg.Uploader.IsClosed(ref.SessionID)
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) passthrough(command string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.run(w, r, map[string]any{"type": command})
	}
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	var cmd map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20)).Decode(&cmd); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "bad request: " + err.Error()})
		return
	}
	t, _ := cmd["type"].(string)
	if !allowedCommands[t] {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": fmt.Sprintf("command %q not allowed", t)})
		return
	}
	delete(cmd, "id")
	s.run(w, r, cmd)
}

func isUserTurnCommand(t string) bool {
	switch t {
	case "prompt", "steer", "follow_up":
		return true
	default:
		return false
	}
}

func (s *Server) run(w http.ResponseWriter, r *http.Request, cmd map[string]any) {
	if t, ok := cmd["type"].(string); ok && isUserTurnCommand(t) {
		st := s.cfg.Uploader.StatusOf(s.cfg.SessionID)
		if st == sessions.StatusClosed || st == sessions.StatusPaused {
			writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": fmt.Sprintf("session %s", st)})
			return
		}
	}
	timeout := pirpc.DefaultCommandTimeout
	if t, _ := cmd["type"].(string); longCommands[t] {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	res, err := s.rpc.Send(ctx, cmd)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.Uploader.Enabled() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": "session store not configured"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), pirpc.DefaultCommandTimeout)
	defer cancel()
	ref, err := s.SessionRef(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": err.Error()})
		return
	}
	st, err := s.cfg.Uploader.Flush(ctx, ref)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"sessionId":   st.SessionKey,
		"finalOffset": st.LastOffset,
		"partCount":   st.PartCount,
		"hash":        st.HashHex,
		"shipped":     st.Shipped,
	})
}

// Pause flushes pending JSONL lines, uploads workspace.bundle if dirty/unpushed,
// sets metadata status="paused" (never _closed), and broadcasts session_paused.
func (s *Server) Pause(ctx context.Context) (*sessions.State, error) {
	s.lifecycleMu.Lock()
	if s.finalized {
		s.lifecycleMu.Unlock()
		return &sessions.State{SessionKey: s.cfg.SessionID, Status: s.cfg.Uploader.StatusOf(s.cfg.SessionID)}, nil
	}
	s.finalized = true
	s.lifecycleMu.Unlock()

	ref, err := s.SessionRef(ctx)
	if err != nil {
		ref = &sessions.Ref{
			SessionID:   s.cfg.SessionID,
			SessionKind: s.cfg.SessionKind,
			Path:        s.cfg.SessionFile,
			Git:         workspace.Inspect(ctx, s.cfg.WorkDir),
			Toolchain:   s.cfg.Toolchain,
		}
	}

	if s.cfg.Store != nil && s.cfg.WorkDir != "" {
		bundle, gitSt, berr := workspace.SaveBundle(ctx, s.cfg.WorkDir)
		if berr != nil {
			log.Printf("session %s workspace bundle warning: %v", ref.SessionID, berr)
		} else {
			ref.Git = gitSt
			if len(bundle) > 0 {
				if perr := s.cfg.Store.Put(ctx, sessions.WorkspaceBundleKey(ref.SessionID), bundle, sessions.BundleContentType); perr != nil {
					log.Printf("session %s workspace bundle upload warning: %v", ref.SessionID, perr)
				}
			}
		}
	}

	st, perr := s.cfg.Uploader.Pause(ctx, ref)
	if s.rpc != nil {
		s.rpc.Broadcast(pirpc.Event{"type": "session_paused", "sessionId": ref.SessionID})
	}
	return st, perr
}

// Close finalizes the session: flushes pending JSONL, writes _closed, composes
// compacted.jsonl.gz, and broadcasts session_closed.
func (s *Server) Close(ctx context.Context, endReason string) (*sessions.State, error) {
	s.lifecycleMu.Lock()
	if s.finalized {
		s.lifecycleMu.Unlock()
		return &sessions.State{SessionKey: s.cfg.SessionID, Status: s.cfg.Uploader.StatusOf(s.cfg.SessionID)}, nil
	}
	s.finalized = true
	s.lifecycleMu.Unlock()

	ref, err := s.SessionRef(ctx)
	if err != nil {
		ref = &sessions.Ref{
			SessionID:   s.cfg.SessionID,
			SessionKind: s.cfg.SessionKind,
			Path:        s.cfg.SessionFile,
			Git:         workspace.Inspect(ctx, s.cfg.WorkDir),
			Toolchain:   s.cfg.Toolchain,
		}
	}
	st, cerr := s.cfg.Uploader.CloseSession(ctx, ref, endReason)
	if s.rpc != nil {
		s.rpc.Broadcast(pirpc.Event{"type": "session_closed", "sessionId": ref.SessionID, "endReason": endReason})
	}
	return st, cerr
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	st, err := s.Pause(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"sessionId":   st.SessionKey,
		"status":      st.Status,
		"finalOffset": st.LastOffset,
		"partCount":   st.PartCount,
	})
	if s.onShutdown != nil {
		go s.onShutdown()
	}
}

func (s *Server) handleClose(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	st, err := s.Close(ctx, "closed")
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"sessionId":   st.SessionKey,
		"status":      st.Status,
		"finalOffset": st.LastOffset,
		"partCount":   st.PartCount,
	})
	if s.onShutdown != nil {
		go s.onShutdown()
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "retry: 1000\n\n")
	flusher.Flush()

	ch, unsub := s.rpc.Subscribe()
	defer unsub()
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
		drain:
			for drained := 0; drained < 256; drained++ {
				select {
				case ev, ok := <-ch:
					if !ok {
						flusher.Flush()
						return
					}
					if data, err := json.Marshal(ev); err == nil {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
					}
				default:
					break drain
				}
			}
			flusher.Flush()
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleExtensionUIResponse(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID        string  `json:"id"`
		Value     *string `json:"value"`
		Confirmed *bool   `json:"confirmed"`
		Cancelled bool    `json:"cancelled"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20)).Decode(&req); err != nil || req.ID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "bad request"})
		return
	}
	if !s.rpc.DialogOpen(req.ID) {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "unknown dialog"})
		return
	}
	response := map[string]any{"id": req.ID}
	switch {
	case req.Cancelled:
		response["cancelled"] = true
	case req.Confirmed != nil:
		response["confirmed"] = *req.Confirmed
	case req.Value != nil:
		response["value"] = *req.Value
	default:
		response["cancelled"] = true
	}
	if err := s.rpc.RespondDialog(response); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func clientHost() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
