// Package hub implements the session-hub control plane: session index,
// store-backed archive browsing (0 running daemons), scoped-token internal
// ingest API, live /s/<id>/* SSE/RPC reverse proxy, and sandbox spawner.
package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/piconfig"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

// Config configures a session-hub Server.
type Config struct {
	Store            sessions.Store
	Spawner          Spawner
	Catalog          piconfig.Catalog
	UIFS             fs.FS
	HubBaseURL       string // Internal URL passed to spawned pi-sessiond instances
	ArchiveSinkToken string // Shared bearer token for agentrunner POST /internal/archive
	AllowedHosts     []string
}

// Server is the session-hub HTTP control plane.
type Server struct {
	store            sessions.Store
	spawner          Spawner
	catalog          piconfig.Catalog
	uiFS             fs.FS
	hubBaseURL       string
	archiveSinkToken string
	allowedHosts     map[string]bool
	tokens           *TokenManager

	subsMu sync.Mutex
	subs   map[chan []byte]struct{}
}

// NewServer creates a session-hub Server.
func NewServer(cfg Config) *Server {
	hosts := map[string]bool{"localhost": true, "127.0.0.1": true, "::1": true}
	for _, h := range cfg.AllowedHosts {
		if h = strings.TrimSpace(h); h != "" {
			hosts[strings.ToLower(h)] = true
		}
	}
	if len(cfg.Catalog.Models) == 0 {
		cfg.Catalog = piconfig.DefaultCatalog()
	}
	return &Server{
		store:            cfg.Store,
		spawner:          cfg.Spawner,
		catalog:          cfg.Catalog,
		uiFS:             cfg.UIFS,
		hubBaseURL:       strings.TrimRight(cfg.HubBaseURL, "/"),
		archiveSinkToken: strings.TrimSpace(cfg.ArchiveSinkToken),
		allowedHosts:     hosts,
		tokens:           NewTokenManager(),
		subs:             map[chan []byte]struct{}{},
	}
}

// SetHubBaseURL updates the internal URL passed to spawned pi-sessiond processes
// (useful in tests when binding to an ephemeral port).
func (s *Server) SetHubBaseURL(u string) {
	s.hubBaseURL = strings.TrimRight(u, "/")
}

// NotifyExit should be called by the spawner when a session daemon exits so
// connected inbox clients see the updated paused/closed state promptly.
func (s *Server) NotifyExit(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if meta, err := s.loadMetadata(ctx, sessionID); err == nil && meta != nil {
		meta.Running = false
		s.broadcastSessionUpdate(meta)
	}
}

// Routes returns the complete HTTP handler for session-hub.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Health probe (exempt from browser Host guard so Kubernetes kubelet PodIP probes succeed).
	mux.HandleFunc("GET /api/health", s.handleHealth)

	// Internal sandbox ingest API + agentrunner multipart archive sink (bearer-authenticated).
	mux.HandleFunc("POST /internal/archive", s.handleInternalArchive)
	mux.HandleFunc("PUT /internal/sessions/{id}/objects/{path...}", s.handleInternalPutObject)
	mux.HandleFunc("GET /internal/sessions/{id}/objects/{path...}", s.handleInternalGetObject)
	mux.HandleFunc("GET /internal/sessions/{id}/objects", s.handleInternalListObjects)
	mux.HandleFunc("POST /internal/sessions/{id}/compose", s.handleInternalCompose)

	// Browser-facing control plane + live session proxy + decoupled SPA.
	pub := http.NewServeMux()
	pub.HandleFunc("GET /api/models", s.handleModels)
	pub.HandleFunc("GET /api/hub/events", s.handleHubEvents)
	pub.HandleFunc("GET /api/sessions", s.handleListSessions)
	pub.HandleFunc("POST /api/sessions", s.handleCreateSession)
	pub.HandleFunc("GET /api/sessions/{id}", s.handleGetSession)
	pub.HandleFunc("GET /api/sessions/{id}/transcript", s.handleGetTranscript)
	pub.HandleFunc("POST /api/sessions/{id}/rejoin", s.handleRejoinSession)
	pub.HandleFunc("POST /api/sessions/{id}/pause", s.handlePauseSession)
	pub.HandleFunc("POST /api/sessions/{id}/close", s.handleCloseSession)
	pub.HandleFunc("POST /api/sessions/{id}/fork", s.handleForkSession)

	// Live session proxy to active pi-sessiond.
	pub.HandleFunc("GET /s/{id}/{rest...}", s.handleSessionProxy)
	pub.HandleFunc("POST /s/{id}/{rest...}", s.handleSessionProxy)

	if s.uiFS != nil {
		pub.Handle("GET /", http.FileServer(http.FS(s.uiFS)))
	}

	mux.Handle("/", s.guard(pub))
	return mux
}

func (s *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.Trim(strings.ToLower(host), "[]")
		if !s.allowedHosts[host] {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || !strings.EqualFold(u.Host, r.Host) {
				http.Error(w, "forbidden origin", http.StatusForbidden)
				return
			}
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
				http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"defaultProvider": s.catalog.DefaultProvider,
		"defaultModel":    s.catalog.DefaultModel,
		"models":          s.catalog.Models,
	})
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	keys, err := s.store.List(r.Context(), "sessions/")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	kindFilter := strings.TrimSpace(r.URL.Query().Get("kind"))
	repoFilter := strings.TrimSpace(r.URL.Query().Get("repo"))

	list := make([]sessions.Metadata, 0)
	for _, k := range keys {
		if !strings.HasSuffix(k, "/metadata.json") {
			continue
		}
		b, err := s.store.Get(r.Context(), k)
		if err != nil {
			continue
		}
		var meta sessions.Metadata
		if err := json.Unmarshal(b, &meta); err != nil {
			continue
		}
		meta.Running = s.spawner != nil && s.spawner.Running(meta.SessionID)
		if meta.Status == sessions.StatusActive && !meta.Running {
			meta.Status = sessions.StatusPaused
		}
		if statusFilter != "" && string(meta.Status) != statusFilter {
			continue
		}
		if kindFilter != "" && string(meta.SessionKind) != kindFilter {
			continue
		}
		if repoFilter != "" && meta.Repo != repoFilter {
			continue
		}
		list = append(list, meta)
	}

	sort.Slice(list, func(i, j int) bool {
		ti := list[i].UpdatedAt
		if ti == "" {
			ti = list[i].StartedAt
		}
		tj := list[j].UpdatedAt
		if tj == "" {
			tj = list[j].StartedAt
		}
		return ti > tj
	})
	writeJSON(w, http.StatusOK, map[string]any{"sessions": list})
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string        `json:"name"`
		Repo      string        `json:"repo"`
		Branch    string        `json:"branch"`
		Toolchain string        `json:"toolchain"`
		Provider  string        `json:"provider"`
		Model     string        `json:"model"`
		Kind      sessions.Kind `json:"kind"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "bad request: " + err.Error()})
		return
	}
	if req.Kind == "" {
		req.Kind = sessions.KindInteractive
	}
	if req.Kind.IsAutomated() {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "cannot create automated ARC sessions via interactive API"})
		return
	}
	if req.Toolchain == "" {
		req.Toolchain = "agent"
	}
	if req.Provider == "" {
		req.Provider = s.catalog.DefaultProvider
	}
	if req.Model == "" {
		req.Model = s.catalog.DefaultModel
	}

	id := sessions.NewID()
	now := time.Now().UTC().Format(time.RFC3339)
	meta := &sessions.Metadata{
		SessionID:   id,
		SessionName: req.Name,
		SessionKind: req.Kind,
		Status:      sessions.StatusActive,
		Repo:        req.Repo,
		Branch:      req.Branch,
		Toolchain:   req.Toolchain,
		ModelConfig: map[string]any{"provider": req.Provider, "modelId": req.Model},
		StartedAt:   now,
		UpdatedAt:   now,
		Running:     true,
	}
	if err := s.saveMetadata(r.Context(), meta); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	tok := s.tokens.Issue(id)
	if _, err := s.spawner.Spawn(r.Context(), SpawnSpec{
		SessionID:   id,
		SessionName: req.Name,
		SessionKind: req.Kind,
		Repo:        req.Repo,
		Branch:      req.Branch,
		Toolchain:   req.Toolchain,
		Provider:    req.Provider,
		Model:       req.Model,
		HubURL:      s.hubBaseURL,
		HubToken:    tok,
	}); err != nil {
		meta.Status = sessions.StatusPaused
		meta.Running = false
		_ = s.saveMetadata(r.Context(), meta)
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusCreated, meta)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := s.loadMetadata(r.Context(), id)
	if err != nil {
		if errors.Is(err, sessions.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	meta.Running = s.spawner != nil && s.spawner.Running(id)
	if meta.Status == sessions.StatusActive && !meta.Running {
		meta.Status = sessions.StatusPaused
	}
	writeJSON(w, http.StatusOK, meta)
}

func (s *Server) handleGetTranscript(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := s.loadMetadata(r.Context(), id)
	if err != nil {
		if errors.Is(err, sessions.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	msgs, err := sessions.ReadTranscript(r.Context(), s.store, id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sessionId": id,
		"status":    meta.Status,
		"metadata":  meta,
		"messages":  msgs,
	})
}

func (s *Server) handleRejoinSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := s.loadMetadata(r.Context(), id)
	if err != nil {
		if errors.Is(err, sessions.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	if meta.Status == sessions.StatusClosed {
		writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": "cannot rejoin closed session; fork it instead"})
		return
	}
	if meta.SessionKind.IsAutomated() {
		writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": "cannot rejoin automated CI session; fork it instead"})
		return
	}
	if s.spawner.Running(id) {
		meta.Status = sessions.StatusActive
		meta.Running = true
		writeJSON(w, http.StatusOK, meta)
		return
	}

	provider, _ := meta.ModelConfig["provider"].(string)
	modelID, _ := meta.ModelConfig["modelId"].(string)
	if provider == "" {
		provider = s.catalog.DefaultProvider
	}
	if modelID == "" {
		modelID = s.catalog.DefaultModel
	}

	tok := s.tokens.Issue(id)
	if _, err := s.spawner.Spawn(r.Context(), SpawnSpec{
		SessionID:   id,
		SessionName: meta.SessionName,
		SessionKind: meta.SessionKind,
		Repo:        meta.Repo,
		Branch:      meta.Branch,
		Toolchain:   meta.Toolchain,
		Provider:    provider,
		Model:       modelID,
		HubURL:      s.hubBaseURL,
		HubToken:    tok,
	}); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}

	meta.Status = sessions.StatusActive
	meta.Running = true
	meta.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	_ = s.saveMetadata(r.Context(), meta)
	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusOK, meta)
}

func (s *Server) handlePauseSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ep, ok := s.spawner.Endpoint(id); ok {
		if err := s.postDaemon(r.Context(), id, ep+"/api/pause"); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
			return
		}
		waitCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		s.spawner.WaitExit(waitCtx, id)
		cancel()
	}
	meta, err := s.loadMetadata(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
		return
	}
	meta.Running = s.spawner.Running(id)
	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusOK, meta)
}

func (s *Server) handleCloseSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ep, ok := s.spawner.Endpoint(id); ok {
		if err := s.postDaemon(r.Context(), id, ep+"/api/close"); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
			return
		}
		waitCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		s.spawner.WaitExit(waitCtx, id)
		cancel()
	} else {
		// Session is already paused/stopped: finalize directly in hub without booting a pod.
		meta, err := s.loadMetadata(r.Context(), id)
		if err != nil {
			if errors.Is(err, sessions.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		meta.Status = sessions.StatusClosed
		meta.UpdatedAt = now
		meta.EndedAt = now
		meta.EndReason = "closed"
		if err := s.saveMetadata(r.Context(), meta); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		rep := sessions.ClosedReport{
			Status:      "ok",
			FinalOffset: meta.LastOffset,
			PartCount:   meta.PartCount,
			Hash:        meta.Hash,
		}
		cb, _ := json.Marshal(rep)
		_ = s.store.Put(r.Context(), sessions.ClosedKey(id), cb, sessions.JSONContentType)
		_ = sessions.ComposeSession(r.Context(), s.store, id)
	}

	meta, err := s.loadMetadata(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "session not found"})
		return
	}
	meta.Running = s.spawner.Running(id)
	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusOK, meta)
}

func (s *Server) handleForkSession(w http.ResponseWriter, r *http.Request) {
	srcID := r.PathValue("id")
	srcMeta, err := s.loadMetadata(r.Context(), srcID)
	if err != nil {
		if errors.Is(err, sessions.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "source session not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	raw, err := sessions.ReadRawJSONL(r.Context(), s.store, srcID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	newID := sessions.NewID()
	now := time.Now().UTC().Format(time.RFC3339)
	name := srcMeta.SessionName
	if name != "" {
		name += " (fork)"
	}

	var partCount int64
	if len(raw) > 0 {
		gz, err := sessions.GzipBytes(raw)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		if err := s.store.Put(r.Context(), sessions.PartKey(newID, 0), gz, sessions.PartContentType); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		partCount = 1
	}

	// Copy workspace.bundle if present on source.
	if bundle, err := s.store.Get(r.Context(), sessions.WorkspaceBundleKey(srcID)); err == nil && len(bundle) > 0 {
		_ = s.store.Put(r.Context(), sessions.WorkspaceBundleKey(newID), bundle, sessions.BundleContentType)
	}

	meta := &sessions.Metadata{
		SessionID:   newID,
		SessionName: name,
		SessionKind: sessions.KindInteractive,
		Status:      sessions.StatusActive,
		Repo:        srcMeta.Repo,
		Branch:      srcMeta.Branch,
		Toolchain:   srcMeta.Toolchain,
		ForkedFrom:  srcID,
		LastOffset:  int64(len(raw)),
		PartCount:   partCount,
		ModelConfig: srcMeta.ModelConfig,
		StartedAt:   now,
		UpdatedAt:   now,
		Running:     true,
	}
	if err := s.saveMetadata(r.Context(), meta); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	provider, _ := meta.ModelConfig["provider"].(string)
	modelID, _ := meta.ModelConfig["modelId"].(string)
	tok := s.tokens.Issue(newID)
	if _, err := s.spawner.Spawn(r.Context(), SpawnSpec{
		SessionID:   newID,
		SessionName: meta.SessionName,
		SessionKind: sessions.KindInteractive,
		Repo:        meta.Repo,
		Branch:      meta.Branch,
		Toolchain:   meta.Toolchain,
		Provider:    provider,
		Model:       modelID,
		HubURL:      s.hubBaseURL,
		HubToken:    tok,
	}); err != nil {
		meta.Status = sessions.StatusPaused
		meta.Running = false
		_ = s.saveMetadata(r.Context(), meta)
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": err.Error()})
		return
	}
	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusCreated, meta)
}

func (s *Server) handleHubEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()

	ch := make(chan []byte, 64)
	s.subsMu.Lock()
	s.subs[ch] = struct{}{}
	s.subsMu.Unlock()
	defer func() {
		s.subsMu.Lock()
		delete(s.subs, ch)
		s.subsMu.Unlock()
	}()

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	for {
		select {
		case msg := <-ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
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

func (s *Server) broadcastSessionUpdate(meta *sessions.Metadata) {
	if meta == nil {
		return
	}
	b, err := json.Marshal(map[string]any{
		"type":    "session_updated",
		"session": meta,
	})
	if err != nil {
		return
	}
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- b:
		default:
		}
	}
}

func (s *Server) postDaemon(ctx context.Context, sessionID, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok, ok := s.tokens.Get(sessionID); ok && tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("daemon %s returned status %d", endpoint, resp.StatusCode)
	}
	return nil
}

func (s *Server) loadMetadata(ctx context.Context, id string) (*sessions.Metadata, error) {
	b, err := s.store.Get(ctx, sessions.MetadataKey(id))
	if err != nil {
		return nil, err
	}
	var meta sessions.Metadata
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	if meta.SessionID == "" {
		meta.SessionID = id
	}
	return &meta, nil
}

func (s *Server) saveMetadata(ctx context.Context, meta *sessions.Metadata) error {
	cp := *meta
	cp.Running = false // transient runtime flag is never persisted
	b, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	return s.store.Put(ctx, sessions.MetadataKey(meta.SessionID), append(b, '\n'), sessions.JSONContentType)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
