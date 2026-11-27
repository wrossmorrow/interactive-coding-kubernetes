package hub

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

var allowedPartPath = regexp.MustCompile(`^parts/part-\d{12}\.jsonl\.gz$`)

func isAllowedObjectPath(rel string) bool {
	switch rel {
	case "metadata.json", "_closed", "workspace.bundle", "compacted.jsonl.gz":
		return true
	default:
		return allowedPartPath.MatchString(rel)
	}
}

func (s *Server) authenticateInternal(w http.ResponseWriter, r *http.Request, sessionID string) bool {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !s.tokens.Verify(sessionID, tok) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "error": "unauthorized"})
		return false
	}
	return true
}

func (s *Server) handleInternalPutObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rel := strings.TrimPrefix(r.PathValue("path"), "/")
	if !s.authenticateInternal(w, r, id) {
		return
	}
	if !isAllowedObjectPath(rel) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "object path not allowed"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"success": false, "error": err.Error()})
		return
	}
	key := "sessions/" + id + "/" + rel
	ct := r.Header.Get("Content-Type")
	if rel == "metadata.json" {
		// Merge incoming daemon metadata with hub-owned control-plane fields
		// (PR linkage, wake policy, anti-ping-pong counters, needs-attention) so
		// periodic daemon flushes never clobber hub state.
		var incoming sessions.Metadata
		if err := json.Unmarshal(body, &incoming); err == nil {
			if incoming.SessionID != "" && incoming.SessionID != id {
				writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "metadata session_id does not match URL session id"})
				return
			}
			incoming.SessionID = id
			s.metaMu.Lock()
			if existing, lerr := s.loadMetadata(r.Context(), id); lerr == nil && existing != nil {
				mergeHubMetadata(&incoming, existing)
			}
			err = s.saveMetadata(r.Context(), &incoming)
			s.metaMu.Unlock()
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
				return
			}
			incoming.Running = s.spawner != nil && s.spawner.Running(id)
			s.broadcastSessionUpdate(&incoming)
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "key": key})
			return
		}
	}
	if err := s.store.Put(r.Context(), key, body, ct); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "key": key})
}

// mergeHubMetadata preserves hub-owned control-plane fields on incoming daemon
// metadata flushes. Daemon-driven transitions to paused or closed still win.
func mergeHubMetadata(incoming, existing *sessions.Metadata) {
	if incoming.SessionID == "" {
		incoming.SessionID = existing.SessionID
	}
	if incoming.ForkedFrom == "" {
		incoming.ForkedFrom = existing.ForkedFrom
	}
	if incoming.HeadSHA == "" {
		incoming.HeadSHA = existing.HeadSHA
	}
	// Hub-owned PR linkage and wake-up bookkeeping are authoritative.
	incoming.PRNumber = existing.PRNumber
	if existing.WakePolicy != "" {
		incoming.WakePolicy = existing.WakePolicy
	}
	if existing.MaxWakeupsPerHumanTurn > 0 {
		incoming.MaxWakeupsPerHumanTurn = existing.MaxWakeupsPerHumanTurn
	}
	incoming.WakeupsSinceHumanTurn = existing.WakeupsSinceHumanTurn
	incoming.LastProcessedReviewID = existing.LastProcessedReviewID
	incoming.LastReviewSessionID = existing.LastReviewSessionID
	incoming.LastWakeAt = existing.LastWakeAt
	incoming.WakeSummary = existing.WakeSummary
	if existing.Status == sessions.StatusNeedsAttention && incoming.Status == sessions.StatusActive {
		incoming.Status = sessions.StatusNeedsAttention
	}
}

func (s *Server) handleInternalGetObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rel := strings.TrimPrefix(r.PathValue("path"), "/")
	if !s.authenticateInternal(w, r, id) {
		return
	}
	if !isAllowedObjectPath(rel) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "object path not allowed"})
		return
	}
	key := "sessions/" + id + "/" + rel
	data, err := s.store.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, sessions.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	if strings.HasSuffix(rel, ".json") || rel == "_closed" {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/gzip")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleInternalListObjects(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.authenticateInternal(w, r, id) {
		return
	}
	prefix := "sessions/" + id + "/"
	keys, err := s.store.List(r.Context(), prefix)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) handleInternalCompose(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.authenticateInternal(w, r, id) {
		return
	}
	if err := sessions.ComposeSession(r.Context(), s.store, id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
