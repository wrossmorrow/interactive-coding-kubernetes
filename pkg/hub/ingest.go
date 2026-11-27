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
	if err := s.store.Put(r.Context(), key, body, ct); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	if rel == "metadata.json" {
		var meta sessions.Metadata
		if err := json.Unmarshal(body, &meta); err == nil {
			meta.Running = s.spawner.Running(id)
			s.broadcastSessionUpdate(&meta)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "key": key})
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
