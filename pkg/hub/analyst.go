package hub

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

func (s *Server) authenticateAnalyst(w http.ResponseWriter, r *http.Request) bool {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !s.tokens.VerifyAnalyst(tok) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "error": "unauthorized analyst token"})
		return false
	}
	return true
}

// handleAnalystListSessions returns archived and live session metadata filtered
// by kind, repo, pr, status, model, and verdict for read-only analyst pods.
func (s *Server) handleAnalystListSessions(w http.ResponseWriter, r *http.Request) {
	if !s.authenticateAnalyst(w, r) {
		return
	}
	keys, err := s.store.List(r.Context(), "sessions/")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	q := r.URL.Query()
	kindFilter := strings.TrimSpace(q.Get("kind"))
	repoFilter := strings.TrimSpace(q.Get("repo"))
	statusFilter := strings.TrimSpace(q.Get("status"))
	modelFilter := strings.TrimSpace(q.Get("model"))
	verdictFilter := strings.TrimSpace(q.Get("verdict"))
	prFilter := 0
	if rawPR := strings.TrimSpace(q.Get("pr")); rawPR != "" {
		if n, err := strconv.Atoi(rawPR); err == nil {
			prFilter = n
		}
	}

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
		if kindFilter != "" && string(meta.SessionKind) != kindFilter {
			continue
		}
		if repoFilter != "" && meta.Repo != repoFilter {
			continue
		}
		if statusFilter != "" && string(meta.Status) != statusFilter {
			continue
		}
		if verdictFilter != "" && !strings.EqualFold(meta.Verdict, verdictFilter) {
			continue
		}
		if prFilter > 0 && meta.PRNumber != prFilter {
			continue
		}
		if modelFilter != "" {
			mid, _ := meta.ModelConfig["modelId"].(string)
			if !strings.Contains(strings.ToLower(mid), strings.ToLower(modelFilter)) {
				continue
			}
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

// handleAnalystGetTranscript returns any session's transcript (parsed JSON
// messages by default, or raw JSONL with ?format=raw) to an authenticated
// read-only analyst pod.
func (s *Server) handleAnalystGetTranscript(w http.ResponseWriter, r *http.Request) {
	if !s.authenticateAnalyst(w, r) {
		return
	}
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

	if strings.EqualFold(r.URL.Query().Get("format"), "raw") {
		raw, err := sessions.ReadRawJSONL(r.Context(), s.store, id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
		return
	}

	msgs, err := sessions.ReadTranscript(r.Context(), s.store, id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sessionId": id,
		"metadata":  meta,
		"messages":  msgs,
	})
}

type analystSearchHit struct {
	SessionID   string        `json:"session_id"`
	SessionName string        `json:"session_name,omitempty"`
	SessionKind sessions.Kind `json:"session_kind,omitempty"`
	Repo        string        `json:"repo,omitempty"`
	PRNumber    int           `json:"pr_number,omitempty"`
	Verdict     string        `json:"verdict,omitempty"`
	Snippets    []string      `json:"snippets"`
}

// handleAnalystSearch performs a bounded case-insensitive substring search
// across archived session metadata and transcripts.
func (s *Server) handleAnalystSearch(w http.ResponseWriter, r *http.Request) {
	if !s.authenticateAnalyst(w, r) {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "query parameter q is required"})
		return
	}
	lowerQ := strings.ToLower(query)
	limit := 50
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		if n, err := strconv.Atoi(rawLimit); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	keys, err := s.store.List(r.Context(), "sessions/")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	const maxScannedSessions = 500
	scanned := 0
	truncated := false
	hits := make([]analystSearchHit, 0)
	for _, k := range keys {
		if !strings.HasSuffix(k, "/metadata.json") {
			continue
		}
		if len(hits) >= limit {
			break
		}
		if scanned >= maxScannedSessions {
			truncated = true
			break
		}
		scanned++
		b, err := s.store.Get(r.Context(), k)
		if err != nil {
			continue
		}
		var meta sessions.Metadata
		if err := json.Unmarshal(b, &meta); err != nil {
			continue
		}
		var snippets []string
		if strings.Contains(strings.ToLower(string(b)), lowerQ) {
			snippets = append(snippets, "matched metadata")
		}
		if raw, err := sessions.ReadRawJSONL(r.Context(), s.store, meta.SessionID); err == nil && len(raw) > 0 {
			lowerText := strings.ToLower(string(raw))
			searchFrom := 0
			for len(snippets) < 3 && searchFrom < len(lowerText) {
				idx := strings.Index(lowerText[searchFrom:], lowerQ)
				if idx < 0 {
					break
				}
				abs := searchFrom + idx
				start := abs - 60
				if start < 0 {
					start = 0
				}
				for start > 0 && !utf8.RuneStart(lowerText[start]) {
					start--
				}
				end := abs + len(lowerQ) + 100
				if end > len(lowerText) {
					end = len(lowerText)
				}
				for end < len(lowerText) && !utf8.RuneStart(lowerText[end]) {
					end++
				}
				snippets = append(snippets, strings.TrimSpace(lowerText[start:end]))
				searchFrom = abs + len(lowerQ)
			}
		}
		if len(snippets) > 0 {
			hits = append(hits, analystSearchHit{
				SessionID:   meta.SessionID,
				SessionName: meta.SessionName,
				SessionKind: meta.SessionKind,
				Repo:        meta.Repo,
				PRNumber:    meta.PRNumber,
				Verdict:     meta.Verdict,
				Snippets:    snippets,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"query": query, "hits": hits, "scanned": scanned, "truncated": truncated})
}
