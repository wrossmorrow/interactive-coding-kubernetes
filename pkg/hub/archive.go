package hub

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

var safeArchiveSessionID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// agentrunnerArchiveMetadata mirrors agentrunner/pkg/review.SessionArchiveMetadata.
type agentrunnerArchiveMetadata struct {
	SessionID   string         `json:"session_id"`
	SessionKind string         `json:"session_kind"`
	SessionName string         `json:"session_name"`
	Repo        string         `json:"repo,omitempty"`
	PRNumber    int            `json:"pr_number,omitempty"`
	IssueNumber int            `json:"issue_number,omitempty"`
	RunID       string         `json:"run_id,omitempty"`
	HeadSHA     string         `json:"head_sha,omitempty"`
	BaseRef     string         `json:"base_ref,omitempty"`
	Tag         string         `json:"tag,omitempty"`
	Provider    string         `json:"provider,omitempty"`
	Model       string         `json:"model,omitempty"`
	Mode        string         `json:"mode,omitempty"`
	Output      string         `json:"output,omitempty"`
	Verdict     string         `json:"verdict,omitempty"`
	Findings    int            `json:"findings"`
	Turns       int            `json:"turns"`
	EndReason   string         `json:"end_reason"`
	StartedAt   time.Time      `json:"started_at"`
	EndedAt     time.Time      `json:"ended_at"`
	Stats       map[string]any `json:"stats,omitempty"`
}

// handleInternalArchive accepts fail-open multipart/form-data session archives
// posted by agentrunner (`metadata` JSON part + optional `transcript` gzip part)
// authenticated with the shared SESSION_SINK_TOKEN bearer secret.
func (s *Server) handleInternalArchive(w http.ResponseWriter, r *http.Request) {
	if s.archiveSinkToken == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": "archive sink not configured"})
		return
	}
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.archiveSinkToken)) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "error": "unauthorized"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64<<20)
	if err := r.ParseMultipartForm(16 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid multipart form: " + err.Error()})
		return
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	metaFile, _, err := r.FormFile("metadata")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "missing metadata part"})
		return
	}
	defer func() { _ = metaFile.Close() }()
	metaBytes, err := io.ReadAll(io.LimitReader(metaFile, 4<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "read metadata: " + err.Error()})
		return
	}

	var in agentrunnerArchiveMetadata
	if err := json.Unmarshal(metaBytes, &in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid metadata json: " + err.Error()})
		return
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(r.Header.Get("X-Session-Id"))
	}
	if !safeArchiveSessionID.MatchString(sessionID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid or missing session_id"})
		return
	}

	kind := sessions.Kind(strings.TrimSpace(in.SessionKind))
	if kind == "" {
		kind = sessions.KindReview
	}

	now := time.Now().UTC().Format(time.RFC3339)
	startedAt := now
	if !in.StartedAt.IsZero() {
		startedAt = in.StartedAt.UTC().Format(time.RFC3339)
	}
	endedAt := now
	if !in.EndedAt.IsZero() {
		endedAt = in.EndedAt.UTC().Format(time.RFC3339)
	}
	endReason := in.EndReason
	if endReason == "" {
		endReason = "closed"
	}

	modelCfg := map[string]any{}
	if in.Provider != "" {
		modelCfg["provider"] = in.Provider
	}
	if in.Model != "" {
		modelCfg["modelId"] = in.Model
	}

	meta := &sessions.Metadata{
		SessionID:     sessionID,
		SessionName:   in.SessionName,
		SessionKind:   kind,
		Status:        sessions.StatusClosed,
		Repo:          in.Repo,
		BaseRef:       in.BaseRef,
		HeadSHA:       in.HeadSHA,
		PRNumber:      in.PRNumber,
		IssueNumber:   in.IssueNumber,
		RunID:         in.RunID,
		Tag:           in.Tag,
		Mode:          in.Mode,
		Output:        in.Output,
		Verdict:       in.Verdict,
		Findings:      in.Findings,
		Turns:         in.Turns,
		ModelConfig:   modelCfg,
		StartedAt:     startedAt,
		UpdatedAt:     endedAt,
		EndedAt:       endedAt,
		EndReason:     endReason,
		Tokens:        sessions.TokensFromStats(in.Stats),
		ToolCalls:     sessions.ToolCallsFromStats(in.Stats),
		MessageCounts: sessions.MessageCountsFromStats(in.Stats),
	}

	var transcriptBytes []byte
	if trFile, _, err := r.FormFile("transcript"); err == nil {
		defer func() { _ = trFile.Close() }()
		transcriptBytes, err = io.ReadAll(io.LimitReader(trFile, 60<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "read transcript: " + err.Error()})
			return
		}
	}

	if len(transcriptBytes) > 0 {
		if err := s.store.Put(r.Context(), sessions.CompactedKey(sessionID), transcriptBytes, sessions.PartContentType); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
			return
		}
		meta.PartCount = 1
	}

	if err := s.saveMetadata(r.Context(), meta); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	rep := sessions.ClosedReport{
		Status:    "ok",
		PartCount: meta.PartCount,
	}
	cb, _ := json.Marshal(rep)
	if err := s.store.Put(r.Context(), sessions.ClosedKey(sessionID), cb, sessions.JSONContentType); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.broadcastSessionUpdate(meta)
	writeJSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"sessionId": sessionID,
		"status":    meta.Status,
	})
}
