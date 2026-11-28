// Package sessions implements the session lifecycle state machine, object
// storage backends (local DirStore, bearer-authenticated HTTPStore, and GCS
// Store with server-side Compose), offset-resuming JSONL part uploader, and
// read-only transcript reconstruction.
package sessions

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// WakePolicy controls how an interactive session responds when a new GitHub PR
// review lands on its linked pull request.
type WakePolicy string

const (
	WakePolicyEvaluateAndStage WakePolicy = "evaluate-and-stage"
	WakePolicyAutoPush         WakePolicy = "auto-push"
	WakePolicyOff              WakePolicy = "off"
)

// DefaultMaxWakeupsPerHumanTurn bounds automated author-vs-reviewer loops.
const DefaultMaxWakeupsPerHumanTurn = 1

// Sandbox resource size presets.
const (
	SizeSmall  = "small"
	SizeMedium = "medium"
	SizeLarge  = "large"
)

// ValidSize reports whether s is an accepted sandbox size preset (empty means auto).
func ValidSize(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", SizeSmall, SizeMedium, SizeLarge:
		return true
	default:
		return false
	}
}

// Status represents the lifecycle state of a session.
type Status string

const (
	StatusActive         Status = "active"
	StatusPaused         Status = "paused"
	StatusNeedsAttention Status = "needs-attention"
	StatusClosed         Status = "closed"
)

// Kind distinguishes interactive developer sessions from automated CI runs
// and read-only analyst sessions. Automated kinds defer to agentrunner's
// deriveSessionKind values ("review", "suggest", "release-draft", "write-branch").
type Kind string

const (
	KindInteractive  Kind = "interactive"
	KindAnalyst      Kind = "analyst"
	KindReview       Kind = "review"
	KindSuggest      Kind = "suggest"
	KindReleaseDraft Kind = "release-draft"
	KindWriteBranch  Kind = "write-branch"
)

// IsAutomated reports whether k represents a CI/ARC runner session that must
// never be resumed as an interactive author session.
func (k Kind) IsAutomated() bool {
	switch k {
	case "", KindInteractive, KindAnalyst:
		return false
	default:
		return true
	}
}

const (
	PartKeyFormat         = "sessions/%s/parts/part-%012d.jsonl.gz"
	PartsPrefixFormat     = "sessions/%s/parts/"
	ClosedKeyFormat       = "sessions/%s/_closed"
	MetadataKeyFormat     = "sessions/%s/metadata.json"
	CompactedKeyFormat    = "sessions/%s/compacted.jsonl.gz"
	WorkspaceBundleFormat = "sessions/%s/workspace.bundle"
	PartContentType       = "application/gzip"
	JSONContentType       = "application/json"
	BundleContentType     = "application/gzip"
)

// Metadata is persisted at sessions/<id>/metadata.json and indexed by session-hub.
type Metadata struct {
	SessionID              string         `json:"session_id"`
	SessionName            string         `json:"session_name,omitempty"`
	SessionKind            Kind           `json:"session_kind,omitempty"`
	Status                 Status         `json:"status"`
	Repo                   string         `json:"repo,omitempty"`
	Branch                 string         `json:"branch,omitempty"`
	BaseRef                string         `json:"base_ref,omitempty"`
	HeadSHA                string         `json:"head_sha,omitempty"`
	PRNumber               int            `json:"pr_number,omitempty"`
	IssueNumber            int            `json:"issue_number,omitempty"`
	RunID                  string         `json:"run_id,omitempty"`
	Tag                    string         `json:"tag,omitempty"`
	Mode                   string         `json:"mode,omitempty"`
	Output                 string         `json:"output,omitempty"`
	Verdict                string         `json:"verdict,omitempty"`
	Findings               int            `json:"findings,omitempty"`
	Turns                  int            `json:"turns,omitempty"`
	Toolchain              string         `json:"toolchain,omitempty"`
	Size                   string         `json:"size,omitempty"`
	ForkedFrom             string         `json:"forked_from,omitempty"`
	WakePolicy             WakePolicy     `json:"wake_policy,omitempty"`
	MaxWakeupsPerHumanTurn int            `json:"max_wakeups_per_human_turn,omitempty"`
	WakeupsSinceHumanTurn  int            `json:"wakeups_since_human_turn,omitempty"`
	LastProcessedReviewID  int64          `json:"last_processed_review_id,omitempty"`
	LastReviewSessionID    string         `json:"last_review_session_id,omitempty"`
	LastWakeAt             string         `json:"last_wake_at,omitempty"`
	WakeSummary            string         `json:"wake_summary,omitempty"`
	LastOffset             int64          `json:"last_offset,omitempty"`
	PartCount              int64          `json:"part_count,omitempty"`
	Hash                   string         `json:"hash,omitempty"`
	AgentEnvironment       map[string]any `json:"agent_environment,omitempty"`
	ModelConfig            map[string]any `json:"model_config,omitempty"`
	AgentIdentity          string         `json:"agent_identity,omitempty"`
	ClientHost             string         `json:"client_host,omitempty"`
	StartedAt              string         `json:"started_at,omitempty"`
	UpdatedAt              string         `json:"updated_at,omitempty"`
	EndedAt                string         `json:"ended_at,omitempty"`
	EndReason              string         `json:"end_reason,omitempty"`
	Tokens                 map[string]any `json:"tokens,omitempty"`
	ToolCalls              int64          `json:"tool_calls,omitempty"`
	MessageCounts          map[string]any `json:"message_counts,omitempty"`
	// Running is a transient runtime flag populated by session-hub responses.
	Running bool `json:"running,omitempty"`
}

// ClosedReport is written at sessions/<id>/_closed only when a session is
// explicitly finalized via Close.
type ClosedReport struct {
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
	FinalOffset int64  `json:"finalOffset"`
	PartCount   int64  `json:"partCount"`
	Hash        string `json:"hash"`
}

// NewID generates a time-ordered UUIDv7 session identifier.
func NewID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.New().String()
}

func PartKey(sessionID string, offset int64) string {
	return fmt.Sprintf(PartKeyFormat, sessionID, offset)
}

func PartsPrefix(sessionID string) string {
	return fmt.Sprintf(PartsPrefixFormat, sessionID)
}

func ClosedKey(sessionID string) string {
	return fmt.Sprintf(ClosedKeyFormat, sessionID)
}

func MetadataKey(sessionID string) string {
	return fmt.Sprintf(MetadataKeyFormat, sessionID)
}

func CompactedKey(sessionID string) string {
	return fmt.Sprintf(CompactedKeyFormat, sessionID)
}

func WorkspaceBundleKey(sessionID string) string {
	return fmt.Sprintf(WorkspaceBundleFormat, sessionID)
}

// SessionIDFromFile derives the stable session id from a pi session file path
// such as ~/.pi/agent/sessions/2026-05-06T12-00-00-000Z_<uuid>.jsonl -> <uuid>.
func SessionIDFromFile(path string) string {
	base := filepath.Base(path)
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	if i := strings.Index(stem, "_"); i >= 0 {
		if id := strings.TrimPrefix(stem[i+1:], "_"); id != "" {
			return id
		}
	}
	return stem
}

// ParsePartOffset extracts the byte offset from a part key such as
// sessions/<id>/parts/part-000000000042.jsonl.gz.
func ParsePartOffset(key string) (int64, bool) {
	base := filepath.Base(key)
	if !strings.HasPrefix(base, "part-") || !strings.HasSuffix(base, ".jsonl.gz") {
		return 0, false
	}
	num := strings.TrimSuffix(strings.TrimPrefix(base, "part-"), ".jsonl.gz")
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
