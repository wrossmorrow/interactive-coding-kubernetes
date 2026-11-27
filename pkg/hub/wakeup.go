package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

var agentrunnerFooterRegex = regexp.MustCompile(`<!--\s*agentrunner-session:\s*([A-Za-z0-9_-]+)\s*-->`)

// PullReview represents the subset of GitHub's PR review REST response needed
// for conditional wake-up evaluation.
type PullReview struct {
	ID       int64  `json:"id"`
	State    string `json:"state"`
	Body     string `json:"body"`
	CommitID string `json:"commit_id"`
	HTMLURL  string `json:"html_url"`
	User     struct {
		Login string `json:"login"`
	} `json:"user"`
	SubmittedAt string `json:"submitted_at"`
}

// ReviewFetcher abstracts conditional GitHub PR review polling (If-None-Match).
type ReviewFetcher interface {
	// ListReviews fetches reviews for owner/repo PR #pr. When etag matches, it
	// returns (nil, etag, true, nil) representing HTTP 304 Not Modified (0 rate-limit cost).
	ListReviews(ctx context.Context, repo string, pr int, etag string) (reviews []PullReview, newETag string, notModified bool, err error)
}

// GitHubReviewFetcher polls GitHub's REST API using conditional ETags.
type GitHubReviewFetcher struct {
	BaseURL       string
	Token         string
	TokenProvider TokenProvider
	HTTPClient    *http.Client
}

func (g *GitHubReviewFetcher) ListReviews(ctx context.Context, repo string, pr int, etag string) ([]PullReview, string, bool, error) {
	base := strings.TrimRight(g.BaseURL, "/")
	if base == "" {
		base = "https://api.github.com"
	}
	u := fmt.Sprintf("%s/repos/%s/pulls/%d/reviews?per_page=100", base, repo, pr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, etag, false, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "k8s-agents-session-hub")
	tok := g.Token
	if g.TokenProvider != nil {
		minted, terr := g.TokenProvider.Token(ctx)
		if terr != nil {
			return nil, etag, false, fmt.Errorf("github auth token: %w", terr)
		}
		tok = minted
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	client := g.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, etag, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotModified {
		return nil, etag, true, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, etag, false, fmt.Errorf("github reviews %s#%d: HTTP %d: %s", repo, pr, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var reviews []PullReview
	if err := json.NewDecoder(resp.Body).Decode(&reviews); err != nil {
		return nil, etag, false, err
	}
	nextETag := resp.Header.Get("ETag")
	if nextETag == "" {
		nextETag = etag
	}
	return reviews, nextETag, false, nil
}

// ParseAgentrunnerSessionID extracts the archived ARC v2 review session UUID
// from the inert HTML comment footer posted by agentrunner/pkg/review/poster.go.
func ParseAgentrunnerSessionID(body string) string {
	m := agentrunnerFooterRegex.FindStringSubmatch(body)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// ShouldWakeForReview returns true when rev is a newer, actionable PR review
// (skipping PENDING, DISMISSED, already-processed IDs, and clean APPROVE reviews
// with no actionable body).
func ShouldWakeForReview(rev PullReview, headSHA string, lastReviewID int64) bool {
	if rev.ID <= lastReviewID {
		return false
	}
	state := strings.ToUpper(strings.TrimSpace(rev.State))
	switch state {
	case "PENDING", "DISMISSED", "":
		return false
	case "APPROVED":
		// Skip clean approvals with no substantive comments.
		clean := strings.TrimSpace(agentrunnerFooterRegex.ReplaceAllString(rev.Body, ""))
		if clean == "" || strings.EqualFold(clean, "lgtm") {
			return false
		}
	}
	if headSHA != "" && rev.CommitID != "" && !strings.HasPrefix(rev.CommitID, headSHA) && !strings.HasPrefix(headSHA, rev.CommitID) {
		return false
	}
	return true
}

// BuildWakeupPrompt constructs the structured wake-up prompt injected into the
// interactive session when a PR review lands, enforcing critical evaluation and
// the configured WakePolicy ("evaluate-and-stage" vs "auto-push").
func BuildWakeupPrompt(meta *sessions.Metadata, rev PullReview) string {
	policy := effectiveWakePolicy(meta)
	reviewer := rev.User.Login
	if reviewer == "" {
		reviewer = "reviewer"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "A new GitHub PR review (%s) from @%s landed on %s#%d (review ID %d).\n\n", rev.State, reviewer, meta.Repo, meta.PRNumber, rev.ID)
	fmt.Fprintf(&b, "Instructions:\n")
	fmt.Fprintf(&b, "1. Fetch the full PR conversation, reviews, and inline diff comments (e.g. `gh pr view %d --comments` and `gh api repos/%s/pulls/%d/comments`).\n", meta.PRNumber, meta.Repo, meta.PRNumber)
	fmt.Fprintf(&b, "2. Critically evaluate every comment and suggested change against the codebase and test suite. Do NOT blindly apply incorrect or out-of-scope reviewer suggestions; note any disputed findings clearly.\n")
	fmt.Fprintf(&b, "3. Run the repo's verification checks (e.g. `just check`).\n")
	if policy == sessions.WakePolicyAutoPush {
		fmt.Fprintf(&b, "4. Policy is `auto-push`: commit and push accepted fixes to the PR branch, reply concisely to review threads, and summarize accepted vs. disputed findings.\n")
	} else {
		fmt.Fprintf(&b, "4. Policy is `evaluate-and-stage`: apply valid fixes locally and verify tests, but do NOT push commits to GitHub. Write a concise summary of accepted fixes vs. disputed findings for human sign-off.\n")
	}
	return b.String()
}

func effectiveWakePolicy(meta *sessions.Metadata) sessions.WakePolicy {
	if meta == nil || meta.WakePolicy == "" {
		return sessions.WakePolicyEvaluateAndStage
	}
	return meta.WakePolicy
}

func effectiveMaxWakeups(meta *sessions.Metadata) int {
	if meta == nil || meta.MaxWakeupsPerHumanTurn <= 0 {
		return sessions.DefaultMaxWakeupsPerHumanTurn
	}
	return meta.MaxWakeupsPerHumanTurn
}

// WakeWatcher polls GitHub PR reviews using conditional ETags and wakes linked
// interactive sessions subject to wake_policy and maxWakeupsPerHumanTurn.
type WakeWatcher struct {
	hub      *Server
	fetcher  ReviewFetcher
	interval time.Duration

	mu    sync.Mutex
	etags map[string]string // "repo#pr" -> ETag
}

func NewWakeWatcher(hub *Server, fetcher ReviewFetcher, interval time.Duration) *WakeWatcher {
	return &WakeWatcher{
		hub:      hub,
		fetcher:  fetcher,
		interval: interval,
		etags:    map[string]string{},
	}
}

// Start launches the periodic PR watcher loop and returns a stop function.
func (w *WakeWatcher) Start(ctx context.Context) func() {
	if w == nil || w.fetcher == nil || w.interval <= 0 {
		return func() {}
	}
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
				w.PollOnce(wctx)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// PollOnce performs one conditional-ETag sweep across all eligible interactive sessions.
func (w *WakeWatcher) PollOnce(ctx context.Context) {
	keys, err := w.hub.store.List(ctx, "sessions/")
	if err != nil {
		log.Printf("wakeup poll list sessions error: %v", err)
		return
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, "/metadata.json") {
			continue
		}
		b, err := w.hub.store.Get(ctx, k)
		if err != nil {
			continue
		}
		var meta sessions.Metadata
		if err := json.Unmarshal(b, &meta); err != nil {
			continue
		}
		if err := w.checkSession(ctx, &meta); err != nil {
			log.Printf("wakeup check session %s error: %v", meta.SessionID, err)
		}
	}
}

func (w *WakeWatcher) checkSession(ctx context.Context, meta *sessions.Metadata) error {
	if meta.Status == sessions.StatusClosed || meta.SessionKind.IsAutomated() {
		return nil
	}
	if meta.Repo == "" || meta.PRNumber <= 0 {
		return nil
	}
	if effectiveWakePolicy(meta) == sessions.WakePolicyOff {
		return nil
	}
	if meta.WakeupsSinceHumanTurn >= effectiveMaxWakeups(meta) {
		return nil
	}

	prKey := fmt.Sprintf("%s#%d", meta.Repo, meta.PRNumber)
	w.mu.Lock()
	etag := w.etags[prKey]
	w.mu.Unlock()

	reviews, nextETag, notModified, err := w.fetcher.ListReviews(ctx, meta.Repo, meta.PRNumber, etag)
	if err != nil {
		return err
	}
	if nextETag != "" {
		w.mu.Lock()
		w.etags[prKey] = nextETag
		w.mu.Unlock()
	}
	if notModified || len(reviews) == 0 {
		return nil
	}

	// Pick the latest actionable review newer than LastProcessedReviewID.
	var latest *PullReview
	for i := range reviews {
		rev := reviews[i]
		if !ShouldWakeForReview(rev, meta.HeadSHA, meta.LastProcessedReviewID) {
			continue
		}
		if latest == nil || rev.ID > latest.ID {
			cp := rev
			latest = &cp
		}
	}
	if latest == nil {
		return nil
	}

	return w.hub.TriggerWakeup(ctx, meta, *latest)
}

// TriggerWakeup rehydrates (if paused) and injects a wake-up prompt into an
// interactive session, updating anti-ping-pong counters and needs-attention metadata.
func (s *Server) TriggerWakeup(ctx context.Context, meta *sessions.Metadata, rev PullReview) error {
	if meta.Status == sessions.StatusClosed || meta.SessionKind.IsAutomated() {
		return fmt.Errorf("cannot wake closed or automated session %s", meta.SessionID)
	}
	if meta.WakeupsSinceHumanTurn >= effectiveMaxWakeups(meta) {
		return fmt.Errorf("wakeup cap reached (%d/%d) for session %s", meta.WakeupsSinceHumanTurn, effectiveMaxWakeups(meta), meta.SessionID)
	}

	endpoint, running := s.spawner.Endpoint(meta.SessionID)
	if !running {
		rejoined, err := s.rejoinSession(ctx, meta)
		if err != nil {
			return fmt.Errorf("rejoin for wakeup: %w", err)
		}
		meta = rejoined
		endpoint, running = s.spawner.Endpoint(meta.SessionID)
		if !running {
			return fmt.Errorf("session %s not running after rejoin", meta.SessionID)
		}
	}

	prompt := BuildWakeupPrompt(meta, rev)
	cmdPayload := map[string]any{
		"type":              "prompt",
		"message":           prompt,
		"streamingBehavior": "followUp",
	}
	if err := s.postDaemonJSON(ctx, meta.SessionID, endpoint+"/api/cmd", cmdPayload); err != nil {
		return fmt.Errorf("inject wakeup prompt: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	s.metaMu.Lock()
	if latestMeta, lerr := s.loadMetadata(ctx, meta.SessionID); lerr == nil && latestMeta != nil {
		meta = latestMeta
	}
	meta.WakeupsSinceHumanTurn++
	meta.LastProcessedReviewID = rev.ID
	if reviewSessID := ParseAgentrunnerSessionID(rev.Body); reviewSessID != "" {
		meta.LastReviewSessionID = reviewSessID
	}
	meta.LastWakeAt = now
	meta.WakeSummary = fmt.Sprintf("Woken by %s review from @%s on %s#%d", rev.State, rev.User.Login, meta.Repo, meta.PRNumber)
	if effectiveWakePolicy(meta) == sessions.WakePolicyEvaluateAndStage {
		meta.Status = sessions.StatusNeedsAttention
	} else {
		meta.Status = sessions.StatusActive
	}
	meta.UpdatedAt = now
	saveErr := s.saveMetadata(ctx, meta)
	s.metaMu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	meta.Running = true
	s.broadcastSessionUpdate(meta)
	return nil
}

func (s *Server) postDaemonJSON(ctx context.Context, sessionID, endpoint string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok, ok := s.tokens.Get(sessionID); ok && tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("daemon %s returned HTTP %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
