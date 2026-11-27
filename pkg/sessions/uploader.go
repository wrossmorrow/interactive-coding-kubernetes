package sessions

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"log"
	"os"
	"sync"
	"time"

	"github.com/wrossmorrow/k8s-agents/pkg/workspace"
)

const (
	DefaultFlushInterval = 30 * time.Second
	DefaultMaxPartBytes  = 5 * 1024 * 1024
)

// Ref is the live pi session snapshot passed to Uploader operations.
type Ref struct {
	SessionID        string
	SessionKind      Kind
	Path             string
	State            map[string]any
	Stats            map[string]any
	Git              workspace.GitState
	Toolchain        string
	AgentEnvironment map[string]any
	AgentIdentity    string
	ClientHost       string
}

// State is the per-session upload bookkeeping.
type State struct {
	SessionKey string
	Path       string
	LastOffset int64
	PartCount  int64
	HashHex    string
	Shipped    bool
	Status     Status
	Metadata   *Metadata
	hash       hash.Hash
}

// Uploader ships pi session JSONL parts to a Store, tracking per-session byte
// offsets across pause/rejoin cycles and separating Pause (resumable, no
// _closed marker) from Close (finalized _closed marker + Compose).
type Uploader struct {
	store         Store
	flushInterval time.Duration
	maxPartBytes  int64
	source        func(context.Context) (*Ref, error)

	mu       sync.Mutex
	sessions map[string]*State

	cancel   context.CancelFunc
	loopDone chan struct{}
}

func NewUploader(store Store, flushInterval time.Duration, maxPartBytes int64, source func(context.Context) (*Ref, error)) *Uploader {
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}
	if maxPartBytes <= 0 {
		maxPartBytes = DefaultMaxPartBytes
	}
	return &Uploader{
		store:         store,
		flushInterval: flushInterval,
		maxPartBytes:  maxPartBytes,
		source:        source,
		sessions:      map[string]*State{},
	}
}

func (u *Uploader) Enabled() bool { return u.store != nil }

func (u *Uploader) SetSource(fn func(context.Context) (*Ref, error)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.source = fn
}

// StatusOf reports the current lifecycle status tracked for sessionKey.
func (u *Uploader) StatusOf(sessionKey string) Status {
	u.mu.Lock()
	defer u.mu.Unlock()
	if st, ok := u.sessions[sessionKey]; ok {
		return st.Status
	}
	return StatusActive
}

// IsClosed reports whether sessionKey has been explicitly closed.
func (u *Uploader) IsClosed(sessionKey string) bool {
	return u.StatusOf(sessionKey) == StatusClosed
}

// Hydrate initializes upload state for a resumed session from existing store
// metadata so LastOffset and PartCount resume where the previous run stopped
// instead of resetting to 0. If the session was paused or needs-attention, it
// transitions to active; closed sessions remain closed.
func (u *Uploader) Hydrate(ctx context.Context, sessionKey, localPath string) (*State, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.store == nil {
		return &State{SessionKey: sessionKey, Path: localPath, Status: StatusActive}, nil
	}
	st, err := u.ensureStateLocked(ctx, sessionKey, localPath, nil)
	if err != nil {
		return nil, err
	}
	if st.Status != StatusClosed {
		st.Status = StatusActive
		st.Metadata.Status = StatusActive
	}
	return cloneState(st), nil
}

// Start begins the periodic flush loop and returns a stop function.
func (u *Uploader) Start(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	u.cancel = cancel
	u.loopDone = make(chan struct{})
	go func() {
		defer close(u.loopDone)
		t := time.NewTicker(u.flushInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				u.tick(ctx)
			}
		}
	}()
	return func() { cancel(); <-u.loopDone }
}

// Stop halts the periodic flush loop.
func (u *Uploader) Stop() {
	if u.cancel != nil {
		u.cancel()
	}
	if u.loopDone != nil {
		<-u.loopDone
	}
}

// Flush ships any new complete JSONL lines for ref since LastOffset.
func (u *Uploader) Flush(ctx context.Context, ref *Ref) (*State, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	key := sessionKeyFromRef(ref)
	if u.store == nil {
		return &State{SessionKey: key, Path: ref.Path, Status: StatusActive}, nil
	}
	st, err := u.ensureStateLocked(ctx, key, ref.Path, ref)
	if err != nil {
		return cloneState(st), err
	}
	st.Path = ref.Path
	if err := u.flushLocked(ctx, st); err != nil {
		return cloneState(st), err
	}
	u.updateMetaFromRef(st, ref)
	st.Metadata.LastOffset = st.LastOffset
	st.Metadata.PartCount = st.PartCount
	st.Metadata.Hash = st.HashHex
	st.Metadata.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if st.Status != StatusClosed {
		st.Status = StatusActive
		st.Metadata.Status = StatusActive
	}
	if err := u.putMetadataLocked(ctx, st); err != nil {
		return cloneState(st), err
	}
	return cloneState(st), nil
}

// Pause flushes pending JSONL lines and writes status="paused" to metadata.json
// without writing _closed, so the session can be browsed at $0 and rejoined later.
func (u *Uploader) Pause(ctx context.Context, ref *Ref) (*State, error) {
	u.Stop()
	u.mu.Lock()
	defer u.mu.Unlock()
	key := sessionKeyFromRef(ref)
	if u.store == nil {
		return &State{SessionKey: key, Path: ref.Path, Status: StatusPaused}, nil
	}
	st, err := u.ensureStateLocked(ctx, key, ref.Path, ref)
	if err != nil {
		return cloneState(st), err
	}
	if st.Status == StatusClosed {
		return cloneState(st), nil
	}
	st.Path = ref.Path
	flushErr := u.flushLocked(ctx, st)
	u.updateMetaFromRef(st, ref)
	st.Status = StatusPaused
	st.Metadata.Status = StatusPaused
	st.Metadata.LastOffset = st.LastOffset
	st.Metadata.PartCount = st.PartCount
	st.Metadata.Hash = st.HashHex
	st.Metadata.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := u.putMetadataLocked(ctx, st); err != nil {
		return cloneState(st), err
	}
	return cloneState(st), flushErr
}

// CloseSession finalizes a session: flushes pending lines, writes metadata.json
// with status="closed", writes _closed, and runs Store.Compose into compacted.jsonl.gz.
func (u *Uploader) CloseSession(ctx context.Context, ref *Ref, endReason string) (*State, error) {
	u.Stop()
	u.mu.Lock()
	defer u.mu.Unlock()
	key := sessionKeyFromRef(ref)
	if u.store == nil {
		return &State{SessionKey: key, Path: ref.Path, Status: StatusClosed}, nil
	}
	st, err := u.ensureStateLocked(ctx, key, ref.Path, ref)
	if err != nil {
		return cloneState(st), err
	}
	st.Path = ref.Path
	flushErr := u.flushLocked(ctx, st)
	u.updateMetaFromRef(st, ref)

	now := time.Now().UTC().Format(time.RFC3339)
	st.Status = StatusClosed
	st.Metadata.Status = StatusClosed
	st.Metadata.LastOffset = st.LastOffset
	st.Metadata.PartCount = st.PartCount
	st.Metadata.Hash = st.HashHex
	st.Metadata.UpdatedAt = now
	st.Metadata.EndedAt = now
	st.Metadata.EndReason = endReason
	if err := u.putMetadataLocked(ctx, st); err != nil {
		return cloneState(st), err
	}

	rep := ClosedReport{
		Status:      "ok",
		FinalOffset: st.LastOffset,
		PartCount:   st.PartCount,
		Hash:        st.HashHex,
	}
	if flushErr != nil {
		rep.Status = "error"
		rep.Error = flushErr.Error()
	}
	cb, err := json.Marshal(rep)
	if err != nil {
		return cloneState(st), err
	}
	if err := u.store.Put(ctx, ClosedKey(key), cb, JSONContentType); err != nil {
		return cloneState(st), fmt.Errorf("write _closed for %s: %w", key, err)
	}
	if err := ComposeSession(ctx, u.store, key); err != nil {
		log.Printf("session %s compose warning: %v", key, err)
	}
	return cloneState(st), flushErr
}

func (u *Uploader) ensureStateLocked(ctx context.Context, key, path string, ref *Ref) (*State, error) {
	if st, ok := u.sessions[key]; ok {
		return st, nil
	}
	st := &State{
		SessionKey: key,
		Path:       path,
		Status:     StatusActive,
		hash:       sha256.New(),
		Metadata: &Metadata{
			SessionID:   key,
			SessionKind: KindInteractive,
			Status:      StatusActive,
			StartedAt:   time.Now().UTC().Format(time.RFC3339),
		},
	}

	if existing, err := u.store.Get(ctx, MetadataKey(key)); err == nil {
		var meta Metadata
		if err := json.Unmarshal(existing, &meta); err == nil {
			if meta.SessionID == "" {
				meta.SessionID = key
			}
			if meta.SessionKind == "" {
				meta.SessionKind = KindInteractive
			}
			st.Metadata = &meta
			st.LastOffset = meta.LastOffset
			st.PartCount = meta.PartCount
			st.HashHex = meta.Hash
			if meta.Status != "" {
				st.Status = meta.Status
			}
		}
	}

	if existing, err := u.store.Get(ctx, ClosedKey(key)); err == nil {
		var cr ClosedReport
		if err := json.Unmarshal(existing, &cr); err == nil {
			st.Status = StatusClosed
			st.Metadata.Status = StatusClosed
		}
	}

	// Re-seed running sha256 over the already-shipped prefix of localPath if present
	// so HashHex stays accurate across pause/rejoin cycles.
	if path != "" && st.LastOffset > 0 {
		if data, err := os.ReadFile(path); err == nil && int64(len(data)) >= st.LastOffset {
			st.hash.Write(data[:st.LastOffset])
			st.HashHex = hex.EncodeToString(st.hash.Sum(nil))
		}
	}

	if ref != nil {
		u.updateMetaFromRef(st, ref)
	}
	u.sessions[key] = st
	return st, nil
}

func (u *Uploader) updateMetaFromRef(st *State, ref *Ref) {
	if ref == nil || st.Metadata == nil {
		return
	}
	m := st.Metadata
	if ref.SessionKind != "" {
		m.SessionKind = ref.SessionKind
	}
	if name := SessionNameFromState(ref.State); name != "" {
		m.SessionName = name
	}
	if ref.Git.Repo != "" {
		m.Repo = ref.Git.Repo
	}
	if ref.Git.Branch != "" {
		m.Branch = ref.Git.Branch
	}
	if ref.Git.HeadSHA != "" {
		m.HeadSHA = ref.Git.HeadSHA
	}
	if ref.Toolchain != "" {
		m.Toolchain = ref.Toolchain
	}
	if len(ref.AgentEnvironment) > 0 {
		m.AgentEnvironment = ref.AgentEnvironment
	}
	if mc := ModelConfigFromState(ref.State); len(mc) > 0 {
		m.ModelConfig = mc
	}
	if ref.AgentIdentity != "" {
		m.AgentIdentity = ref.AgentIdentity
	}
	if ref.ClientHost != "" {
		m.ClientHost = ref.ClientHost
	}
	if ref.Stats != nil {
		if tk := TokensFromStats(ref.Stats); len(tk) > 0 {
			m.Tokens = tk
		}
		if tc := ToolCallsFromStats(ref.Stats); tc > 0 {
			m.ToolCalls = tc
		}
		if mc := MessageCountsFromStats(ref.Stats); len(mc) > 0 {
			m.MessageCounts = mc
		}
	}
}

func (u *Uploader) putMetadataLocked(ctx context.Context, st *State) error {
	b, err := json.MarshalIndent(st.Metadata, "", "  ")
	if err != nil {
		return err
	}
	if err := u.store.Put(ctx, MetadataKey(st.SessionKey), append(b, '\n'), JSONContentType); err != nil {
		return fmt.Errorf("write metadata %s: %w", MetadataKey(st.SessionKey), err)
	}
	return nil
}

func (u *Uploader) tick(ctx context.Context) {
	if !u.Enabled() {
		return
	}
	u.mu.Lock()
	src := u.source
	u.mu.Unlock()
	if src == nil {
		return
	}
	ref, err := src(ctx)
	if err != nil || ref == nil || ref.Path == "" {
		return
	}
	if _, err := u.Flush(ctx, ref); err != nil {
		log.Printf("session upload flush failed for %s: %v", ref.Path, err)
	}
}

func (u *Uploader) flushLocked(ctx context.Context, st *State) error {
	st.Shipped = false
	if st.Path == "" {
		return nil
	}
	data, err := os.ReadFile(st.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read session file %s: %w", st.Path, err)
	}
	if int64(len(data)) < st.LastOffset {
		return fmt.Errorf("session file shrank from offset %d to %d", st.LastOffset, len(data))
	}
	end := endOffset(data, st.LastOffset)
	if end <= st.LastOffset {
		return nil
	}
	content := data[st.LastOffset:end]
	rest := content
	partStart := st.LastOffset
	var part []byte
	shippedAny := false
	for len(rest) > 0 {
		var line []byte
		if idx := bytes.IndexByte(rest, '\n'); idx >= 0 {
			line = rest[:idx+1]
			rest = rest[idx+1:]
		} else {
			line = rest
			rest = nil
		}
		if len(part) > 0 && int64(len(part))+int64(len(line)) > u.maxPartBytes {
			if err := u.ship(ctx, st, part, partStart); err != nil {
				return err
			}
			shippedAny = true
			part = nil
			partStart = st.LastOffset
		}
		if len(part) == 0 && int64(len(line)) > u.maxPartBytes {
			if err := u.ship(ctx, st, line, partStart); err != nil {
				return err
			}
			shippedAny = true
			partStart = st.LastOffset
			continue
		}
		part = append(part, line...)
	}
	if len(part) > 0 {
		if err := u.ship(ctx, st, part, partStart); err != nil {
			return err
		}
		shippedAny = true
	}
	st.Shipped = shippedAny
	return nil
}

func (u *Uploader) ship(ctx context.Context, st *State, part []byte, partStart int64) error {
	key := PartKey(st.SessionKey, partStart)
	gz, err := GzipBytes(part)
	if err != nil {
		return fmt.Errorf("gzip part %s: %w", key, err)
	}
	if err := u.store.Put(ctx, key, gz, PartContentType); err != nil {
		return fmt.Errorf("upload part %s: %w", key, err)
	}
	st.hash.Write(part)
	st.HashHex = hex.EncodeToString(st.hash.Sum(nil))
	st.PartCount++
	st.LastOffset = partStart + int64(len(part))
	st.Shipped = true
	return nil
}

func endOffset(data []byte, from int64) int64 {
	start := int(from)
	if start > len(data) {
		return from
	}
	idx := bytes.LastIndexByte(data[start:], '\n')
	if idx < 0 {
		return from
	}
	return int64(start + idx + 1)
}

func sessionKeyFromRef(ref *Ref) string {
	if ref == nil {
		return ""
	}
	if ref.SessionID != "" {
		return ref.SessionID
	}
	return SessionIDFromFile(ref.Path)
}

func cloneState(st *State) *State {
	if st == nil {
		return nil
	}
	cp := *st
	if st.Metadata != nil {
		m := *st.Metadata
		cp.Metadata = &m
	}
	return &cp
}

// GzipBytes compresses b using standard RFC 1952 gzip framing.
func GzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SessionNameFromState reads the human-readable session name from get_state.
func SessionNameFromState(state map[string]any) string {
	if state == nil {
		return ""
	}
	if v, ok := state["sessionName"].(string); ok {
		return v
	}
	if v, ok := state["name"].(string); ok {
		return v
	}
	return ""
}

// ModelConfigFromState reads provider/model settings out of get_state.
func ModelConfigFromState(state map[string]any) map[string]any {
	mc := map[string]any{}
	if state == nil {
		return mc
	}
	if v, ok := state["model"].(map[string]any); ok {
		if provider, ok := v["provider"].(string); ok {
			mc["provider"] = provider
		}
		if id, ok := v["modelId"].(string); ok {
			mc["modelId"] = id
		}
		if id, ok := v["id"].(string); ok {
			mc["modelId"] = id
		}
		for _, k := range []string{"contextWindow", "maxTokens", "baseUrl"} {
			if val, ok := v[k]; ok {
				mc[k] = val
			}
		}
	}
	return mc
}

// TokensFromStats extracts token totals from get_session_stats.
func TokensFromStats(stats map[string]any) map[string]any {
	tokens := map[string]any{}
	if stats == nil {
		return tokens
	}
	if v, ok := stats["tokens"].(map[string]any); ok {
		for k, val := range v {
			tokens[k] = val
		}
	}
	return tokens
}

// ToolCallsFromStats extracts the tool-call total from get_session_stats.
func ToolCallsFromStats(stats map[string]any) int64 {
	if stats == nil {
		return 0
	}
	switch v := stats["toolCalls"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
	}
	return 0
}

// MessageCountsFromStats extracts per-role message counts from get_session_stats.
func MessageCountsFromStats(stats map[string]any) map[string]any {
	counts := map[string]any{}
	if stats == nil {
		return counts
	}
	for _, k := range []string{"userMessages", "assistantMessages", "toolCalls", "toolResults", "totalMessages"} {
		switch v := stats[k].(type) {
		case float64:
			counts[k] = int64(v)
		case int64:
			counts[k] = v
		case json.Number:
			if i, err := v.Int64(); err == nil {
				counts[k] = i
			}
		}
	}
	return counts
}
