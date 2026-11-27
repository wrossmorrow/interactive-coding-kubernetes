package sessions

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ReadRawJSONL reconstructs the uncompressed JSONL byte stream for sessionID
// from Store. It prefers compacted.jsonl.gz when present and falls back to
// concatenating all ordered parts/part-*.jsonl.gz objects.
func ReadRawJSONL(ctx context.Context, store Store, sessionID string) ([]byte, error) {
	if store == nil {
		return nil, nil
	}
	if compacted, err := store.Get(ctx, CompactedKey(sessionID)); err == nil {
		return gunzipAll(compacted)
	} else if !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("read compacted transcript for %s: %w", sessionID, err)
	}

	keys, err := store.List(ctx, PartsPrefix(sessionID))
	if err != nil {
		return nil, fmt.Errorf("list transcript parts for %s: %w", sessionID, err)
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := ParsePartOffset(k); ok {
			parts = append(parts, k)
		}
	}
	sort.Strings(parts)
	var out bytes.Buffer
	for _, k := range parts {
		gz, err := store.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("read part %s: %w", k, err)
		}
		raw, err := gunzipAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gunzip part %s: %w", k, err)
		}
		out.Write(raw)
	}
	return out.Bytes(), nil
}

// ReadTranscript reconstructs the transcript for sessionID into the same
// message slice shape returned by pi's `get_messages` RPC command so session-ui
// can render archived/paused sessions with zero running daemons.
func ReadTranscript(ctx context.Context, store Store, sessionID string) ([]map[string]any, error) {
	raw, err := ReadRawJSONL(ctx, store, sessionID)
	if err != nil || len(raw) == 0 {
		return []map[string]any{}, err
	}
	return ParseMessagesFromJSONL(raw), nil
}

// ParseMessagesFromJSONL parses pi session JSONL records (either wrapped
// `{type:"message", message:{...}}` entries or bare `{role:"..."}` objects)
// into a flat list of displayable messages.
func ParseMessagesFromJSONL(raw []byte) []map[string]any {
	msgs := make([]map[string]any, 0)
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		if t, ok := rec["type"].(string); ok {
			switch t {
			case "message":
				if m, ok := rec["message"].(map[string]any); ok {
					msgs = append(msgs, m)
				}
			case "compaction":
				msgs = append(msgs, map[string]any{
					"role":         "compactionSummary",
					"summary":      rec["summary"],
					"tokensBefore": rec["tokensBefore"],
					"timestamp":    rec["timestamp"],
				})
			case "branch_summary":
				msgs = append(msgs, map[string]any{
					"role":      "branchSummary",
					"summary":   rec["summary"],
					"timestamp": rec["timestamp"],
				})
			}
			continue
		}
		if _, hasRole := rec["role"].(string); hasRole {
			msgs = append(msgs, rec)
		}
	}
	return msgs
}

func gunzipAll(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = gr.Close() }()
	var out bytes.Buffer
	if _, err := io.Copy(&out, gr); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
