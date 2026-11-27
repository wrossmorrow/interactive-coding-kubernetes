package sessions

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrNotFound is returned by Store.Get when an object does not exist.
var ErrNotFound = errors.New("object not found")

// Store is the object-storage contract shared by DirStore, HTTPStore, and GCSStore.
type Store interface {
	Put(ctx context.Context, key string, data []byte, contentType string) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
	Compose(ctx context.Context, dstKey string, srcKeys []string, contentType string) error
}

// ComposeSession combines all ordered parts/part-*.jsonl.gz objects for
// sessionID into sessions/<id>/compacted.jsonl.gz using Store.Compose.
// Because parts are immutable and offset-ordered, calling ComposeSession again
// after additional pause/rejoin cycles idempotently re-composes the full stream.
func ComposeSession(ctx context.Context, store Store, sessionID string) error {
	if store == nil {
		return nil
	}
	keys, err := store.List(ctx, PartsPrefix(sessionID))
	if err != nil {
		return fmt.Errorf("list parts for %s: %w", sessionID, err)
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := ParsePartOffset(k); ok {
			parts = append(parts, k)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	sort.Strings(parts)
	return store.Compose(ctx, CompactedKey(sessionID), parts, PartContentType)
}
