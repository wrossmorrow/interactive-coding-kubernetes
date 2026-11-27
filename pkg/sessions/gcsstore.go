package sessions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// gcsMaxComposeSources is the GCS server-side Compose limit per operation.
const gcsMaxComposeSources = 32

// GCSStore implements Store over Google Cloud Storage using Application Default
// Credentials (GKE Workload Identity on session-hub only) and server-side
// Object.Compose for zero-egress session compaction.
type GCSStore struct {
	client *storage.Client
	bucket string
}

// NewGCSStore creates a GCSStore backed by bucket using Application Default Credentials.
func NewGCSStore(ctx context.Context, bucket string) (*GCSStore, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	return &GCSStore{client: client, bucket: bucket}, nil
}

// Close releases the underlying GCS client.
func (g *GCSStore) Close() error {
	if g.client == nil {
		return nil
	}
	return g.client.Close()
}

func (g *GCSStore) Put(ctx context.Context, key string, data []byte, contentType string) error {
	w := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	if contentType != "" {
		w.ContentType = contentType
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs put %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs put close %s: %w", key, err)
	}
	return nil
}

func (g *GCSStore) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := g.client.Bucket(g.bucket).Object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("gcs get %s: %w", key, err)
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func (g *GCSStore) List(ctx context.Context, prefix string) ([]string, error) {
	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	var keys []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs list %s: %w", prefix, err)
		}
		keys = append(keys, attrs.Name)
	}
	sort.Strings(keys)
	return keys, nil
}

func (g *GCSStore) Delete(ctx context.Context, key string) error {
	err := g.client.Bucket(g.bucket).Object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return err
}

// Compose concatenates srcKeys into dstKey server-side in GCS. If more than 32
// sources are provided, it performs multi-pass tree composition using temporary
// objects under dstKey + ".tmp-compose-*" and cleans them up afterward.
func (g *GCSStore) Compose(ctx context.Context, dstKey string, srcKeys []string, contentType string) error {
	if len(srcKeys) == 0 {
		return nil
	}
	bkt := g.client.Bucket(g.bucket)
	current := append([]string(nil), srcKeys...)
	var temps []string
	defer func() {
		for _, tmp := range temps {
			_ = bkt.Object(tmp).Delete(context.Background())
		}
	}()

	pass := 0
	for len(current) > gcsMaxComposeSources {
		pass++
		var next []string
		for i := 0; i < len(current); i += gcsMaxComposeSources {
			end := i + gcsMaxComposeSources
			if end > len(current) {
				end = len(current)
			}
			chunk := current[i:end]
			if len(chunk) == 1 {
				next = append(next, chunk[0])
				continue
			}
			tmpKey := fmt.Sprintf("%s.tmp-compose-%d-%d", dstKey, pass, i)
			if err := g.composeOnce(ctx, tmpKey, chunk, contentType); err != nil {
				return err
			}
			temps = append(temps, tmpKey)
			next = append(next, tmpKey)
		}
		current = next
	}
	return g.composeOnce(ctx, dstKey, current, contentType)
}

func (g *GCSStore) composeOnce(ctx context.Context, dstKey string, srcKeys []string, contentType string) error {
	bkt := g.client.Bucket(g.bucket)
	srcs := make([]*storage.ObjectHandle, len(srcKeys))
	for i, k := range srcKeys {
		srcs[i] = bkt.Object(k)
	}
	c := bkt.Object(dstKey).ComposerFrom(srcs...)
	if contentType != "" {
		c.ContentType = contentType
	}
	if _, err := c.Run(ctx); err != nil {
		return fmt.Errorf("gcs compose %s: %w", dstKey, err)
	}
	return nil
}
