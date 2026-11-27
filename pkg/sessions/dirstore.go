package sessions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DirStore implements Store over a local filesystem directory. Writes are
// atomic (temp file + rename) and Compose concatenates RFC 1952 gzip streams.
type DirStore struct {
	root string
}

// NewDirStore creates a DirStore rooted at dir, creating the directory if needed.
func NewDirStore(dir string) (*DirStore, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve store dir: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &DirStore{root: abs}, nil
}

func (d *DirStore) resolve(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid store key %q", key)
	}
	full := filepath.Join(d.root, clean)
	rel, err := filepath.Rel(d.root, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("key escapes root %q", key)
	}
	return full, nil
}

func (d *DirStore) Put(_ context.Context, key string, data []byte, _ string) error {
	full, err := d.resolve(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, full)
}

func (d *DirStore) Get(_ context.Context, key string) ([]byte, error) {
	full, err := d.resolve(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(full)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return b, nil
}

func (d *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(d.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(d.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix == "" || strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func (d *DirStore) Delete(_ context.Context, key string) error {
	full, err := d.resolve(key)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (d *DirStore) Compose(ctx context.Context, dstKey string, srcKeys []string, contentType string) error {
	if len(srcKeys) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, k := range srcKeys {
		b, err := d.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("compose read %s: %w", k, err)
		}
		buf.Write(b)
	}
	return d.Put(ctx, dstKey, buf.Bytes(), contentType)
}
