package filestore

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/jacob2161/objectstore"
)

// Reader reads objects from a directory and never changes it: it has only
// Get and List, and opening one creates nothing.
type Reader struct {
	store *Store
}

// OpenReader opens the objects under prefix in an existing directory, as
// s3store scopes a bucket to its prefix. A missing directory or prefix is
// an error, never created.
func OpenReader(directory string, prefix string) (*Reader, error) {
	prefix = strings.Trim(prefix, "/")
	if strings.Contains(prefix, "..") {
		return nil, fmt.Errorf("invalid prefix %q", prefix)
	}
	root := filepath.Join(directory, filepath.FromSlash(prefix))
	information, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", root, err)
	}
	if !information.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}
	return &Reader{store: &Store{root: root}}, nil
}

// Get opens an object.
func (r *Reader) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return r.store.Get(ctx, key)
}

// List yields objects whose keys start with prefix, in key order, after startAfter.
func (r *Reader) List(
	ctx context.Context,
	prefix string,
	startAfter string,
) iter.Seq2[objectstore.Object, error] {
	return r.store.List(ctx, prefix, startAfter)
}
