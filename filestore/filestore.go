// Package filestore keeps objects as files under a directory, for development and tests.
package filestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jacob2161/atomicfile"
	"github.com/jacob2161/objectstore"
)

// Store is a directory-backed object store.
type Store struct {
	root string
}

// NewStore creates a store rooted at directory, creating it durably when it
// is missing. A root reached through a symlink is resolved once, here.
func NewStore(directory string) (*Store, error) {
	if err := atomicfile.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	return &Store{root: resolved}, nil
}

// ValidKey reports whether filestore can hold key: slash-separated segments,
// none empty, ".", "..", or starting with "." (the store's own temporary
// files), and no backslash or NUL. Every valid key names one file, and List
// returns every object written under one.
func ValidKey(key string) bool {
	if key == "" || strings.ContainsAny(key, "\\\x00") {
		return false
	}
	for segment := range strings.SplitSeq(key, "/") {
		if segment == "" || strings.HasPrefix(segment, ".") {
			return false
		}
	}
	return true
}

// path maps an object key to a file path, refusing keys ValidKey refuses.
func (s *Store) path(key string) (string, error) {
	if !ValidKey(key) {
		return "", fmt.Errorf("invalid key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

// Put writes an object atomically, under the object's lock, so it never
// lands between a PutIfMatch's ETag comparison and its replacement.
func (s *Store) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	content, err := readBody(body, size)
	if err != nil {
		return err
	}
	unlock, err := s.lockObject(ctx, path)
	if err != nil {
		return err
	}
	defer unlock()
	return atomicfile.WriteFile(path, content, 0o600)
}

// readBody reads exactly size bytes of a request body.
func readBody(body io.Reader, size int64) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) != size {
		return nil, fmt.Errorf("body has %d bytes, want %d", len(content), size)
	}
	return content, nil
}

// contentETag is a file's ETag: the SHA-256 of its content, so any change
// to the object changes it.
func contentETag(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// PutIfAbsent writes a hidden temporary file and links it to the key's path
// under the object's lock; the link fails when the path exists, so of
// concurrent creators exactly one wins, even across processes, and none
// lands inside another writer's compare-and-swap.
func (s *Store) PutIfAbsent(
	ctx context.Context,
	key string,
	body io.ReadSeeker,
	size int64,
) (string, error) {
	path, err := s.path(key)
	if err != nil {
		return "", err
	}
	content, err := readBody(body, size)
	if err != nil {
		return "", err
	}
	// The lock comes before the temporary file, so the directory it is
	// written in is the one the object is linked into.
	unlock, err := s.lockObject(ctx, path)
	if err != nil {
		return "", err
	}
	defer unlock()
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".create-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(temporary.Name())
	_, err = temporary.Write(content)
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err := os.Link(temporary.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: %s exists", objectstore.ErrPrecondition, key)
		}
		return "", err
	}
	if err := atomicfile.SyncDirectory(directory); err != nil {
		return "", err
	}
	return contentETag(content), nil
}

// PutIfMatch compares the object's ETag and replaces it atomically under an
// exclusive flock on its directory, which every mutation of the object
// takes, so the comparison and the replacement never interleave with another
// write or delete, in this process or another. A
// missing object does not match.
func (s *Store) PutIfMatch(
	ctx context.Context,
	key string,
	body io.ReadSeeker,
	size int64,
	etag string,
) (string, error) {
	path, err := s.path(key)
	if err != nil {
		return "", err
	}
	content, err := readBody(body, size)
	if err != nil {
		return "", err
	}
	unlock, err := s.lockObject(ctx, path)
	if err != nil {
		return "", err
	}
	defer unlock()
	current, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("%w: %s does not exist", objectstore.ErrPrecondition, key)
	}
	if err != nil {
		return "", err
	}
	if contentETag(current) != etag {
		return "", fmt.Errorf("%w: %s changed", objectstore.ErrPrecondition, key)
	}
	if err := atomicfile.WriteFile(path, content, 0o600); err != nil {
		return "", err
	}
	return contentETag(content), nil
}

// GetWithETag reads an object whole and returns it with its ETag.
func (s *Store) GetWithETag(ctx context.Context, key string) (io.ReadCloser, string, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, "", err
	}
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", objectstore.ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return io.NopCloser(bytes.NewReader(content)), contentETag(content), nil
}

// lockObject takes an exclusive flock on the directory holding the object
// and returns its release, so every mutation of the objects in a directory
// is serialized, in this process and others, without a lock file beside
// each object. A development store loses nothing by the coarser lock.
// RemoveEmptyPrefix may remove the directory, under the same lock, while a
// mutation waits for it: the mutation then starts again on the directory
// the path names now.
func (s *Store) lockObject(ctx context.Context, path string) (func(), error) {
	directory := filepath.Dir(path)
	for range lockAttempts {
		err := atomicfile.MkdirAll(directory, 0o700)
		if errors.Is(err, os.ErrNotExist) {
			// A parent was removed while it was created.
			continue
		}
		if err != nil {
			return nil, err
		}
		file, err := os.Open(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := lockDirectory(ctx, file); err != nil {
			return nil, errors.Join(err, file.Close())
		}
		current, err := directoryCurrent(file, directory)
		if err != nil {
			return nil, errors.Join(err, file.Close())
		}
		if current {
			// Another writer may have created the directories and not yet
			// synced them; the parents' entries are synced here, under the
			// lock, before anything is committed below them.
			if err := s.syncAncestors(directory); err != nil {
				return nil, errors.Join(err, file.Close())
			}
			if err := ctx.Err(); err != nil {
				return nil, errors.Join(err, file.Close())
			}
			return func() {
				// Closing the descriptor releases the flock.
				_ = file.Close()
			}, nil
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%s was removed each time it was locked", directory)
}

// syncAncestors syncs the entry of directory and of each directory above it
// in its parent, up to the store's root, so a path another writer created
// survives a crash before an object is committed under it.
func (s *Store) syncAncestors(directory string) error {
	for current := directory; s.below(current); current = filepath.Dir(current) {
		if err := atomicfile.SyncDirectory(filepath.Dir(current)); err != nil {
			return err
		}
	}
	return nil
}

// below reports whether path lies under the store's root, not at it.
func (s *Store) below(path string) bool {
	relative, err := filepath.Rel(s.root, path)
	return err == nil && relative != "." && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// lockAttempts is how many times lockObject starts again on a directory
// removed while it waited for its lock before it fails.
const lockAttempts = 10

// directoryCurrent reports whether directory still names the directory file
// is open on, so a lock taken on it guards the path. It follows a symlink,
// as opening the directory did.
func directoryCurrent(file *os.File, directory string) (bool, error) {
	opened, err := file.Stat()
	if err != nil {
		return false, err
	}
	named, err := os.Stat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return os.SameFile(opened, named), nil
}

// Get opens an object.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, objectstore.ErrNotFound
	}
	return file, err
}

// List walks the directory and yields matching keys in order.
func (s *Store) List(
	ctx context.Context,
	prefix string,
	startAfter string,
) iter.Seq2[objectstore.Object, error] {
	return func(yield func(objectstore.Object, error) bool) {
		var objects []objectstore.Object
		err := s.listDirectory(ctx, s.root, "", nil, func(key string, information os.FileInfo) {
			if strings.HasPrefix(key, prefix) && key > startAfter {
				objects = append(objects, objectstore.Object{
					Key:      key,
					Size:     information.Size(),
					Modified: information.ModTime(),
				})
			}
		})
		if err != nil {
			yield(objectstore.Object{}, err)
			return
		}
		slices.SortFunc(
			objects,
			func(a, b objectstore.Object) int { return strings.Compare(a.Key, b.Key) },
		)
		for _, object := range objects {
			if !yield(object, nil) {
				return
			}
		}
	}
}

// listDirectory reports every object under directory, whose keys start with
// keyPrefix, following symlinked directories as writes do. ancestors holds
// the directories above it on this path; a symlink back to one of them is a
// cycle and is not followed, while other paths to one directory all are.
func (s *Store) listDirectory(
	ctx context.Context,
	directory string,
	keyPrefix string,
	ancestors []os.FileInfo,
	report func(key string, information os.FileInfo),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directoryInformation, err := os.Stat(directory)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestors {
		if os.SameFile(ancestor, directoryInformation) {
			return nil
		}
	}
	ancestors = append(ancestors, directoryInformation)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !ValidKey(entry.Name()) {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		information, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			// Removed since the directory was read, or a dangling symlink.
			continue
		}
		if err != nil {
			return err
		}
		key := keyPrefix + entry.Name()
		if information.IsDir() {
			if err := s.listDirectory(ctx, path, key+"/", ancestors, report); err != nil {
				return err
			}
			continue
		}
		if information.Mode().IsRegular() {
			report(key, information)
		}
	}
	return nil
}

// Delete removes an object; deleting a missing object succeeds.
func (s *Store) Delete(ctx context.Context, key string) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	// A key whose directory does not exist has nothing to delete; taking
	// its lock would create the directory.
	if _, err := os.Stat(filepath.Dir(path)); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	unlock, err := s.lockObject(ctx, path)
	if err != nil {
		return err
	}
	defer unlock()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// RemoveEmptyPrefix removes the directory under prefix when it holds
// nothing, and succeeds when it is missing, holds anything, or is not a
// directory. It holds the directory's object lock while it looks and
// removes, so no mutation in it runs meanwhile, and one waiting for the
// lock starts again on a new directory.
func (s *Store) RemoveEmptyPrefix(ctx context.Context, prefix string) error {
	directory, err := s.path(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}
	directoryStat, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !directoryStat.IsDir() {
		return nil
	}
	file, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// Closing the descriptor releases the flock.
	defer file.Close()
	if err := lockDirectory(ctx, file); err != nil {
		return err
	}
	current, err := directoryCurrent(file, directory)
	if err != nil || !current {
		return err
	}
	entryNames, err := file.Readdirnames(1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entryNames) > 0 {
		return nil
	}
	err = os.Remove(directory)
	if errors.Is(err, os.ErrNotExist) || directoryNotEmpty(err) {
		return nil
	}
	return err
}

// DeleteMany deletes keys one at a time.
func (s *Store) DeleteMany(ctx context.Context, keys []string) ([]string, error) {
	return objectstore.DeleteEach(ctx, s, keys)
}

// Versioning reports a directory, which keeps no old versions.
func (s *Store) Versioning(ctx context.Context) (objectstore.Versioning, error) {
	return objectstore.VersioningDisabled, nil
}
