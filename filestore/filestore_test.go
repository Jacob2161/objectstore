package filestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jacob2161/objectstore/objectstoretest"
)

// TestConditionalWrites runs the conditional write checks on a directory.
func TestConditionalWrites(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	objectstoretest.CheckConcurrentCreates(t, store)
}

// TestMutationsWaitOnTheObjectLock holds an object's lock, as a PutIfMatch
// between its ETag comparison and its replacement does, and checks that Put, PutIfAbsent,
// and Delete of the same object each wait for it, while an object in
// another directory is written meanwhile.
func TestMutationsWaitOnTheObjectLock(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	content := []byte("first")
	if err := store.Put(
		ctx,
		"objects/held",
		bytes.NewReader(content),
		int64(len(content)),
	); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		label string
		apply func() error
	}{
		{"Put", func() error {
			return store.Put(ctx, "objects/held", bytes.NewReader([]byte("second")), 6)
		}},
		{"PutIfAbsent", func() error {
			_, err := store.PutIfAbsent(ctx, "objects/held", bytes.NewReader([]byte("third")), 5)
			return err
		}},
		{"Delete", func() error { return store.Delete(ctx, "objects/held") }},
	} {
		t.Run(mutation.label, func(t *testing.T) {
			path, err := store.path("objects/held")
			if err != nil {
				t.Fatal(err)
			}
			unlock, err := store.lockObject(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- mutation.apply() }()
			select {
			case err := <-done:
				unlock()
				t.Fatalf("%s finished while the object's lock was held: %v", mutation.label, err)
			case <-time.After(200 * time.Millisecond):
			}
			unrelatedBody := []byte("unrelated")
			if err := store.Put(
				ctx,
				"elsewhere/other",
				bytes.NewReader(unrelatedBody),
				int64(len(unrelatedBody)),
			); err != nil {
				t.Fatalf("another object's write while the lock was held: %v", err)
			}
			unlock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s never finished after the lock was released", mutation.label)
			}
		})
	}
	if err := store.Delete(ctx, "missing/directory/key"); err != nil {
		t.Fatalf("deleting a key whose directory is missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.root, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleting a missing key created its directory: %v", err)
	}
}

// TestRemoveEmptyPrefixWaitsOnTheObjectLock checks that removing an empty
// prefix waits for a mutation that holds its directory's lock and keeps
// what that mutation wrote, and that a mutation waiting while the directory
// is removed under the lock still lands its object.
func TestRemoveEmptyPrefixWaitsOnTheObjectLock(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	lockedObjectPath, err := store.path("tests/locked")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Dir(lockedObjectPath)

	unlock, err := store.lockObject(t.Context(), lockedObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	removed := make(chan error, 1)
	go func() { removed <- store.RemoveEmptyPrefix(ctx, "tests/") }()
	select {
	case err := <-removed:
		unlock()
		t.Fatalf("RemoveEmptyPrefix finished while the directory's lock was held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	// The mutation that holds the lock writes its object.
	if err := os.WriteFile(lockedObjectPath, []byte("written"), 0o600); err != nil {
		t.Fatal(err)
	}
	unlock()
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lockedObjectPath); err != nil {
		t.Fatalf("RemoveEmptyPrefix removed a directory a mutation wrote into: %v", err)
	}
	if err := store.Delete(ctx, "tests/locked"); err != nil {
		t.Fatal(err)
	}

	// A write that waits for the lock while the directory is removed.
	content := []byte("after removal")
	for _, mutation := range []struct {
		label string
		apply func() error
	}{
		{"Put", func() error {
			return store.Put(ctx, "tests/waiting", bytes.NewReader(content), int64(len(content)))
		}},
		{"PutIfAbsent", func() error {
			_, err := store.PutIfAbsent(
				ctx,
				"tests/waiting",
				bytes.NewReader(content),
				int64(len(content)),
			)
			return err
		}},
	} {
		unlock, err = store.lockObject(t.Context(), lockedObjectPath)
		if err != nil {
			t.Fatal(err)
		}
		written := make(chan error, 1)
		go func() { written <- mutation.apply() }()
		select {
		case err := <-written:
			unlock()
			t.Fatalf("%s finished while the directory's lock was held: %v", mutation.label, err)
		case <-time.After(200 * time.Millisecond):
		}
		if err := os.Remove(directory); err != nil {
			t.Fatal(err)
		}
		unlock()
		if err := <-written; err != nil {
			t.Fatalf("a %s that waited while its directory was removed: %v", mutation.label, err)
		}
		read, err := os.ReadFile(filepath.Join(directory, "waiting"))
		if err != nil || string(read) != string(content) {
			t.Fatalf("the waiting %s's object: %q %v", mutation.label, read, err)
		}
		if err := store.Delete(ctx, "tests/waiting"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RemoveEmptyPrefix(ctx, "tests/"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an empty prefix's directory stayed: %v", err)
	}
}

// TestSymlinkedDirectories checks that objects in a directory reached
// through a symlink are written, and that RemoveEmptyPrefix leaves the
// symlink and its empty target.
func TestSymlinkedDirectories(t *testing.T) {
	target := t.TempDir()
	root := filepath.Join(t.TempDir(), "bucket")
	if err := os.Symlink(target, root); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	content := []byte("through a symlink")
	if err := store.Put(ctx, "top", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Put into a symlinked root: %v", err)
	}
	linkedTarget := t.TempDir()
	if err := os.Symlink(linkedTarget, filepath.Join(target, "linked")); err != nil {
		t.Fatal(err)
	}
	_, err = store.PutIfAbsent(ctx, "linked/object", bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutIfAbsent into a symlinked directory: %v", err)
	}
	if err := store.Delete(ctx, "linked/object"); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveEmptyPrefix(ctx, "linked/"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(target, "linked")); err != nil {
		t.Fatalf("RemoveEmptyPrefix removed a symlink: %v", err)
	}
	if _, err := os.Stat(linkedTarget); err != nil {
		t.Fatalf("RemoveEmptyPrefix removed a symlink's target: %v", err)
	}
}

// TestValidKey checks the keys filestore holds, each naming one file.
func TestValidKey(t *testing.T) {
	for key, want := range map[string]bool{
		"a/b": true, "a..b/c": true, "segments/0001": true,
		"": false, ".": false, "..": false, "a/./b": false, "a/../b": false, "a//b": false,
		"/a": false, "a/": false, ".hidden": false, "a/.tmp": false, "a\\b": false, "a\x00b": false,
	} {
		if got := ValidKey(key); got != want {
			t.Errorf("ValidKey(%q) = %v, want %v", key, got, want)
		}
	}
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), ".hidden", bytes.NewReader(nil), 0); err == nil {
		t.Fatal("a key List would hide was written")
	}
	if err := store.Delete(t.Context(), "."); err == nil {
		t.Fatal("the store root was deletable")
	}
}

// TestListFollowsSymlinkedDirectories lists an object written through a
// symlinked directory, once, even with a symlink cycle.
func TestListFollowsSymlinkedDirectories(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root, filepath.Join(root, "cycle")); err != nil {
		t.Fatal(err)
	}
	content := []byte("listed")
	if err := store.Put(t.Context(), "linked/object", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for object, err := range store.List(t.Context(), "", "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, object.Key)
	}
	if len(keys) != 1 || keys[0] != "linked/object" {
		t.Fatalf("listed %v, want [linked/object]", keys)
	}
}

// TestLockWaitEndsWithTheContext holds a directory's lock and checks that a
// Put waiting for it ends when its context does, writing nothing.
func TestLockWaitEndsWithTheContext(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.path("held/object")
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.lockObject(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = store.Put(ctx, "held/object", bytes.NewReader([]byte("late")), 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a Put behind a held lock: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the waiting Put wrote anyway: %v", err)
	}
}

// TestListAliasedDirectory lists an object under a directory that a symlink
// also reaches, by its own path, whichever path the walk meets first.
func TestListAliasedDirectory(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("aliased")
	if err := store.Put(t.Context(), "z/object", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "z"), filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for object, err := range store.List(t.Context(), "z/", "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, object.Key)
	}
	if len(keys) != 1 || keys[0] != "z/object" {
		t.Fatalf("listed %v under z/, want [z/object]", keys)
	}
}

// TestCancelledPutWritesNothing checks that a Put on a context that already
// ended writes nothing, even with its lock free.
func TestCancelledPutWritesNothing(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Put(ctx, "object", bytes.NewReader([]byte("late")), 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled Put: %v", err)
	}
	if _, err := store.Get(t.Context(), "object"); err == nil {
		t.Fatal("the cancelled Put wrote")
	}
}

// TestRelativeRoot writes under a store rooted at ".", whose ancestors are
// synced like any other root's.
func TestRelativeRoot(t *testing.T) {
	t.Chdir(t.TempDir())
	store, err := NewStore(".")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(store.root) {
		t.Fatalf("root %q is not absolute", store.root)
	}
	if !store.below(filepath.Join(store.root, "nested")) || store.below(store.root) || store.below(filepath.Dir(store.root)) {
		t.Fatal("below misjudges the root")
	}
	if err := store.Put(t.Context(), "nested/object", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatal(err)
	}
}
