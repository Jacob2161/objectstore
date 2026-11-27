package filestore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jacob2161/objectstore"
)

// TestReaderCreatesNothing opens a reader on a missing directory and on a
// prefix, and checks that it reads what a store wrote there and creates no
// file or directory.
func TestReaderCreatesNothing(t *testing.T) {
	ctx := t.Context()
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenReader(missing, ""); err == nil {
		t.Fatal("a reader opened a missing directory")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opening a reader created %s: %v", missing, err)
	}
	directory := t.TempDir()
	store, err := NewStore(filepath.Join(directory, "tenant", "backups"))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("object")
	if err := store.Put(
		ctx,
		"objects/a",
		bytes.NewReader(content),
		int64(len(content)),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(directory, "other"); err == nil {
		t.Fatal("a reader opened a missing prefix")
	}
	if _, err := OpenReader(directory, "../escape"); err == nil {
		t.Fatal("a reader opened a prefix outside its directory")
	}
	reader, err := OpenReader(directory, "/tenant/backups/")
	if err != nil {
		t.Fatal(err)
	}
	body, err := reader.Get(ctx, "objects/a")
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil || !bytes.Equal(read, content) {
		t.Fatalf("read %q: %v", read, err)
	}
	if _, err := reader.Get(ctx, "objects/b"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("a missing object read as %v", err)
	}
	var keys []string
	for listed, err := range reader.List(ctx, "objects/", "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, listed.Key)
	}
	if len(keys) != 1 || keys[0] != "objects/a" {
		t.Fatalf("listed %v", keys)
	}
}
