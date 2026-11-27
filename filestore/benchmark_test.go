package filestore

import (
	"bytes"
	"io"
	"testing"
)

func BenchmarkPutGet(b *testing.B) {
	store, err := NewStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	contents := bytes.Repeat([]byte("x"), 1<<20)
	b.SetBytes(int64(len(contents)))
	for b.Loop() {
		if err := store.Put(b.Context(), "segments/one", bytes.NewReader(contents), int64(len(contents))); err != nil {
			b.Fatal(err)
		}
		reader, err := store.Get(b.Context(), "segments/one")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
