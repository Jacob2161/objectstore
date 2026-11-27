// Package objectstoretest holds checks every objectstore.Store implementation
// must pass, for the implementations' own tests.
package objectstoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/jacob2161/objectstore"
)

// CheckConcurrentCreates checks that of many concurrent PutIfAbsent calls on
// one key exactly one wins and the others get ErrPrecondition, then runs
// CheckConditionalWrites on the winner's object.
func CheckConcurrentCreates(t *testing.T, store objectstore.Store) {
	t.Helper()
	ctx := t.Context()
	const creators = 8
	var group sync.WaitGroup
	results := make([]error, creators)
	etags := make([]string, creators)
	start := make(chan struct{})
	for index := range creators {
		group.Go(func() {
			content := fmt.Appendf(nil, "creator %d", index)
			<-start
			etags[index], results[index] = store.PutIfAbsent(
				ctx,
				conditionalKey,
				bytes.NewReader(content),
				int64(len(content)),
			)
		})
	}
	close(start)
	group.Wait()
	winner := -1
	for index, err := range results {
		switch {
		case err == nil && winner >= 0:
			t.Fatalf("creators %d and %d both created %s", winner, index, conditionalKey)
		case err == nil:
			winner = index
		case !errors.Is(err, objectstore.ErrPrecondition):
			t.Fatalf("creator %d: %v, want ErrPrecondition", index, err)
		}
	}
	if winner < 0 {
		t.Fatalf("no creator won %s", conditionalKey)
	}
	checkReplaces(t, store, fmt.Sprintf("creator %d", winner), etags[winner])
}

// conditionalKey is the object the checks create and replace.
const conditionalKey = "conditional/claim.json"

// CheckConditionalWrites checks PutIfAbsent, PutIfMatch, and GetWithETag one
// call at a time: a second create of a key is refused, a replace with the
// current ETag succeeds, and one with a stale ETag, or of a missing key, is
// refused. Providers that honor conditions but do not serialize concurrent
// ones, such as some test servers, pass this and fail CheckConcurrentCreates.
func CheckConditionalWrites(t *testing.T, store objectstore.Store) {
	t.Helper()
	ctx := t.Context()
	etag, err := store.PutIfAbsent(
		ctx,
		conditionalKey,
		bytes.NewReader([]byte("first")),
		int64(len("first")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutIfAbsent(
		ctx,
		conditionalKey,
		bytes.NewReader([]byte("second")),
		int64(len("second")),
	); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("second create: %v, want ErrPrecondition", err)
	}
	checkReplaces(t, store, "first", etag)
}

// checkReplaces checks the created object, replaces it with its ETag, checks
// that a stale ETag and a missing key are refused, and removes it.
func checkReplaces(t *testing.T, store objectstore.Store, want string, created string) {
	t.Helper()
	ctx := t.Context()
	key := conditionalKey
	content, etag := read(ctx, t, store, key)
	if content != want {
		t.Fatalf("%s holds %q, not %q", key, content, want)
	}
	if etag != created {
		t.Fatalf("GetWithETag returned %q, the create returned %q", etag, created)
	}
	replaced, err := store.PutIfMatch(
		ctx,
		key,
		bytes.NewReader([]byte("replaced")),
		int64(len("replaced")),
		etag,
	)
	if err != nil {
		t.Fatalf("replace with the current ETag: %v", err)
	}
	if _, err := store.PutIfMatch(
		ctx,
		key,
		bytes.NewReader([]byte("stale")),
		int64(len("stale")),
		etag,
	); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("replace with a stale ETag: %v, want ErrPrecondition", err)
	}
	content, etag = read(ctx, t, store, key)
	if content != "replaced" || etag != replaced {
		t.Fatalf(
			"after the replace %s holds %q with ETag %q, want %q with %q",
			key,
			content,
			etag,
			"replaced",
			replaced,
		)
	}
	if _, err := store.PutIfMatch(
		ctx,
		"conditional/missing.json",
		bytes.NewReader([]byte("x")),
		1,
		etag,
	); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("replace of a missing key: %v, want ErrPrecondition", err)
	}
	if _, _, err := store.GetWithETag(ctx, "conditional/missing.json"); !errors.Is(
		err,
		objectstore.ErrNotFound,
	) {
		t.Fatalf("GetWithETag of a missing key: %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	features, err := objectstore.RequireBucketTest(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !features.ConditionalWrites {
		t.Fatal("the bucket test says the store ignores conditional writes")
	}
}

// read returns an object's content and ETag.
func read(ctx context.Context, t *testing.T, store objectstore.Store, key string) (string, string) {
	t.Helper()
	body, etag, err := store.GetWithETag(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(body)
	if closeErr := body.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(content), etag
}
