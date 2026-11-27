package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jacob2161/objectstore"
	"github.com/jacob2161/objectstore/filestore"
)

// deniedError looks like a provider's AccessDenied.
type deniedError struct{}

func (deniedError) Error() string     { return "AccessDenied: not allowed" }
func (deniedError) ErrorCode() string { return "AccessDenied" }

// denyDeletes is a store whose key may write but not delete.
type denyDeletes struct {
	objectstore.Store
}

func (denyDeletes) Delete(context.Context, string) error { return deniedError{} }

// TestBucketTests checks that a working bucket passes every test, and
// that a key without delete permission fails that check by name, as access
// denied, with the checks after it skipped.
func TestBucketTests(t *testing.T) {
	store, err := filestore.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	checks := objectstore.TestBucket(t.Context(), store, nil)
	if !checks.Passed() || len(checks) != 8 {
		t.Fatalf("a working bucket: %+v", checks)
	}
	checks = objectstore.TestBucket(t.Context(), denyDeletes{store}, nil)
	failure, failed := checks.FirstFailure()
	if !failed || failure.Name != objectstore.BucketTestDelete ||
		failure.Problem != objectstore.ProblemAccessDenied {
		t.Fatalf("a key that cannot delete: %+v", checks)
	}
	for _, check := range checks {
		if (check.Name == objectstore.BucketTestConditionalWrites ||
			check.Name == objectstore.BucketTestVersioning) &&
			check.Status != objectstore.BucketTestSkipped {
			t.Errorf("%s ran after the delete failed", check.Name)
		}
	}
	if objectstore.Classify(
		errors.New("dial tcp: lookup nowhere: no such host"),
	) != objectstore.ProblemUnreachable {
		t.Error("a DNS failure is not unreachable")
	}
}

// endlessReads is a store whose objects never end, as a hostile endpoint's.
type endlessReads struct {
	objectstore.Store
	readBytes *int64
}

func (s endlessReads) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(&countedZeros{readBytes: s.readBytes}), nil
}

// countedZeros reads zeros forever and counts them.
type countedZeros struct{ readBytes *int64 }

func (z *countedZeros) Read(buffer []byte) (int, error) {
	clear(buffer)
	*z.readBytes += int64(len(buffer))
	return len(buffer), nil
}

// TestReadBackIsLimited checks that the bucket test reads back little more
// than the object it wrote, however much the endpoint sends.
func TestReadBackIsLimited(t *testing.T) {
	store, err := filestore.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var readBytes int64
	checks := objectstore.TestBucket(t.Context(), endlessReads{store, &readBytes}, nil)
	if checks.Passed() {
		t.Fatal("an endless read-back passed")
	}
	if readBytes > 1<<20 {
		t.Fatalf("the bucket test read %d bytes back", readBytes)
	}
}

// ignoresConditions is a store that accepts every conditional create, as an
// S3-compatible server without If-None-Match support does.
type ignoresConditions struct {
	objectstore.Store
}

func (s ignoresConditions) PutIfAbsent(
	ctx context.Context,
	key string,
	body io.ReadSeeker,
	size int64,
) (string, error) {
	if err := s.Store.Put(ctx, key, body, size); err != nil {
		return "", err
	}
	return "etag", nil
}

// TestConditionalWritesAreRequired checks that a store that ignores
// conditional writes fails the bucket test, so it is never saved.
func TestConditionalWritesAreRequired(t *testing.T) {
	store, err := filestore.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	checks := objectstore.TestBucket(t.Context(), ignoresConditions{store}, nil)
	failure, failed := checks.FirstFailure()
	if !failed || failure.Name != objectstore.BucketTestConditionalWrites ||
		failure.Problem != objectstore.ProblemNoConditional {
		t.Fatalf("a store without conditional writes: %+v", checks)
	}
}

// TestBucketTestLeavesNothing checks that a bucket test in a directory
// leaves neither its objects nor their directories, and keeps a
// directory that still holds another object.
func TestBucketTestLeavesNothing(t *testing.T) {
	root := t.TempDir()
	store, err := filestore.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if checks := objectstore.TestBucket(t.Context(), store, nil); !checks.Passed() {
		t.Fatalf("a working bucket: %+v", checks)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the bucket test left %v in %s", entries, root)
	}

	unrelatedObjectContent := []byte("another object")
	unrelatedKey := objectstore.DefaultBucketTestPrefix + "other"
	err = store.Put(
		t.Context(),
		unrelatedKey,
		bytes.NewReader(unrelatedObjectContent),
		int64(len(unrelatedObjectContent)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if checks := objectstore.TestBucket(t.Context(), store, nil); !checks.Passed() {
		t.Fatalf("a bucket with another object under the test prefix: %+v", checks)
	}
	testDirectory := filepath.Join(root, filepath.FromSlash(objectstore.DefaultBucketTestPrefix))
	entries, err = os.ReadDir(testDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "other" {
		t.Fatalf("the bucket test changed %s: %v", testDirectory, entries)
	}
}
