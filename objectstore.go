// Package objectstore is one small interface for object storage buckets,
// including the conditional writes that let several processes share a bucket
// safely: create an object only if it is absent, and replace it only if its
// ETag still matches.
//
// Package s3store implements it over the S3 API for AWS S3, Cloudflare R2,
// and Google Cloud Storage's S3-compatible endpoint; package filestore keeps
// objects in a local directory for development and tests; package
// objectstoretest holds the conformance checks every implementation passes.
// TestBucket verifies that a configured bucket does what an application
// needs before it relies on it.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"
)

// ErrNotFound reports a missing object.
var ErrNotFound = errors.New("object not found")

// ErrPrecondition reports a conditional write that lost: the object already
// existed (PutIfAbsent), or it changed or vanished after the caller read its
// ETag (PutIfMatch). The caller reads the object again and decides anew.
var ErrPrecondition = errors.New("precondition failed")

// Object describes a listed object.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Store is a bucket, already scoped to its configured prefix.
type Store interface {
	// Put uploads size bytes from body. Callers pass a seekable body so the
	// SDK can retry without buffering.
	Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// PutIfAbsent uploads an object only when key does not exist yet, and
	// returns its ETag. When it exists, the result is ErrPrecondition, so of
	// two concurrent callers exactly one wins. A provider that ignores the
	// condition overwrites instead; TestBucket reports that.
	PutIfAbsent(ctx context.Context, key string, body io.ReadSeeker, size int64) (string, error)
	// PutIfMatch replaces an object only while its ETag is still etag, and
	// returns the new ETag; otherwise the result is ErrPrecondition.
	PutIfMatch(
		ctx context.Context,
		key string,
		body io.ReadSeeker,
		size int64,
		etag string,
	) (string, error)
	// GetWithETag opens an object and returns the ETag to pass to PutIfMatch.
	GetWithETag(ctx context.Context, key string) (io.ReadCloser, string, error)
	// List yields objects whose keys start with prefix, in key order, after startAfter.
	List(ctx context.Context, prefix string, startAfter string) iter.Seq2[Object, error]
	Delete(ctx context.Context, key string) error
	// DeleteMany removes keys, batching where the provider allows; missing
	// keys succeed. It returns the keys it deleted and the first failure.
	DeleteMany(ctx context.Context, keys []string) ([]string, error)
	// Versioning reports whether the bucket keeps noncurrent versions of
	// deleted objects; providers without the API report VersioningUnknown.
	Versioning(ctx context.Context) (Versioning, error)
}

// Versioning is a bucket's object versioning state.
type Versioning string

const (
	// VersioningEnabled keeps billable noncurrent versions after a delete
	// unless a lifecycle rule expires them.
	VersioningEnabled Versioning = "enabled"
	// VersioningSuspended keeps the noncurrent versions written while it was enabled.
	VersioningSuspended Versioning = "suspended"
	// VersioningDisabled deletes objects outright.
	VersioningDisabled Versioning = "disabled"
	// VersioningUnknown is a provider without the versioning API, such as R2.
	VersioningUnknown Versioning = "unknown"
)

// DeleteEach deletes keys one at a time, for providers without batch deletes.
func DeleteEach(ctx context.Context, store Store, keys []string) ([]string, error) {
	deleted := make([]string, 0, len(keys))
	for _, key := range keys {
		if err := store.Delete(ctx, key); err != nil {
			return deleted, err
		}
		deleted = append(deleted, key)
	}
	return deleted, nil
}

// DefaultBucketTestPrefix is the key prefix TestBucket writes and deletes
// its objects under when its arguments name none.
const DefaultBucketTestPrefix = "objectstore-bucket-test/"

// EmptyPrefixRemover is a store that keeps directories, such as filestore,
// and can remove the one under a key prefix once it holds nothing.
type EmptyPrefixRemover interface {
	// RemoveEmptyPrefix removes prefix's directory if it is empty, and
	// succeeds when it is missing or holds anything.
	RemoveEmptyPrefix(ctx context.Context, prefix string) error
}

// BucketFeatures is what a bucket test learned about a bucket beyond its
// permissions.
type BucketFeatures struct {
	// ConditionalWrites reports that the bucket refuses a PutIfAbsent on an
	// existing key and a PutIfMatch with a stale ETag. Without them, two
	// processes creating one object at once can both believe they won.
	ConditionalWrites bool
}

// RequireBucket runs TestBucket and returns its first failure as an error,
// for callers that need a yes or no. args may be nil.
func RequireBucketTest(ctx context.Context, store Store, args *TestBucketArgs) (*BucketFeatures, error) {
	checks := TestBucket(ctx, store, args)
	failure, failed := checks.FirstFailure()
	if failed {
		return nil, fmt.Errorf(
			"bucket test %s failed: %s",
			failure.Name,
			failure.Detail,
		)
	}
	return &BucketFeatures{
		ConditionalWrites: checks.status(BucketTestConditionalWrites) == BucketTestPassed,
	}, nil
}

// testConditionalWrites creates the conditional test object, then checks
// that a second create and a replace with a stale ETag are both refused.
func testConditionalWrites(ctx context.Context, store Store, bucketTestConditionalKey string, content []byte) (bool, error) {
	if err := store.Delete(ctx, bucketTestConditionalKey); err != nil {
		return false, err
	}
	created, err := store.PutIfAbsent(
		ctx,
		bucketTestConditionalKey,
		newBytesReader(content),
		int64(len(content)),
	)
	if err != nil {
		return false, err
	}
	denied := true
	_, err = store.PutIfAbsent(
		ctx,
		bucketTestConditionalKey,
		newBytesReader(content),
		int64(len(content)),
	)
	switch {
	case errors.Is(err, ErrPrecondition):
	case err != nil:
		return false, errors.Join(err, store.Delete(ctx, bucketTestConditionalKey))
	default:
		denied = false
	}
	replacement := append([]byte("replaced "), content...)
	replaced, err := store.PutIfMatch(
		ctx,
		bucketTestConditionalKey,
		newBytesReader(replacement),
		int64(len(replacement)),
		created,
	)
	if err != nil && !errors.Is(err, ErrPrecondition) {
		return false, errors.Join(err, store.Delete(ctx, bucketTestConditionalKey))
	}
	if err == nil {
		// The ETag from the create was current; one from before the replace is stale now.
		_, err = store.PutIfMatch(
			ctx,
			bucketTestConditionalKey,
			newBytesReader(content),
			int64(len(content)),
			created,
		)
		switch {
		case errors.Is(err, ErrPrecondition):
		case err != nil:
			return false, errors.Join(err, store.Delete(ctx, bucketTestConditionalKey))
		default:
			denied = false
		}
		denied = denied && replaced != created
	} else {
		// A refused replace with the current ETag means the condition is not
		// understood either.
		denied = false
	}
	return denied, store.Delete(ctx, bucketTestConditionalKey)
}
