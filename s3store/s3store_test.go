//go:build linux || darwin || windows || freebsd

package s3store

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jacob2161/objectstore"
	"github.com/jacob2161/objectstore/objectstoretest"
)

// s3Image is an S3-compatible test server; Docker Hub no longer serves MinIO images.
const s3Image = "adobe/s3mock:4.7.0"

// TestS3Compatible runs the probe, conditional writes, listing, and errors
// // against a real S3-compatible server. s3mock honors If-None-Match and
// If-Match one request at a time, but lets two concurrent creates of one key
// both win, so CheckConcurrentCreates runs on filestore here and on real
// AWS S3, GCS, and R2 buckets in TestRealBuckets.
func TestS3Compatible(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := t.Context()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        s3Image,
			ExposedPorts: []string{"9090/tcp"},
			WaitingFor:   wait.ForHTTP("/").WithPort("9090/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("the S3 test server is unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	})
	address, err := container.PortEndpoint(ctx, "9090/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(&NewStoreArgs{
		Provider:        ProviderS3,
		Endpoint:        "http://" + address,
		Bucket:          "objectstore",
		Prefix:          "tenant",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
		// The test server listens on this host.
		PrivateAddressesAllowed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.client.CreateBucket(
		ctx,
		&s3.CreateBucketInput{Bucket: aws.String("objectstore")},
	); err != nil {
		t.Fatal(err)
	}
	features, err := objectstore.RequireBucketTest(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !features.ConditionalWrites {
		t.Fatalf("%s ignores conditional writes", s3Image)
	}
	// s3mock checks each condition but lets concurrent creates of one key
	// both win, so the concurrent check runs on filestore only.
	objectstoretest.CheckConditionalWrites(t, store)
	for _, key := range []string{"objects/a/1", "objects/a/2", "objects/b/1"} {
		if err := store.Put(ctx, key, strings.NewReader(key), int64(len(key))); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	for object, err := range store.List(ctx, "objects/a/", "objects/a/1") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, object.Key)
	}
	if len(keys) != 1 || keys[0] != "objects/a/2" {
		t.Fatalf("listed %v", keys)
	}
	reader, err := store.Get(ctx, "objects/b/1")
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	if err != nil || string(content) != "objects/b/1" || reader.Close() != nil {
		t.Fatal("get round trip")
	}
	if _, err := store.Get(ctx, "missing"); err != objectstore.ErrNotFound {
		t.Fatalf("missing object: %v", err)
	}
	deleted, err := store.DeleteMany(
		ctx,
		[]string{"objects/a/1", "objects/a/2", "objects/missing"},
	)
	if err != nil || len(deleted) != 3 {
		t.Fatalf("batch delete: %v %v", deleted, err)
	}
	keys = nil
	for object, err := range store.List(ctx, "objects/", "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, object.Key)
	}
	if len(keys) != 1 || keys[0] != "objects/b/1" {
		t.Fatalf("listed %v after the batch delete", keys)
	}
	versioning, err := store.Versioning(ctx)
	if err != nil || versioning != objectstore.VersioningDisabled {
		t.Fatalf("versioning of a new bucket: %s %v", versioning, err)
	}
	if _, err := store.client.PutBucketVersioning(
		ctx,
		&s3.PutBucketVersioningInput{
			Bucket: aws.String("objectstore"),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	versioning, err = store.Versioning(ctx)
	if err != nil || versioning != objectstore.VersioningEnabled {
		t.Fatalf("versioning after enabling it: %s %v", versioning, err)
	}
}
