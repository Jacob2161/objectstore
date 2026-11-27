package s3store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jacob2161/objectstore"
	"github.com/jacob2161/objectstore/objectstoretest"
)

// realBucket is one provider's test bucket, as JSON in its
// OBJECTSTORE_BUCKET_<PROVIDER> variable.
type realBucket struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	PathStyle       bool   `json:"path_style"`
}

// TestRealBuckets runs the probe and the conditional-write checks, including
// eight concurrent creates of one key, against real AWS S3, GCS, and R2
// whose OBJECTSTORE_BUCKET_<PROVIDER> variable holds its bucket as JSON,
// under a fresh prefix it deletes after.
// is set, under a fresh prefix it deletes after.
func TestRealBuckets(t *testing.T) {
	if os.Getenv("OBJECTSTORE_BUCKET_TEST") != "1" {
		t.Skip("set OBJECTSTORE_BUCKET_TEST=1 and an OBJECTSTORE_BUCKET_<PROVIDER> variable")
	}
	ran := 0
	for _, provider := range []Provider{ProviderAWS, ProviderGCS, ProviderR2} {
		variable := "OBJECTSTORE_BUCKET_" + strings.ToUpper(string(provider))
		document := os.Getenv(variable)
		if document == "" {
			t.Logf("%s is not set; skipping %s", variable, provider)
			continue
		}
		ran++
		t.Run(string(provider), func(t *testing.T) {
			var bucket realBucket
			if err := json.Unmarshal([]byte(document), &bucket); err != nil {
				t.Fatalf("%s: %v", variable, err)
			}
			store, err := NewStore(&NewStoreArgs{
				Provider:        provider,
				Endpoint:        bucket.Endpoint,
				Region:          bucket.Region,
				Bucket:          bucket.Bucket,
				Prefix:          "objectstore-test/" + uuid.NewString(),
				AccessKeyID:     bucket.AccessKeyID,
				SecretAccessKey: bucket.SecretAccessKey,
				PathStyle:       bucket.PathStyle,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { deletePrefix(t, store) })
			features, err := objectstore.RequireBucketTest(t.Context(), store, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !features.ConditionalWrites {
				t.Fatal("the probe says the bucket ignores conditional writes")
			}
			objectstoretest.CheckConditionalWrites(t, store)
			objectstoretest.CheckConcurrentCreates(t, store)
		})
	}
	if ran == 0 {
		t.Fatal("no OBJECTSTORE_BUCKET_<PROVIDER> variable is set")
	}
}

// deletePrefix deletes everything the test wrote under the store's prefix.
func deletePrefix(t *testing.T, store *Store) {
	ctx := context.WithoutCancel(t.Context())
	var keys []string
	for object, err := range store.List(ctx, "", "") {
		if err != nil {
			t.Errorf("listing the test prefix: %v", err)
			return
		}
		keys = append(keys, object.Key)
	}
	if len(keys) == 0 {
		return
	}
	if _, err := store.DeleteMany(ctx, keys); err != nil {
		t.Errorf("deleting the test prefix: %v", err)
	}
}
