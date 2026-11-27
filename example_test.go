package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/jacob2161/objectstore"
	"github.com/jacob2161/objectstore/filestore"
)

func Example() {
	directory, err := os.MkdirTemp("", "objectstore-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(directory)
	// s3store.NewStore takes the same place for AWS S3, R2, or GCS.
	store, err := filestore.NewStore(directory)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if _, err := objectstore.RequireBucketTest(ctx, store, nil); err != nil {
		log.Fatal(err)
	}

	// Of two processes creating the same object, exactly one wins.
	owner := []byte(`{"owner":"first"}`)
	if _, err := store.PutIfAbsent(ctx, "owner.json", bytes.NewReader(owner), int64(len(owner))); err != nil {
		log.Fatal(err)
	}
	second := []byte(`{"owner":"second"}`)
	_, err = store.PutIfAbsent(ctx, "owner.json", bytes.NewReader(second), int64(len(second)))
	fmt.Println(errors.Is(err, objectstore.ErrPrecondition))
	// Output: true
}
