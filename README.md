# objectstore

[![Go Reference](https://pkg.go.dev/badge/github.com/jacob2161/objectstore.svg)](https://pkg.go.dev/github.com/jacob2161/objectstore)
[![CI](https://github.com/jacob2161/objectstore/actions/workflows/ci.yml/badge.svg)](https://github.com/jacob2161/objectstore/actions/workflows/ci.yml)

One small interface for object storage buckets, including the conditional
writes that let several processes share a bucket safely, with
implementations for the S3 API and a local directory.

```bash
go get github.com/jacob2161/objectstore
```

| Package | What it is |
|---|---|
| `objectstore` | The `Store` interface, `TestBucket`, and error classification |
| `objectstore/s3store` | AWS S3, Cloudflare R2, and Google Cloud Storage (S3-compatible endpoint), on aws-sdk-go-v2 |
| `objectstore/filestore` | A local directory on Unix-like systems, for development and tests, with atomic, durable writes |
| `objectstore/objectstoretest` | Conformance checks every implementation passes |

## Usage

```go
store, err := s3store.NewStore(&s3store.NewStoreArgs{
	Provider: s3store.ProviderAWS, Region: "us-east-1",
	Bucket: "my-bucket", Prefix: "app", AccessKeyID: id, SecretAccessKey: secret,
})
if err != nil {
	return err
}

// Before relying on a bucket, test what it can do:
checks := objectstore.TestBucket(ctx, store, nil)
// credentials, bucket, list, write, read, delete, conditional_writes, versioning

// Create an object only if it is absent; replace it only if unchanged:
etag, err := store.PutIfAbsent(ctx, "owner.json", body, size)
_, err = store.PutIfMatch(ctx, "owner.json", newBody, newSize, etag)
if errors.Is(err, objectstore.ErrPrecondition) {
	// another process changed it first: read again and decide anew
}
```

- `TestBucket` reports each capability with a `Problem` (bad credentials, no
  such bucket, wrong region, unreachable, no conditional writes, versioning
  on) so an interface can say how to fix it. `BeforeWrites` adds your own
  check before anything is written.
- `List` is an `iter.Seq2`, in key order, resumable with `startAfter`.
- `Versioning` warns about buckets that keep billable noncurrent versions.
- s3store never connects to link-local or cloud metadata addresses, and
  connects to private and loopback ones only with `PrivateAddressesAllowed`,
  checked at dial time, so an endpoint a user typed cannot reach services
  inside the network. It never follows a redirect, since every request is
  signed.

The s3store tests run against an S3 mock in Docker (skipped without Docker),
and against real AWS S3, R2, and GCS buckets with `OBJECTSTORE_BUCKET_TEST=1`.

Tested on the two newest Go releases. Licensed under the MIT license.
