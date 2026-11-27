// Package s3store implements objectstore.Store over the S3 API for AWS S3,
// Cloudflare R2, GCS (S3-compatible endpoint with HMAC keys), and MinIO.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/jacob2161/objectstore"
)

// Provider selects endpoint and checksum defaults.
type Provider string

const (
	// ProviderAWS is Amazon S3.
	ProviderAWS Provider = "aws"
	// ProviderR2 is Cloudflare R2.
	ProviderR2 Provider = "r2"
	// ProviderGCS is Google Cloud Storage's S3-compatible XML API, with HMAC
	// keys.
	ProviderGCS Provider = "gcs"
	// ProviderS3 is any other S3-compatible service, such as MinIO.
	ProviderS3 Provider = "s3"
)

// gcsEndpoint is the S3-compatible XML API endpoint of Google Cloud Storage.
const gcsEndpoint = "https://storage.googleapis.com"

// NewStoreArgs configures NewStore.
type NewStoreArgs struct {
	// Provider selects endpoint and checksum defaults. Required.
	Provider Provider
	// Endpoint is the service URL. R2 and ProviderS3 require it; GCS
	// defaults to storage.googleapis.com, and AWS to its regional endpoint.
	Endpoint string
	// Region is the bucket's region. AWS requires it; R2 uses "auto", GCS
	// defaults to "auto", and ProviderS3 to us-east-1.
	Region string
	// Bucket is the bucket's name. Required.
	Bucket string
	// Prefix scopes every key to a directory of the bucket, without leading
	// or trailing slashes; "" uses the whole bucket.
	Prefix string
	// AccessKeyID is the credential's ID (an HMAC key ID on GCS).
	AccessKeyID string
	// SecretAccessKey is the credential's secret. It signs every request
	// and is never sent.
	SecretAccessKey string
	// PathStyle puts the bucket in the URL path instead of the host name,
	// as MinIO and most self-hosted servers need.
	PathStyle bool
	// PrivateAddressesAllowed lets the store connect to private, loopback,
	// and unspecified addresses, for a bucket on a LAN or on this host.
	// Without it such an endpoint fails with ErrPrivateEndpoint, so an
	// endpoint a user typed cannot reach services inside the network.
	// Link-local and cloud metadata addresses are refused either way.
	PrivateAddressesAllowed bool
}

// Store is a bucket reached through the S3 API.
type Store struct {
	client   *s3.Client
	bucket   string
	prefix   string
	provider Provider
}

// maximumBatchDelete is the most keys one DeleteObjects request takes.
const maximumBatchDelete = 1000

// NewStore builds a client for the bucket.
func NewStore(args *NewStoreArgs) (*Store, error) {
	if args == nil || args.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	region := args.Region
	endpoint := args.Endpoint
	switch args.Provider {
	case ProviderAWS:
		if region == "" {
			return nil, errors.New("AWS buckets need a region")
		}
	case ProviderR2:
		if endpoint == "" {
			return nil, errors.New("R2 buckets need the account endpoint")
		}
		region = "auto"
	case ProviderGCS:
		if endpoint == "" {
			endpoint = gcsEndpoint
		}
		if region == "" {
			region = "auto"
		}
	case ProviderS3:
		if endpoint == "" {
			return nil, errors.New("S3-compatible buckets need an endpoint")
		}
		if region == "" {
			region = "us-east-1"
		}
	default:
		return nil, fmt.Errorf("unknown provider %q", args.Provider)
	}
	clientSettings := s3.Options{
		Region:       region,
		UsePathStyle: args.PathStyle,
		Credentials: credentials.NewStaticCredentialsProvider(
			args.AccessKeyID,
			args.SecretAccessKey,
			"",
		),
		// R2 and GCS reject the newer default checksum headers on some operations.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		HTTPClient: newHTTPClient(
			addressPolicy{privateAllowed: args.PrivateAddressesAllowed},
		),
	}
	if endpoint != "" {
		clientSettings.BaseEndpoint = aws.String(endpoint)
	}
	if args.Provider == ProviderGCS {
		clientSettings.HTTPSignerV4 = goog4Signer{}
	}
	prefix := strings.Trim(args.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &Store{
		client:   s3.New(clientSettings),
		bucket:   args.Bucket,
		prefix:   prefix,
		provider: args.Provider,
	}, nil
}

// Put uploads an object with a known size.
func (s *Store) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.prefix + key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("writing %s: %w", key, err)
	}
	return nil
}

// PutIfAbsent uploads with If-None-Match: *, so the bucket refuses the
// write when the key exists.
func (s *Store) PutIfAbsent(
	ctx context.Context,
	key string,
	body io.ReadSeeker,
	size int64,
) (string, error) {
	if s.provider == ProviderGCS {
		return s.putIfGeneration(ctx, "creating", key, body, size, "0")
	}
	output, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.prefix + key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		return "", conditionalError("creating", key, err)
	}
	return aws.ToString(output.ETag), nil
}

// PutIfMatch uploads with If-Match: etag, so the bucket refuses the write
// when the object changed or is gone.
func (s *Store) PutIfMatch(
	ctx context.Context,
	key string,
	body io.ReadSeeker,
	size int64,
	etag string,
) (string, error) {
	if s.provider == ProviderGCS {
		return s.putIfGeneration(ctx, "replacing", key, body, size, etag)
	}
	output, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.prefix + key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
		IfMatch:       aws.String(etag),
	})
	if err != nil {
		return "", conditionalError("replacing", key, err)
	}
	return aws.ToString(output.ETag), nil
}

// conditionalError maps a refused condition to objectstore.ErrPrecondition:
// 412 PreconditionFailed, a 409 ConditionalRequestConflict when another
// conditional write to the key was in flight, and a 404 for PutIfMatch on a
// key that is gone.
func conditionalError(operation string, key string, err error) error {
	apiError, ok := errors.AsType[smithy.APIError](err)
	if ok {
		switch apiError.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict", "NoSuchKey":
			return fmt.Errorf("%w: %s %s: %w", objectstore.ErrPrecondition, operation, key, err)
		}
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

// GetWithETag opens an object and returns its ETag, or on GCS its
// generation, which is what PutIfMatch takes there.
func (s *Store) GetWithETag(ctx context.Context, key string) (io.ReadCloser, string, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
	})
	_, ok := errors.AsType[*types.NoSuchKey](err)
	if ok {
		return nil, "", objectstore.ErrNotFound
	}
	if err != nil {
		return nil, "", fmt.Errorf("reading %s: %w", key, err)
	}
	if s.provider == ProviderGCS {
		return output.Body, generation(output.ResultMetadata), nil
	}
	return output.Body, aws.ToString(output.ETag), nil
}

// Get opens an object for reading.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
	})
	_, ok := errors.AsType[*types.NoSuchKey](err)
	if ok {
		return nil, objectstore.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", key, err)
	}
	return output.Body, nil
}

// List pages through ListObjectsV2.
func (s *Store) List(
	ctx context.Context,
	prefix string,
	startAfter string,
) iter.Seq2[objectstore.Object, error] {
	return func(yield func(objectstore.Object, error) bool) {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(s.prefix + prefix),
		}
		if startAfter != "" {
			input.StartAfter = aws.String(s.prefix + startAfter)
		}
		paginator := s3.NewListObjectsV2Paginator(s.client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(objectstore.Object{}, fmt.Errorf("listing %s: %w", prefix, err))
				return
			}
			for _, item := range page.Contents {
				object := objectstore.Object{
					Key:  strings.TrimPrefix(aws.ToString(item.Key), s.prefix),
					Size: aws.ToInt64(item.Size),
				}
				if item.LastModified != nil {
					object.Modified = *item.LastModified
				}
				if !yield(object, nil) {
					return
				}
			}
		}
	}
}

// Delete removes an object. A missing key succeeds: S3 and R2 answer 204,
// and GCS answers 404 NoSuchKey, which is taken as success here.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
	})
	apiError, ok := errors.AsType[smithy.APIError](err)
	if ok && apiError.ErrorCode() == "NoSuchKey" {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting %s: %w", key, err)
	}
	return nil
}

// DeleteMany removes keys with DeleteObjects, up to 1,000 per request. GCS's
// XML API has no multi-object delete, so GCS buckets delete one key at a time.
func (s *Store) DeleteMany(ctx context.Context, keys []string) ([]string, error) {
	if s.provider == ProviderGCS {
		return objectstore.DeleteEach(ctx, s, keys)
	}
	deleted := make([]string, 0, len(keys))
	for start := 0; start < len(keys); start += maximumBatchDelete {
		batch := keys[start:min(start+maximumBatchDelete, len(keys))]
		identifiers := make([]types.ObjectIdentifier, 0, len(batch))
		for _, key := range batch {
			identifiers = append(
				identifiers,
				types.ObjectIdentifier{Key: aws.String(s.prefix + key)},
			)
		}
		output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: identifiers, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return deleted, fmt.Errorf("deleting %d objects: %w", len(batch), err)
		}
		failed := make(map[string]string, len(output.Errors))
		for _, failure := range output.Errors {
			failed[strings.TrimPrefix(aws.ToString(failure.Key), s.prefix)] = aws.ToString(
				failure.Code,
			) + ": " + aws.ToString(
				failure.Message,
			)
		}
		var firstFailure error
		for _, key := range batch {
			reason, ok := failed[key]
			if !ok {
				deleted = append(deleted, key)
				continue
			}
			if firstFailure == nil {
				firstFailure = fmt.Errorf("deleting %s: %s", key, reason)
			}
		}
		if firstFailure != nil {
			return deleted, firstFailure
		}
	}
	return deleted, nil
}

// Versioning reads the bucket's versioning state. R2 has no versioning API,
// and a provider that refuses the call reports unknown rather than failing.
func (s *Store) Versioning(ctx context.Context) (objectstore.Versioning, error) {
	if s.provider == ProviderR2 {
		return objectstore.VersioningUnknown, nil
	}
	output, err := s.client.GetBucketVersioning(
		ctx,
		&s3.GetBucketVersioningInput{Bucket: aws.String(s.bucket)},
	)
	if err != nil {
		if ctx.Err() != nil {
			return objectstore.VersioningUnknown, ctx.Err()
		}
		return objectstore.VersioningUnknown, nil
	}
	switch output.Status {
	case types.BucketVersioningStatusEnabled:
		return objectstore.VersioningEnabled, nil
	case types.BucketVersioningStatusSuspended:
		return objectstore.VersioningSuspended, nil
	}
	return objectstore.VersioningDisabled, nil
}
