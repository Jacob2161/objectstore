package s3store

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/jacob2161/objectstore"
)

// goog4Signer signs GCS requests with GOOG4-HMAC-SHA256, the HMAC-key
// scheme of GCS's XML API. It is SigV4 with GOOG4 names: x-goog-date and
// x-goog-content-sha256 for x-amz-date and x-amz-content-sha256, and
// "goog4_request" ending the scope. SigV4 itself works for plain requests,
// but GCS refuses an x-goog-* header next to an x-amz-* one
// (ExcessHeaderValues) and ignores the S3 conditions, so generation
// preconditions (x-goog-if-generation-match) need this scheme. It signs the
// host, Content-Type, Content-MD5, and x-goog-* headers only: Google's front
// end rewrites Accept-Encoding before GCS checks the signature.
type goog4Signer struct{}

// SignHTTP signs r in place, as the SDK's V4 signer would.
func (goog4Signer) SignHTTP(
	ctx context.Context,
	credentials aws.Credentials,
	r *http.Request,
	payloadHash string,
	service string,
	region string,
	signingTime time.Time,
	signerSettings ...func(*v4.SignerOptions),
) error {
	for name, values := range r.Header {
		rest, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-")
		if !ok {
			continue
		}
		r.Header.Del(name)
		if rest != "content-sha256" && rest != "date" {
			r.Header["X-Goog-"+http.CanonicalHeaderKey(rest)] = values
		}
	}
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	stamp := signingTime.UTC().Format("20060102T150405Z")
	day := stamp[:8]
	r.Header.Set("X-Goog-Date", stamp)
	r.Header.Set("X-Goog-Content-Sha256", payloadHash)
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	signed := map[string]string{"host": host}
	for name := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-goog-") || lower == "content-type" ||
			lower == "content-md5" {
			signed[lower] = strings.Join(r.Header.Values(name), ",")
		}
	}
	names := slices.Sorted(maps.Keys(signed))
	var headers strings.Builder
	for _, name := range names {
		headers.WriteString(name + ":" + strings.TrimSpace(signed[name]) + "\n")
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	signedHeaders := strings.Join(names, ";")
	canonical := strings.Join(
		[]string{
			r.Method,
			path,
			canonicalQuery(r.URL.Query()),
			headers.String(),
			signedHeaders,
			payloadHash,
		},
		"\n",
	)
	scope := day + "/" + region + "/" + service + "/goog4_request"
	digest := sha256.Sum256([]byte(canonical))
	toSign := "GOOG4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(digest[:])
	key := []byte("GOOG4" + credentials.SecretAccessKey)
	for _, part := range []string{day, region, service, "goog4_request"} {
		key = hmacSHA256(key, part)
	}
	signature := hex.EncodeToString(hmacSHA256(key, toSign))
	r.Header.Set(
		"Authorization",
		"GOOG4-HMAC-SHA256 Credential="+credentials.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature,
	)
	return nil
}

// hmacSHA256 is one step of the signing key derivation.
func hmacSHA256(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}

// canonicalQuery sorts and strictly percent-encodes the query, as SigV4
// requires: every byte but A-Z, a-z, 0-9, '-', '.', '_', and '~'.
func canonicalQuery(query url.Values) string {
	pairs := make([]string, 0, len(query))
	for _, name := range slices.Sorted(maps.Keys(query)) {
		values := slices.Sorted(slices.Values(query[name]))
		for _, value := range values {
			pairs = append(pairs, strictEscape(name)+"="+strictEscape(value))
		}
	}
	return strings.Join(pairs, "&")
}

// strictEscape percent-encodes text for a canonical query.
func strictEscape(text string) string {
	return strings.ReplaceAll(url.QueryEscape(text), "+", "%20")
}

// GCS's XML API ignores If-None-Match and If-Match on uploads, and its ETag
// is the content's MD5, so two writes of the same bytes share one. Its
// conditions are on the object's generation instead, a number that changes
// with every write: x-goog-if-generation-match: 0 creates only a missing
// object, and a generation replaces only that version. On GCS the "ETag"
// PutIfAbsent, PutIfMatch, and GetWithETag pass around is the generation.
const (
	generationHeader      = "X-Goog-Generation"
	generationMatchHeader = "X-Goog-If-Generation-Match"
)

// putIfGeneration uploads only when the object's generation is the one
// given ("0" for none) and returns the new generation.
func (s *Store) putIfGeneration(
	ctx context.Context,
	operation string,
	key string,
	body io.ReadSeeker,
	size int64,
	current string,
) (string, error) {
	if _, err := strconv.ParseInt(current, 10, 64); err != nil {
		return "", fmt.Errorf(
			"%w: %s %s: %q is not a GCS generation",
			objectstore.ErrPrecondition,
			operation,
			key,
			current,
		)
	}
	output, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.prefix + key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
	}, func(clientSettings *s3.Options) {
		clientSettings.APIOptions = append(
			clientSettings.APIOptions,
			smithyhttp.SetHeaderValue(generationMatchHeader, current),
		)
	})
	if err != nil {
		return "", conditionalError(operation, key, err)
	}
	written := generation(output.ResultMetadata)
	if written == "" {
		return "", fmt.Errorf("%s %s: GCS returned no generation", operation, key)
	}
	return written, nil
}

// generation reads the object generation GCS returns with a response.
func generation(metadata middleware.Metadata) string {
	response, ok := awsmiddleware.GetRawResponse(metadata).(*smithyhttp.Response)
	if !ok {
		return ""
	}
	return response.Header.Get(generationHeader)
}
