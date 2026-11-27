package objectstore

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

// BucketTestName names one thing TestBucket verifies. An application's own
// tests, added through TestBucketArgs.BeforeWrites, use names of their own.
type BucketTestName string

// The tests, in the order TestBucket runs and reports them.
const (
	BucketTestCredentials       BucketTestName = "credentials"
	BucketTestFound             BucketTestName = "bucket"
	BucketTestList              BucketTestName = "list"
	BucketTestWrite             BucketTestName = "write"
	BucketTestRead              BucketTestName = "read"
	BucketTestDelete            BucketTestName = "delete"
	BucketTestConditionalWrites BucketTestName = "conditional_writes"
	BucketTestVersioning        BucketTestName = "versioning"
)

// BucketTestStatus is how one test ended.
type BucketTestStatus string

const (
	// BucketTestPassed means the bucket does what the test needs.
	BucketTestPassed BucketTestStatus = "passed"
	// BucketTestFailed blocks the bucket from being used.
	BucketTestFailed BucketTestStatus = "failed"
	// BucketTestWarning works but costs money or safety; the bucket may be used.
	BucketTestWarning BucketTestStatus = "warning"
	// BucketTestSkipped did not run because an earlier test failed.
	BucketTestSkipped BucketTestStatus = "skipped"
)

// Problem classifies a failed test, so an interface can say how to fix it.
// An application's own tests may use problems of their own.
type Problem string

const (
	ProblemNone           Problem = ""
	ProblemBadCredentials Problem = "bad_credentials"
	ProblemNoSuchBucket   Problem = "no_such_bucket"
	ProblemAccessDenied   Problem = "access_denied"
	ProblemWrongRegion    Problem = "wrong_region"
	ProblemUnreachable    Problem = "unreachable"
	ProblemMismatch       Problem = "mismatch"
	ProblemNoConditional  Problem = "no_conditional_writes"
	ProblemVersioning     Problem = "versioning"
	ProblemUnrecognized   Problem = "unrecognized"
)

// BucketTest is one line of a bucket test.
type BucketTest struct {
	Name    BucketTestName   `json:"name"`
	Status  BucketTestStatus `json:"status"`
	Problem Problem          `json:"problem,omitempty"`
	// Detail is the provider's own message for a failure, or a note.
	Detail string `json:"detail,omitempty"`
}

// BucketTests is a bucket test's result, in report order.
type BucketTests []BucketTest

// Passed reports that no test failed; warnings still pass.
func (c BucketTests) Passed() bool {
	for _, check := range c {
		if check.Status == BucketTestFailed {
			return false
		}
	}
	return len(c) > 0
}

// FirstFailure returns the first failed test, if any.
func (c BucketTests) FirstFailure() (BucketTest, bool) {
	for _, check := range c {
		if check.Status == BucketTestFailed {
			return check, true
		}
	}
	return BucketTest{}, false
}

// FirstFailureName names the first failed test, or "" when none failed.
func (c BucketTests) FirstFailureName() BucketTestName {
	failure, _ := c.FirstFailure()
	return failure.Name
}

// status returns the test's status by name.
func (c BucketTests) status(name BucketTestName) BucketTestStatus {
	for _, check := range c {
		if check.Name == name {
			return check.Status
		}
	}
	return BucketTestSkipped
}

// errorCoder is what provider API errors carry (smithy.APIError for the S3
// API), matched without importing a provider SDK.
type errorCoder interface {
	error
	ErrorCode() string
}

// Classify sorts a bucket error into a Problem by its provider error code,
// or as unreachable for network failures.
func Classify(err error) Problem {
	if err == nil {
		return ProblemNone
	}
	coded, hasCode := errors.AsType[errorCoder](err)
	if hasCode {
		switch coded.ErrorCode() {
		case "InvalidAccessKeyId",
			"SignatureDoesNotMatch",
			"InvalidToken",
			"ExpiredToken",
			"Unauthorized":
			return ProblemBadCredentials
		case "NoSuchBucket":
			return ProblemNoSuchBucket
		case "AccessDenied", "Forbidden", "AllAccessDisabled":
			return ProblemAccessDenied
		case "AuthorizationHeaderMalformed",
			"PermanentRedirect",
			"IllegalLocationConstraintException":
			return ProblemWrongRegion
		}
	}
	_, isNetwork := errors.AsType[net.Error](err)
	if isNetwork {
		return ProblemUnreachable
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "no such host") ||
		strings.Contains(message, "connection refused") {
		return ProblemUnreachable
	}
	return ProblemUnrecognized
}

// TestBucketArgs configures TestBucket.
type TestBucketArgs struct {
	// Prefix is where the test writes and deletes its objects; nothing else
	// may write under it. Empty uses DefaultBucketTestPrefix.
	Prefix string
	// BeforeWrites, when set, adds a test that must pass before anything is
	// written, such as one that no other installation already uses the
	// bucket.
	BeforeWrites func(context.Context) BucketTest
}

// TestBucket runs every test a bucket must pass, in order, and never stops
// early without saying why: after a failure, the tests that depend on it are
// skipped. It lists first, because a list tells bad credentials and a
// missing bucket apart without writing anything. args may be nil.
func TestBucket(ctx context.Context, store Store, args *TestBucketArgs) BucketTests {
	if args == nil {
		args = &TestBucketArgs{}
	}
	prefix := args.Prefix
	if prefix == "" {
		prefix = DefaultBucketTestPrefix
	}
	bucketTestObjectKey := prefix + "object"
	bucketTestConditionalKey := prefix + "conditional"
	checks := BucketTests{}
	add := func(name BucketTestName, status BucketTestStatus, problem Problem, detail string) {
		checks = append(
			checks,
			BucketTest{Name: name, Status: status, Problem: problem, Detail: detail},
		)
	}
	skipRest := func(names ...BucketTestName) BucketTests {
		for _, name := range names {
			add(name, BucketTestSkipped, ProblemNone, "")
		}
		return checks
	}
	// Listing the check's own prefix checks the list permission.
	var listErr error
	for _, err := range store.List(ctx, prefix, "") {
		if err != nil {
			listErr = err
			break
		}
	}
	switch problem := Classify(listErr); problem {
	case ProblemNone:
		add(BucketTestCredentials, BucketTestPassed, ProblemNone, "")
		add(BucketTestFound, BucketTestPassed, ProblemNone, "")
		add(BucketTestList, BucketTestPassed, ProblemNone, "")
	case ProblemBadCredentials, ProblemUnreachable, ProblemUnrecognized:
		add(BucketTestCredentials, BucketTestFailed, problem, listErr.Error())
		return skipRest(
			BucketTestFound,
			BucketTestList,
			BucketTestWrite,
			BucketTestRead,
			BucketTestDelete,
			BucketTestConditionalWrites,
			BucketTestVersioning,
		)
	case ProblemNoSuchBucket, ProblemWrongRegion:
		add(BucketTestCredentials, BucketTestPassed, ProblemNone, "")
		add(BucketTestFound, BucketTestFailed, problem, listErr.Error())
		return skipRest(
			BucketTestList,
			BucketTestWrite,
			BucketTestRead,
			BucketTestDelete,
			BucketTestConditionalWrites,
			BucketTestVersioning,
		)
	default:
		add(BucketTestCredentials, BucketTestPassed, ProblemNone, "")
		add(BucketTestFound, BucketTestPassed, ProblemNone, "")
		add(BucketTestList, BucketTestFailed, problem, listErr.Error())
	}
	if args.BeforeWrites != nil {
		gate := args.BeforeWrites(ctx)
		checks = append(checks, gate)
		if gate.Status == BucketTestFailed {
			return skipRest(
				BucketTestWrite,
				BucketTestRead,
				BucketTestDelete,
				BucketTestConditionalWrites,
				BucketTestVersioning,
			)
		}
	}
	// A store that keeps directories keeps none of the check's once it ends.
	defer removeEmptyTestPrefix(ctx, store, prefix)
	content := []byte("objectstore bucket test " + time.Now().UTC().Format(time.RFC3339Nano))
	err := store.Put(ctx, bucketTestObjectKey, newBytesReader(content), int64(len(content)))
	if err != nil {
		add(BucketTestWrite, BucketTestFailed, Classify(err), err.Error())
		return skipRest(
			BucketTestRead,
			BucketTestDelete,
			BucketTestConditionalWrites,
			BucketTestVersioning,
		)
	}
	add(BucketTestWrite, BucketTestPassed, ProblemNone, "")
	readErr := readBack(ctx, store, bucketTestObjectKey, content)
	if readErr != nil {
		problem := Classify(readErr)
		if errors.Is(readErr, errMismatch) {
			problem = ProblemMismatch
		}
		add(BucketTestRead, BucketTestFailed, problem, readErr.Error())
	} else {
		add(BucketTestRead, BucketTestPassed, ProblemNone, "")
	}
	err = store.Delete(ctx, bucketTestObjectKey)
	if err != nil {
		add(BucketTestDelete, BucketTestFailed, Classify(err), err.Error())
		return skipRest(BucketTestConditionalWrites, BucketTestVersioning)
	}
	add(BucketTestDelete, BucketTestPassed, ProblemNone, "")
	if checks.status(BucketTestList) == BucketTestFailed ||
		checks.status(BucketTestRead) == BucketTestFailed {
		return skipRest(BucketTestConditionalWrites, BucketTestVersioning)
	}
	conditional, err := testConditionalWrites(ctx, store, bucketTestConditionalKey, content)
	switch {
	case err != nil:
		add(BucketTestConditionalWrites, BucketTestFailed, Classify(err), err.Error())
	case conditional:
		add(BucketTestConditionalWrites, BucketTestPassed, ProblemNone, "")
	default:
		// Every decision processes share through a bucket is a conditional
		// write, so a store that ignores them is refused.
		add(BucketTestConditionalWrites, BucketTestFailed, ProblemNoConditional, "")
	}
	versioning, err := store.Versioning(ctx)
	switch {
	case err != nil:
		add(BucketTestVersioning, BucketTestFailed, Classify(err), err.Error())
	case versioning == VersioningEnabled || versioning == VersioningSuspended:
		add(BucketTestVersioning, BucketTestWarning, ProblemVersioning, string(versioning))
	default:
		add(BucketTestVersioning, BucketTestPassed, ProblemNone, string(versioning))
	}
	return checks
}

// errMismatch reports a test object that read back differently.
var errMismatch = errors.New("the bucket test object read back differently")

// readBack reads the test object and compares it with what was written.
func readBack(ctx context.Context, store Store, bucketTestObjectKey string, content []byte) error {
	reader, err := store.Get(ctx, bucketTestObjectKey)
	if err != nil {
		return err
	}
	// One byte past the test object shows a longer object without reading it all.
	read, err := io.ReadAll(io.LimitReader(reader, int64(len(content))+1))
	if closeErr := reader.Close(); closeErr != nil {
		return closeErr
	}
	if err != nil {
		return err
	}
	if string(read) != string(content) {
		return errMismatch
	}
	return nil
}

// removeEmptyTestPrefix removes the directory a store that keeps them made
// for the test's objects, when it holds nothing. A failure leaves an empty
// directory and nothing else, so it is not a test failure.
func removeEmptyTestPrefix(ctx context.Context, store Store, prefix string) {
	remover, holdsDirectories := store.(EmptyPrefixRemover)
	if !holdsDirectories {
		return
	}
	_ = remover.RemoveEmptyPrefix(ctx, prefix)
}
