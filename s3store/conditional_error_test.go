package s3store

import (
	"errors"
	"testing"

	smithy "github.com/aws/smithy-go"

	"github.com/jacob2161/objectstore"
)

// TestConditionalErrorMessages checks that a failed conditional write says
// what it was doing to which key, and that a refused
// condition is objectstore.ErrPrecondition.
func TestConditionalErrorMessages(t *testing.T) {
	transferErr := errors.New("connection reset")
	if err := conditionalError("creating", "owner.json", transferErr); err.Error() !=
		"creating owner.json: connection reset" ||
		errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("a create cut short: %v", err)
	}
	denied := &smithy.GenericAPIError{
		Code:    "PreconditionFailed",
		Message: "At least one condition failed",
	}
	err := conditionalError("replacing", "settings.json", denied)
	if !errors.Is(err, objectstore.ErrPrecondition) ||
		err.Error() != "precondition failed: replacing settings.json: "+denied.Error() {
		t.Fatalf("a replace refused: %v", err)
	}
}
