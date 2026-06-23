package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// TestSuspendedDeletePreconditionAgainstCurrentVersion verifies that an
// If-Match-style precondition on a simple delete of a suspended bucket is
// checked against the current version, even when there is no null version to
// remove (which previously bypassed the check entirely).
func TestSuspendedDeletePreconditionAgainstCurrentVersion(t *testing.T) {
	const bucket = "test-bucket"
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, bucket); err != nil {
		t.Fatal(err)
	} else if err := store.PutBucketVersioning(testAccessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	// a real (non-null) current version with a known ETag; no null version exists
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "key", md5, nil, 0, nil); err != nil {
		t.Fatal(err)
	}

	if err := store.PutBucketVersioning(testAccessKeyID, bucket, s3.VersioningStatusSuspended); err != nil {
		t.Fatal(err)
	}

	// a simple delete with a non-matching ETag must fail the precondition,
	// checked against the current version.
	wrongMD5 := frand.Entropy128()
	wrong := s3.FormatETag(wrongMD5[:], 0)
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "key", ETag: &wrong}); !errors.Is(err, s3errs.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// the matching ETag succeeds and inserts a null delete marker
	correct := s3.FormatETag(md5[:], 0)
	versionID, isDeleteMarker, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "key", ETag: &correct})
	if err != nil {
		t.Fatal(err)
	} else if !isDeleteMarker || versionID != s3.Null {
		t.Fatalf("expected a null delete marker, got versionID=%q isDeleteMarker=%v", versionID, isDeleteMarker)
	}
}
