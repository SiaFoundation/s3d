package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
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
	if err := store.CreateBucket(testAccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)
	if err := store.PutBucketVersioning(bid, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	// a real (non-null) current version with a known ETag; no null version exists
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(bid, "key", objects.PutOptions{ContentMD5: md5}); err != nil {
		t.Fatal(err)
	}

	if err := store.PutBucketVersioning(bid, s3.VersioningStatusSuspended); err != nil {
		t.Fatal(err)
	}

	// a simple delete with a non-matching ETag must fail the precondition,
	// checked against the current version.
	wrongMD5 := frand.Entropy128()
	wrong := s3.FormatETag(wrongMD5[:], 0)
	if _, _, _, err := store.DeleteObject(bid, s3.ObjectID{Key: "key", ETag: &wrong}, false); !errors.Is(err, s3errs.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// the matching ETag succeeds and inserts a null delete marker
	correct := s3.FormatETag(md5[:], 0)
	versionID, isDeleteMarker, _, err := store.DeleteObject(bid, s3.ObjectID{Key: "key", ETag: &correct}, false)
	if err != nil {
		t.Fatal(err)
	} else if !isDeleteMarker || versionID != s3.Null {
		t.Fatalf("expected a null delete marker, got versionID=%q isDeleteMarker=%v", versionID, isDeleteMarker)
	}
}

// TestBucketAccessInfo checks that the access info reports the bucket owner and
// its current policy document.
func TestBucketAccessInfo(t *testing.T) {
	const bucket = "test-bucket"
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)
	_, owner, err := store.LoadSecret(testAccessKeyID)
	if err != nil {
		t.Fatal(err)
	}

	const document = `{"Version":"2012-10-17","Statement":[]}`
	if err := store.PutBucketPolicy(bid, document); err != nil {
		t.Fatal(err)
	}
	info, err := store.BucketAccessInfo(bucket)
	if err != nil {
		t.Fatal(err)
	} else if info.Owner.ID != owner || info.PolicyDocument != document {
		t.Fatalf("unexpected access info: %+v", info)
	}
	if _, err := store.BucketAccessInfo("missing"); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatalf("expected %v, got %v", s3errs.ErrNoSuchBucket, err)
	}
}

// TestRecreatedBucketID checks that a recreated bucket gets a new ID, so a
// write authorized against the old bucket is not stored in the new one.
func TestRecreatedBucketID(t *testing.T) {
	const bucket = "test-bucket"
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	old := store.bucketID(bucket)
	if err := store.DeleteBucket(old); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testAccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}

	if store.bucketID(bucket) == old {
		t.Fatal("expected a new bucket ID")
	} else if _, _, err := store.PutObject(old, "key", objects.PutOptions{}); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatalf("expected %v, got %v", s3errs.ErrNoSuchBucket, err)
	}
}
