package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestCreateMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zap.NewNop())

	// assert [s3errs.ErrNoSuchBucket] for unknown bucket - then create it
	if _, err := store.CreateMultipartUpload(bucket, object, nil); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatal(err)
	} else if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload (and assert no error on duplicate creation)
	uid1, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	uid2, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if uid1 == uid2 {
		t.Fatal("expected unique upload IDs")
	}
	store.assertCount(2, "multipart_uploads")
}

func TestAddMultipartPart(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if err := store.AddMultipartPart(bucket, object, unknownUID, 1); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part (assert no error on duplicate part addition)
	if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	} else if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "multipart_parts")
}

func TestAbortMultipartUpload(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if err := store.AbortMultipartUpload(bucket, object, unknownUID); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part
	if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	}

	// abort the upload
	if err := store.AbortMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "multipart_uploads")
	store.assertCount(0, "multipart_parts")

	// assert [s3errs.ErrNoSuchUpload] for aborted upload
	if err := store.AbortMultipartUpload(bucket, object, uid); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}
}

func TestListParts(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if _, err := store.ListParts(accessKeyID, bucket, object, unknownUID, 0, 1000); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add finalized parts
	const totalParts = 5
	for i := 1; i <= totalParts; i++ {
		if err := store.AddMultipartPart(bucket, object, uid, i); err != nil {
			t.Fatal(err)
		} else if err := store.FinishMultipartPart(bucket, object, uid, i, frand.Entropy128(), nil, int64(frand.Uint64n(100)+1)); err != nil {
			t.Fatal(err)
		}
	}

	// add a non-finalized part
	if err := store.AddMultipartPart(bucket, object, uid, totalParts+1); err != nil {
		t.Fatal(err)
	}

	// list parts
	result, err := store.ListParts(accessKeyID, bucket, object, uid, 0, 1000)
	if err != nil {
		t.Fatal(err)
	} else if result.IsTruncated {
		t.Fatal("expected non-truncated result")
	} else if result.NextPartNumberMarker != "" {
		t.Fatal("expected empty NextPartNumberMarker")
	} else if int64(len(result.Parts)) != totalParts {
		t.Fatalf("expected %d parts, got %d", totalParts, len(result.Parts))
	}
	for i, p := range result.Parts {
		expectedPartNumber := i + 1
		if p.PartNumber != expectedPartNumber {
			t.Fatalf("part %d: expected part number %d, got %d", i, expectedPartNumber, p.PartNumber)
		}
	}

	// paginate through parts
	var partNumberMarker int
	for partNumberMarker < totalParts {
		result, err := store.ListParts(accessKeyID, bucket, object, uid, partNumberMarker, 1)
		if err != nil {
			t.Fatal(err)
		} else if !result.IsTruncated && partNumberMarker < totalParts-1 {
			t.Fatal("expected truncated result")
		} else if result.IsTruncated && partNumberMarker == totalParts-1 {
			t.Fatal("expected non-truncated result")
		} else if int64(len(result.Parts)) != 1 {
			t.Fatalf("expected 1 part, got %d", len(result.Parts))
		} else if result.Parts[0].PartNumber != partNumberMarker+1 {
			t.Fatalf("expected part number %d, got %d", partNumberMarker+1, result.Parts[0].PartNumber)
		}
		partNumberMarker = result.Parts[0].PartNumber
	}
}
