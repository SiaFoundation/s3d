package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/multipart"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/indexd/sdk"
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
	if err := store.CreateMultipartUpload(bucket, object, s3.NewUploadID(), nil); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatal(err)
	} else if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload
	uid1 := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid1, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "multipart_uploads")

	// abort the multipart upload
	if err := store.AbortMultipartUpload(bucket, object, uid1); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "multipart_uploads")
}

func TestAddMultipartPart(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		location    = "part-location"
	)

	var contentMD5 [16]byte
	frand.Read(contentMD5[:])

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if _, err := store.AddMultipartPart(bucket, object, s3.NewUploadID(), location, 1, contentMD5, nil, 0); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part (assert no error on duplicate part addition)
	prev, err := store.AddMultipartPart(bucket, object, uid, location, 1, contentMD5, nil, 0)
	if err != nil {
		t.Fatal(err)
	} else if prev != "" {
		t.Fatal("expected empty previous filename for first part upload", prev)
	}

	prev, err = store.AddMultipartPart(bucket, object, uid, location, 1, contentMD5, nil, 0)
	if err != nil {
		t.Fatal(err)
	} else if prev == "" || prev != location {
		t.Fatal("expected previous filename to be returned on part overwrite", prev)
	}

	store.assertCount(1, "multipart_parts")
}

func TestAbortMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		filename    = "part-filename"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if err := store.AbortMultipartUpload(bucket, object, s3.NewUploadID()); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	var contentMD5 [16]byte
	frand.Read(contentMD5[:])

	// add a part
	if _, err := store.AddMultipartPart(bucket, object, uid, filename, 1, contentMD5, nil, 0); err != nil {
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

func TestHasMultipartUpload(t *testing.T) {
	const (
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
	if err := store.HasMultipartUpload(bucket, object, s3.NewUploadID()); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// assert no error for existing upload
	if err := store.HasMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	}
}

func TestListParts(t *testing.T) {
	const (
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
	if _, err := store.ListParts(bucket, object, s3.NewUploadID(), 0, 1000); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add finalized parts
	const totalParts = 5
	for i := 1; i <= totalParts; i++ {
		_, err := store.AddMultipartPart(bucket, object, uid, "", i, frand.Entropy128(), nil, int64(frand.Uint64n(100)+1))
		if err != nil {
			t.Fatal(err)
		}
	}

	// list parts
	result, err := store.ListParts(bucket, object, uid, 0, 1000)
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
		result, err := store.ListParts(bucket, object, uid, partNumberMarker, 1)
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

func TestCompleteMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	obj := sdk.Object{}
	var contentMD5 [16]byte
	frand.Read(contentMD5[:])
	if err := store.PutObject(accessKeyID, bucket, object, &objects.Object{
		ID:         obj.ID(),
		ContentMD5: contentMD5,
		Size:       15,
		UpdatedAt:  time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	partMD5A := frand.Entropy128()
	partMD5B := frand.Entropy128()
	parts := []multipart.Part{
		{PartNumber: 1, Filename: "part-1", Size: 10, MD5: partMD5A},
		{PartNumber: 2, Filename: "part-2", Size: 5, MD5: partMD5B},
	}

	if err := store.CompleteMultipartUpload(bucket, object, uid, parts); err != nil {
		t.Fatal(err)
	}

	store.assertCount(0, "multipart_uploads")
	store.assertCount(len(parts), "object_parts")

	rows, err := store.db.Query(`SELECT part_number, content_md5, offset, length FROM object_parts ORDER BY part_number`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var offsets []int64
	for rows.Next() {
		var partNumber int
		var length int64
		var offset int64
		var contentMD5 sqlMD5
		if err := rows.Scan(&partNumber, &contentMD5, &offset, &length); err != nil {
			t.Fatal(err)
		}
		idx := partNumber - 1
		if idx < 0 || idx >= len(parts) {
			t.Fatalf("unexpected part number %d", partNumber)
		}
		if parts[idx].Size != int64(length) {
			t.Fatalf("expected length %d, got %d", parts[idx].Size, length)
		}
		if parts[idx].MD5 != [16]byte(contentMD5) {
			t.Fatalf("expected MD5 %x, got %x", parts[idx].MD5, contentMD5)
		}
		offsets = append(offsets, offset)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != len(parts) {
		t.Fatalf("expected %d parts, got %d", len(parts), len(offsets))
	}
	if offsets[0] != 0 || offsets[1] != parts[0].Size {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
}
