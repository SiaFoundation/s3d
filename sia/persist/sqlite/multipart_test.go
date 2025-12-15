package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
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
