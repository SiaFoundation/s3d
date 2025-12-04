package sia_test

import (
	"bytes"
	"crypto/md5"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
)

func TestCreateMultipartUpload(t *testing.T) {
	s3Tester := NewTester(t)

	const (
		bucket = "multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// initiating multipart upload on a missing bucket should fail
	resp, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	} else if resp.Bucket == nil || *resp.Bucket != bucket {
		t.Fatalf("expected bucket %q, got %v", bucket, resp.Bucket)
	}
	if resp.Key == nil || *resp.Key != object {
		t.Fatalf("expected key %q, got %v", object, resp.Key)
	}
	if resp.UploadId == nil || *resp.UploadId == "" {
		t.Fatal("expected upload id in response")
	}

	// assert [s3errs.ErrNoSuchBucket] is returned for missing bucket
	_, err = s3Tester.CreateMultipartUpload(t.Context(), "nonexistent-bucket", object, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.MetadataTooLarge] is returned for too large metadata
	tooLargeMeta := map[string]string{
		"too-much": strings.Repeat("a", s3.MetadataSizeLimit),
	}
	_, err = s3Tester.CreateMultipartUpload(t.Context(), bucket, object, tooLargeMeta)
	testutil.AssertS3Error(t, s3errs.ErrMetadataTooLarge, err)
}

func TestAbortMultipartUpload(t *testing.T) {
	s3Tester := NewTester(t)

	const (
		unknownID = "001f6350ae92ef759626ac909dbc027e"
		bucket    = "abort-multipart-bucket"
		object    = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// initiate multipart upload
	resp, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if resp.UploadId == nil || *resp.UploadId == "" {
		t.Fatal("expected upload id in response")
	}
	uploadID := *resp.UploadId

	// assert [s3errs.ErrNoSuchBucket] is returned for missing bucket
	_, err = s3Tester.CreateMultipartUpload(t.Context(), "nonexistent-bucket", object, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert abort returns [ErrNoSuchUpload] for wrong object
	err = s3Tester.AbortMultipartUpload(t.Context(), bucket, "wrong-object", uploadID)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert abort returns [ErrNoSuchUpload] for wrong uploadID
	err = s3Tester.AbortMultipartUpload(t.Context(), bucket, object, unknownID)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// abort the multipart upload
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}

	// assert abort returns [ErrNoSuchUpload] for already aborted upload
	err = s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)
}

func TestMultipartAddPart(t *testing.T) {
	dataDir := t.TempDir()
	s3Tester := NewCustomTester(t, dataDir)

	const (
		unknownID = "001f6350ae92ef759626ac909dbc027e"
		bucket    = "bucket"
		object    = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload
	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if res.UploadId == nil || *res.UploadId == "" {
		t.Fatal("expected upload id in response")
	}
	uploadID := *res.UploadId

	// prepare a part
	part := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize))
	md5Sum := md5.Sum(part)
	res2, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 1, part)
	if err != nil {
		t.Fatal(err)
	} else if res2 == nil || res2.ETag == nil || *res2.ETag != s3.FormatETag(md5Sum[:]) {
		t.Fatalf("unexpected upload part result: %+v", res2)
	}

	// verify part is on disk
	partDir := filepath.Join(dataDir, sia.MultipartDirectory, uploadID)
	partPath := filepath.Join(partDir, "1.part")
	onDisk, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("failed to read part from disk: %v", err)
	} else if !bytes.Equal(onDisk, part) {
		t.Fatalf("unexpected part contents on disk: got %d bytes", len(onDisk))
	}

	// TODO: verify part metadata in the database

	// assert [s3errs.ErrNoSuchUpload] is returned for wrong upload ID
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, unknownID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// TODO: assert various s3 errors for invalid part uploads
}
