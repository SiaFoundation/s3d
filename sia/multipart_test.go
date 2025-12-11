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
	// TODO: assert various s3 errors for invalid part uploads

	// verify part is on disk
	entries, err := os.ReadDir(filepath.Join(dataDir, sia.MultipartDirectory, uploadID, "1"))
	if err != nil {
		t.Fatalf("failed to read part directory: %v", err)
	} else if len(entries) != 1 {
		t.Fatalf("expected 1 part file in directory, got %d", len(entries))
	} else if !strings.HasSuffix(entries[0].Name(), ".part") {
		t.Fatalf("expected part file to have .part suffix, got %q", entries[0].Name())
	}

	// TODO: verify part metadata in the database

	// assert multipart upload is aborted and part files are removed
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}

	// verify multipart upload directory is removed
	_, err = os.Stat(filepath.Join(dataDir, sia.MultipartDirectory, uploadID))
	if !os.IsNotExist(err) {
		t.Fatalf("expected multipart upload directory to be removed, but it exists")
	}

	// assert [s3errs.ErrNoSuchUpload] is returned for wrong upload ID
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, unknownID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)
}

func TestMultipartUploadPartCopy(t *testing.T) {
	dataDir := t.TempDir()
	s3Tester := NewCustomTester(t, dataDir)

	const (
		bucketSrc = "copy-src"
		bucketDst = "copy-dst"
		objectSrc = "object-src"
		objectDst = "object-dst"
	)

	// create buckets
	if err := s3Tester.CreateBucket(t.Context(), bucketSrc); err != nil {
		t.Fatal(err)
	}
	if err := s3Tester.CreateBucket(t.Context(), bucketDst); err != nil {
		t.Fatal(err)
	}

	// upload source object
	srcData := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize*2))
	if _, err := s3Tester.PutObject(t.Context(), bucketSrc, objectSrc, bytes.NewReader(srcData), nil); err != nil {
		t.Fatal(err)
	}

	// initiate multipart upload on destination bucket
	initRes, err := s3Tester.CreateMultipartUpload(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	} else if initRes.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID := *initRes.UploadId

	// copy a range from the source object into part 1
	rnge := &s3.ObjectRange{
		Start:  s3.MinUploadPartSize / 2,
		Length: s3.MinUploadPartSize,
	}
	res, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, 1, rnge)
	if err != nil {
		t.Fatal(err)
	} else if res.CopyPartResult == nil || res.CopyPartResult.ETag == nil {
		t.Fatal("expected CopyPartResult in response")
	}

	// assert ETag matches the copied data
	expectedData := srcData[rnge.Start : rnge.Start+rnge.Length]
	expectedMD5 := md5.Sum(expectedData)
	if got := *res.CopyPartResult.ETag; got != s3.FormatETag(expectedMD5[:]) {
		t.Fatalf("expected ETag %q, got %q", s3.FormatETag(expectedMD5[:]), got)
	}

	// verify part is written to disk with the expected contents
	partDir := filepath.Join(dataDir, sia.MultipartDirectory, uploadID, "1")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatalf("failed to read part directory: %v", err)
	} else if len(entries) != 1 {
		t.Fatalf("expected 1 part file in directory, got %d", len(entries))
	} else if !strings.HasSuffix(entries[0].Name(), ".part") {
		t.Fatalf("expected part file to have .part suffix, got %q", entries[0].Name())
	}
	partData, err := os.ReadFile(filepath.Join(partDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("failed to read part file: %v", err)
	}
	if !bytes.Equal(partData, expectedData) {
		t.Fatalf("unexpected part data")
	}
}
