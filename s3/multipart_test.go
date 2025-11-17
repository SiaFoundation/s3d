package s3_test

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestCreateMultipartUpload(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucket = "multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// initiate multipart upload with custom metadata
	resp, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, map[string]string{
		"foo": "bar",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Bucket == nil || *resp.Bucket != bucket {
		t.Fatalf("expected bucket %q, got %v", bucket, resp.Bucket)
	}
	if resp.Key == nil || *resp.Key != object {
		t.Fatalf("expected key %q, got %v", object, resp.Key)
	}
	if resp.UploadId == nil || *resp.UploadId == "" {
		t.Fatal("expected upload id in response")
	}

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.CreateMultipartUpload(t.Context(), "nonexistent-bucket", object, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrAccessDenied] is returned for a bucket we don't own
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrMetadataTooLarge] is returned for too-large metadata
	tooLargeMeta := map[string]string{
		"too-much": strings.Repeat("a", s3.MetadataSizeLimit),
	}
	_, err = s3Tester.CreateMultipartUpload(t.Context(), bucket, object, tooLargeMeta)
	testutil.AssertS3Error(t, s3errs.ErrMetadataTooLarge, err)
}

func TestUploadPart(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucket = "multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// helper function to initiate a multipart upload
	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.UploadId == nil {
		t.Fatal("expected upload id in response")
	}

	// upload a part
	data := []byte(t.Name())
	part, err := s3Tester.UploadPart(t.Context(), bucket, object, *res.UploadId, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	if part.ETag == nil {
		t.Fatal("expected ETag in response")
	}
	expectedMD5 := md5.Sum(data)
	if got := strings.Trim(*part.ETag, `"`); got != hex.EncodeToString(expectedMD5[:]) {
		t.Fatalf("expected ETag %x, got %q", expectedMD5, *part.ETag)
	}

	// assert [s3errs.ErrInvalidArgument] is returned for invalid part number
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, *res.UploadId, 0, data)
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, "nonexistent-upload", 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.UploadPart(t.Context(), bucket, object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.UploadPart(t.Context(), "missing-bucket", object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
}
