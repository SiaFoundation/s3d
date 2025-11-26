package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCreateMultipartUpload(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
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

func TestAbortMultipartUpload(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucket = "abort-multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	// initiate multipart upload
	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if res.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID := *res.UploadId

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	err = s3Tester.AbortMultipartUpload(t.Context(), "nonexistent-bucket", object, uploadID)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrAccessDenied] is returned for a bucket we don't own
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	err = otherTester.AbortMultipartUpload(t.Context(), bucket, object, uploadID)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// abort the multipart upload
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}

	// assert we can call abort again without error
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}
}

func TestUploadPart(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
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

	// initiate multipart upload
	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if res.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID := *res.UploadId

	// upload a part
	data := []byte(t.Name())
	part, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 1, data)
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

	// assert [s3errs.ErrNoSuchUpload] is returned for aborted upload
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrInvalidArgument] is returned for invalid part number
	uploadID, _ = newTestMultipartUpload(t, s3Tester, bucket, object, nil)
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 0, data)
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

func TestCompleteMultipartUpload(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucket = "complete-multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// prepare part data
	p1Data := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize))
	p2Data := []byte(t.Name())

	// initiate multipart upload
	uploadID, parts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})

	// complete the multipart upload
	completed, err := s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	if err != nil {
		t.Fatal(err)
	}

	// assert completeMultipartUpload is idempotent
	completed, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	if err != nil {
		t.Fatal(err)
	} else if completed.ETag == nil {
		t.Fatal("expected ETag in completion response")
	}

	// assert final ETag is correct
	p1MD5 := md5.Sum(p1Data)
	p2MD5 := md5.Sum(p2Data)
	combined := make([]byte, 32)
	copy(combined, p1MD5[:])
	copy(combined[16:], p2MD5[:])
	hash := md5.Sum(combined)
	expectedETag := s3.FormatMultipartETag(hash[:], 2)
	if *completed.ETag != expectedETag {
		t.Fatalf("expected final ETag %q, got %q", expectedETag, *completed.ETag)
	}

	// assert object data is correct
	obj, err := s3Tester.GetObject(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()

	data, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	expectedData := append(append([]byte{}, p1Data...), p2Data...)
	if !bytes.Equal(data, expectedData) {
		t.Fatalf("unexpected object data")
	}

	// assert [s3errs.ErrNoSuchUpload] for already completed upload
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, "nonexistent-upload", parts)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for aborted upload
	uploadID, _ = newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), "missing-bucket", object, uploadID, parts)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrEntityTooSmall] is returned if a part is smaller than
	// the minimum size
	uploadID, tooSmallParts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{[]byte("too"), []byte("small")})
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, tooSmallParts)
	testutil.AssertS3Error(t, s3errs.ErrEntityTooSmall, err)

	// assert [s3errs.ErrInvalidPart] is returned for invalid part ETag
	uploadID, invalidParts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})
	invalidParts[1].ETag = aws.String(`"ffffffffffffffffffffffffffffffff"`)
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, invalidParts)
	testutil.AssertS3Error(t, s3errs.ErrInvalidPart, err)

	// assert [s3errs.ErrInvalidPartOrder] is returned for parts out of order
	uploadID, outOfOrderParts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})
	outOfOrderParts[0], outOfOrderParts[1] = outOfOrderParts[1], outOfOrderParts[0]
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, outOfOrderParts)
	testutil.AssertS3Error(t, s3errs.ErrInvalidPartOrder, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	uploadID, parts = newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})
	_, err = otherTester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

func TestListMultipartUploads(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const bucket = "list-multipart-bucket"

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// assert there's no uploads initially
	res, err := s3Tester.ListMultipartUploads(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 0 {
		t.Fatalf("expected no uploads, got %d", len(res.Uploads))
	}

	// create multipart upload
	uploadID1, parts1 := newTestMultipartUpload(t, s3Tester, bucket, "multipart-upload-1", [][]byte{[]byte("part1")})

	// assert the upload shows up in the listing
	res, err = s3Tester.ListMultipartUploads(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(res.Uploads))
	} else if *res.Uploads[0].UploadId != uploadID1 {
		t.Fatalf("expected upload ID %q, got %q", uploadID1, *res.Uploads[0].UploadId)
	}

	// create another multipart upload
	uploadID2, parts2 := newTestMultipartUpload(t, s3Tester, bucket, "multipart-upload-2", [][]byte{[]byte("part1")})

	// assert both uploads show up in the listing
	res, err = s3Tester.ListMultipartUploads(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 2 {
		t.Fatalf("expected 2 uploads, got %d", len(res.Uploads))
	}

	// assert max uploads, key marker and prefix filtering work
	limited, err := s3Tester.ListMultipartUploads(t.Context(), bucket, &service.ListMultipartUploadsInput{
		MaxUploads: aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	} else if !aws.ToBool(limited.IsTruncated) {
		t.Fatal("expected truncated response")
	} else if aws.ToString(limited.NextKeyMarker) != "multipart-upload-1" {
		t.Fatal("expected next key marker in response")
	} else if aws.ToString(limited.NextUploadIdMarker) != uploadID1 {
		t.Fatal("expected next upload id marker in response")
	} else if len(limited.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(limited.Uploads))
	} else if *limited.Uploads[0].UploadId != uploadID1 {
		t.Fatalf("expected upload ID %q, got %q", uploadID1, *limited.Uploads[0].UploadId)
	}

	paginated, err := s3Tester.ListMultipartUploads(t.Context(), bucket, &service.ListMultipartUploadsInput{
		KeyMarker:      limited.NextKeyMarker,
		UploadIdMarker: limited.NextUploadIdMarker,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(paginated.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(paginated.Uploads))
	} else if *paginated.Uploads[0].UploadId != uploadID2 {
		t.Fatalf("expected upload ID %q, got %q", uploadID2, *paginated.Uploads[0].UploadId)
	} else if aws.ToBool(paginated.IsTruncated) {
		t.Fatal("did not expect truncated response")
	}

	// create another multipart upload
	_, _ = newTestMultipartUpload(t, s3Tester, bucket, "non-prefixed-upload-3", [][]byte{[]byte("part1")})
	prefixed, err := s3Tester.ListMultipartUploads(t.Context(), bucket, &service.ListMultipartUploadsInput{
		Prefix: aws.String("multipart-"),
	})
	if err != nil {
		t.Fatal(err)
	} else if len(prefixed.Uploads) != 2 {
		t.Fatalf("expected 2 uploads, got %d", len(prefixed.Uploads))
	}
	for _, upload := range prefixed.Uploads {
		if !strings.HasPrefix(aws.ToString(upload.Key), "multipart-") {
			t.Fatalf("unexpected key in prefix listing: %v", upload.Key)
		}
	}

	// complete the first two uploads
	_, err1 := s3Tester.CompleteMultipartUpload(t.Context(), bucket, "multipart-upload-1", uploadID1, parts1)
	_, err2 := s3Tester.CompleteMultipartUpload(t.Context(), bucket, "multipart-upload-2", uploadID2, parts2)
	if err := errors.Join(err1, err2); err != nil {
		t.Fatal(err)
	}

	// assert only the remaining upload shows up in the listing
	res, err = s3Tester.ListMultipartUploads(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(res.Uploads))
	} else if *res.Uploads[0].Key != "non-prefixed-upload-3" {
		t.Fatalf("expected remaining upload to be 'non-prefixed-upload-3', got %q", *res.Uploads[0].Key)
	}

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.ListMultipartUploads(t.Context(), bucket, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.ListMultipartUploads(t.Context(), "missing-bucket", nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
}

func newTestMultipartUpload(t *testing.T, s3Tester *testutil.S3Tester, bucket, object string, parts [][]byte) (uploadID string, completedParts []types.CompletedPart) {
	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID = *res.UploadId

	for i, part := range parts {
		partNumber := int32(i + 1)
		uploaded, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, partNumber, part)
		if err != nil {
			t.Fatal(err)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploaded.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}
	return
}

func TestListParts(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	const (
		bucket = "multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload
	parts := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("bax"),
	}
	uploadID, _ := newTestMultipartUpload(t, s3Tester, bucket, object, parts)

	// list parts without pagination
	res, err := s3Tester.ListParts(t.Context(), bucket, object, uploadID, nil, nil)
	if err != nil {
		t.Fatal(err)
	} else if res == nil {
		t.Fatal("expected response")
	} else if len(res.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(res.Parts))
	} else if res.IsTruncated == nil || *res.IsTruncated {
		t.Fatal("expected untruncated response")
	} else if res.MaxParts == nil || *res.MaxParts != 1000 {
		t.Fatalf("expected default max parts 1000, got %v", res.MaxParts)
	}

	for i, got := range res.Parts {
		if got.PartNumber == nil || *got.PartNumber != int32(i+1) {
			t.Fatalf("part %d: expected part number %d, got %v", i, i+1, got.PartNumber)
		} else if expectedMD5 := md5.Sum(parts[i]); got.ETag == nil || *got.ETag != hex.EncodeToString(expectedMD5[:]) {
			t.Fatalf("part %d: expected ETag %x, got %v", i, expectedMD5, got.ETag)
		} else if got.Size == nil || *got.Size != int64(len(parts[i])) {
			t.Fatalf("part %d: expected size %d, got %v", i, len(parts[i]), got.Size)
		}
	}

	// list parts with pagination
	maxParts := int32(2)
	paginated, err := s3Tester.ListParts(t.Context(), bucket, object, *res.UploadId, nil, &maxParts)
	if err != nil {
		t.Fatal(err)
	} else if len(paginated.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(paginated.Parts))
	} else if paginated.IsTruncated == nil || !*paginated.IsTruncated {
		t.Fatal("expected truncated response")
	} else if paginated.NextPartNumberMarker == nil {
		t.Fatal("expected next marker")
	} else if paginated.MaxParts == nil || *paginated.MaxParts != maxParts {
		t.Fatalf("expected max parts %d, got %v", maxParts, paginated.MaxParts)
	}

	nextMarker, err := strconv.Atoi(*paginated.NextPartNumberMarker)
	if err != nil {
		t.Fatalf("expected numeric next marker, got %v: %v", *paginated.NextPartNumberMarker, err)
	} else if nextMarker != 2 {
		t.Fatalf("expected next marker 2, got %d", nextMarker)
	}

	// fetch next page
	marker := paginated.NextPartNumberMarker
	nextPage, err := s3Tester.ListParts(t.Context(), bucket, object, *res.UploadId, marker, &maxParts)
	if err != nil {
		t.Fatal(err)
	} else if len(nextPage.Parts) != 1 {
		t.Fatalf("expected final page of 1 part, got %d", len(nextPage.Parts))
	} else if nextPage.IsTruncated == nil || *nextPage.IsTruncated {
		t.Fatal("expected final page to not be truncated")
	} else if nextPage.PartNumberMarker == nil {
		t.Fatalf("expected part number marker %s, got nil", *marker)
	}

	parsedMarker, err := strconv.Atoi(*nextPage.PartNumberMarker)
	if err != nil {
		t.Fatalf("expected numeric part number marker, got %v: %v", *nextPage.PartNumberMarker, err)
	} else if parsedMarker != nextMarker {
		t.Fatalf("expected part number marker %d, got %d", nextMarker, parsedMarker)
	} else if nextPage.Parts[0].PartNumber == nil || *nextPage.Parts[0].PartNumber != 3 {
		t.Fatalf("expected final part number 3, got %v", nextPage.Parts[0].PartNumber)
	}

	// assert we can list parts after aborting the upload
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}
	aborted, err := s3Tester.ListParts(t.Context(), bucket, object, uploadID, nil, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(aborted.Parts) != 0 {
		t.Fatalf("expected 0 parts, got %d", len(aborted.Parts))
	}
}
