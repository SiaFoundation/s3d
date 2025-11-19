package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
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
	} else if res.UploadId == nil {
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

func TestListMultipartUploads(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("foo", "bar"),
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
	mp1, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, "multipart-upload-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	// assert the upload shows up in the listing
	res, err = s3Tester.ListMultipartUploads(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(res.Uploads))
	} else if *res.Uploads[0].UploadId != *mp1.UploadId {
		t.Fatalf("expected upload ID %q, got %q", *mp1.UploadId, *res.Uploads[0].UploadId)
	}

	// create another multipart upload
	mp2, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, "multipart-upload-2", nil)
	if err != nil {
		t.Fatal(err)
	}

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
	} else if aws.ToString(limited.NextUploadIdMarker) != *mp1.UploadId {
		t.Fatal("expected next upload id marker in response")
	} else if len(limited.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(limited.Uploads))
	} else if *limited.Uploads[0].UploadId != *mp1.UploadId {
		t.Fatalf("expected upload ID %q, got %q", *mp1.UploadId, *limited.Uploads[0].UploadId)
	}

	paginated, err := s3Tester.ListMultipartUploads(t.Context(), bucket, &service.ListMultipartUploadsInput{
		KeyMarker:      limited.NextKeyMarker,
		UploadIdMarker: limited.NextUploadIdMarker,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(paginated.Uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(paginated.Uploads))
	} else if *paginated.Uploads[0].UploadId != *mp2.UploadId {
		t.Fatalf("expected upload ID %q, got %q", *mp2.UploadId, *paginated.Uploads[0].UploadId)
	} else if aws.ToBool(paginated.IsTruncated) {
		t.Fatal("did not expect truncated response")
	}

	// create another multipart upload
	_, err = s3Tester.CreateMultipartUpload(t.Context(), bucket, "non-prefixed-upload-3", nil)
	if err != nil {
		t.Fatal(err)
	}
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

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.ListMultipartUploads(t.Context(), bucket, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.ListMultipartUploads(t.Context(), "missing-bucket", nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
}

func TestCompleteMultipartUpload(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("foo", "bar"),
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

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, "nonexistent-upload", parts)
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
