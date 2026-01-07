package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math"
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

	// assert [s3errs.ErrNoSuchUpload] is returned if the upload was already aborted
	err = s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)
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

	// assert [s3errs.ErrInvalidArgument] is returned for invalid part number
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, *res.UploadId, 0, data)
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, s3.NewUploadID().String(), 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.UploadPart(t.Context(), bucket, object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.UploadPart(t.Context(), "missing-bucket", object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
}

func TestUploadPartCopy(t *testing.T) {
	// prepare a backend with 2 keypairs
	backend := testutil.NewMemoryBackend(
		testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		testutil.WithKeyPair("other", "foo", "bar"),
	)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucketSrc = "bucket-src"
		bucketDst = "bucket-dst"
		objectSrc = "object-src"
		objectDst = "object-dst"
	)

	// create both buckets
	err1 := s3Tester.CreateBucket(t.Context(), bucketSrc)
	err2 := s3Tester.CreateBucket(t.Context(), bucketDst)
	if err := errors.Join(err1, err2); err != nil {
		t.Fatal(err)
	}

	// prepare two parts
	p1Data := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize))
	p2Data := bytes.Repeat([]byte("b"), int(s3.MinUploadPartSize))

	// upload object to copy parts from
	id, parts := newTestMultipartUpload(t, s3Tester, bucketSrc, objectSrc, [][]byte{p1Data, p2Data})
	_, err := s3Tester.CompleteMultipartUpload(t.Context(), bucketSrc, objectSrc, id, parts)
	if err != nil {
		t.Fatal(err)
	}

	// initiate multipart upload to copy parts to
	res2, err := s3Tester.CreateMultipartUpload(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	} else if res2.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID := *res2.UploadId

	// upload the second part first, copying from the source object
	res3, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, 2, &s3.ObjectRange{
		Start:  s3.MinUploadPartSize / 2,
		Length: s3.MinUploadPartSize,
	})
	if err != nil {
		t.Fatal(err)
	} else if res3.CopyPartResult == nil {
		t.Fatal("expected CopyPartResult in response")
	}

	// upload the first part
	res4, err := s3Tester.UploadPart(t.Context(), bucketDst, objectDst, uploadID, 1, p1Data)
	if err != nil {
		t.Fatal(err)
	}

	// complete the multipart upload
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucketDst, objectDst, uploadID, []types.CompletedPart{
		{
			PartNumber: aws.Int32(1),
			ETag:       res4.ETag,
		},
		{
			PartNumber: aws.Int32(2),
			ETag:       res3.CopyPartResult.ETag,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert final object data is correct
	obj, err := s3Tester.GetObject(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()

	data, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}

	expectedData := append(append(append([]byte{}, p1Data...), p1Data[:s3.MinUploadPartSize/2]...), p2Data[s3.MinUploadPartSize/2:]...)
	if !bytes.Equal(data, expectedData) {
		t.Fatalf("unexpected object data")
	}

	// initiate new multipart upload
	res5, err := s3Tester.CreateMultipartUpload(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	} else if res5.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID = *res5.UploadId

	// upload a part, copy the entire source object
	res6, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, 1, nil)
	if err != nil {
		t.Fatal(err)
	} else if res6.CopyPartResult == nil {
		t.Fatal("expected CopyPartResult in response")
	}

	// complete the multipart upload
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucketDst, objectDst, uploadID, []types.CompletedPart{
		{
			PartNumber: aws.Int32(1),
			ETag:       res6.CopyPartResult.ETag,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert final object data is correct
	obj, err = s3Tester.GetObject(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()

	data, err = io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}

	expectedData = append(append([]byte{}, p1Data...), p2Data...)
	if !bytes.Equal(data, expectedData) {
		t.Fatalf("unexpected object data")
	}

	// assert [s3errs.ErrInvalidArgument] is returned for invalid part number
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, s3.NewUploadID().String(), math.MaxInt32, nil)
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, s3.NewUploadID().String(), 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.UploadPartCopy(t.Context(), "missing-bucket", objectSrc, bucketDst, objectDst, uploadID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrNoSuchKey] is returned for nonexistent source object
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, "missing-object", bucketDst, objectDst, uploadID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
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
	uploadID, parts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data[1:]})

	// re-upload the second part
	uploaded, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 2, p2Data)
	if err != nil {
		t.Fatal(err)
	}
	parts = append(parts, types.CompletedPart{
		ETag:       uploaded.ETag,
		PartNumber: aws.Int32(2),
	})

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
	expectedETag := s3.FormatETag(hash[:], 2)
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
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, s3.NewUploadID().String(), parts)
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
	p3Data := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize))
	uploadID, invalidOrder := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p3Data, p2Data})
	invalidOrder[0], invalidOrder[1] = invalidOrder[1], invalidOrder[0]
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, invalidOrder)
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
		etag := strings.Trim(*got.ETag, `"`)
		if got.PartNumber == nil || *got.PartNumber != int32(i+1) {
			t.Fatalf("part %d: expected part number %d, got %v", i, i+1, got.PartNumber)
		} else if expectedMD5 := md5.Sum(parts[i]); etag != hex.EncodeToString(expectedMD5[:]) {
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
}
