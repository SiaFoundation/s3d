package s3_test

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCreateMultipartUpload(t *testing.T) {
	s3Tester := testutil.NewTester(t)

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
	otherTester := s3Tester.AddAccessKey(t, "foo", "bar")
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
	s3Tester := testutil.NewTester(t)

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
	otherTester := s3Tester.AddAccessKey(t, "foo", "bar")
	_, err = otherTester.UploadPart(t.Context(), bucket, object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.UploadPart(t.Context(), "missing-bucket", object, *res.UploadId, 1, data)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
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
