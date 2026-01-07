package sia_test

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
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
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	s3Tester := NewCustomTester(t, dir, store, NewMemorySDK(), zap.NewNop())

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
	} else if res2 == nil || res2.ETag == nil || *res2.ETag != s3.FormatETag(md5Sum[:], 1) {
		t.Fatalf("unexpected upload part result: %+v", res2)
	}
	// TODO: assert various s3 errors for invalid part uploads

	// verify part is on disk
	entries, err := os.ReadDir(filepath.Join(dir, sia.MultipartDirectory, uploadID, "1"))
	if err != nil {
		t.Fatalf("failed to read part directory: %v", err)
	} else if len(entries) != 1 {
		t.Fatalf("expected 1 part file in directory, got %d", len(entries))
	} else if !strings.HasSuffix(entries[0].Name(), ".part") {
		t.Fatalf("expected part file to have .part suffix, got %q", entries[0].Name())
	}

	// re-upload the part to test part overwrite
	res3, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 1, part)
	if err != nil {
		t.Fatal(err)
	} else if res3 == nil || res3.ETag == nil || *res3.ETag != s3.FormatETag(md5Sum[:], 1) {
		t.Fatalf("unexpected upload part result: %+v", res3)
	}

	// verify only one part file exists
	entries, err = os.ReadDir(filepath.Join(dir, sia.MultipartDirectory, uploadID, "1"))
	if err != nil {
		t.Fatalf("failed to read part directory: %v", err)
	} else if len(entries) != 1 {
		t.Fatalf("expected 1 part file in directory after overwrite, got %d", len(entries))
	}

	// TODO: verify part metadata in the database

	// assert multipart upload is aborted and part files are removed
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, uploadID); err != nil {
		t.Fatal(err)
	}

	// verify multipart upload directory is removed
	_, err = os.Stat(filepath.Join(dir, sia.MultipartDirectory, uploadID))
	if !os.IsNotExist(err) {
		t.Fatalf("expected multipart upload directory to be removed, but it exists")
	}

	// assert [s3errs.ErrNoSuchUpload] is returned for wrong upload ID
	_, err = s3Tester.UploadPart(t.Context(), bucket, object, unknownID, 1, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)
}

func TestMultipartListParts(t *testing.T) {
	s3Tester := NewTester(t)

	const (
		bucket = "list-parts-bucket"
		object = "object"
	)

	var (
		uid = s3.NewUploadID().String()
	)

	// assert [s3errs.ErrNoSuchBucket] for missing bucket
	_, err := s3Tester.ListParts(t.Context(), "nonexistent-bucket", object, uid, nil, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	_, err = s3Tester.ListParts(t.Context(), bucket, object, uid, nil, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// create multipart upload
	resp, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if resp.UploadId == nil || *resp.UploadId == "" {
		t.Fatal("expected upload id in response")
	}
	uploadID := *resp.UploadId

	// add parts
	partData := [][]byte{
		bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("b"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("c"), int(s3.MinUploadPartSize)),
	}
	etags := make([]string, len(partData))
	for i, data := range partData {
		md5Sum := md5.Sum(data)
		res, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, int32(i+1), data)
		if err != nil {
			t.Fatal(err)
		} else if res == nil || res.ETag == nil || *res.ETag != s3.FormatETag(md5Sum[:], 1) {
			t.Fatalf("unexpected upload part result: %+v", res)
		}
		etags[i] = *res.ETag
	}

	// list parts without pagination
	res, err := s3Tester.ListParts(t.Context(), bucket, object, uploadID, nil, nil)
	if err != nil {
		t.Fatal(err)
	} else if *res.IsTruncated {
		t.Fatal("expected non-truncated result")
	} else if len(res.Parts) != len(partData) {
		t.Fatalf("expected %d parts, got %d", len(partData), len(res.Parts))
	}
	for i, p := range res.Parts {
		expectedPartNumber := int32(i + 1)
		if *p.PartNumber != expectedPartNumber {
			t.Fatalf("part %d: expected part number %d, got %d", i, expectedPartNumber, p.PartNumber)
		}
		if *p.ETag != etags[i] {
			t.Fatalf("part %d: expected ETag %q, got %q", i, etags[i], *p.ETag)
		}
	}

	// list parts with pagination
	var partNumberMarker int32
	for partNumberMarker < int32(len(partData)) {
		res, err := s3Tester.ListParts(t.Context(), bucket, object, uploadID, aws.String(strconv.Itoa(int(partNumberMarker))), aws.Int32(1))
		if err != nil {
			t.Fatal(err)
		} else if !*res.IsTruncated && partNumberMarker < int32(len(partData))-1 {
			t.Fatal("expected truncated result")
		} else if *res.IsTruncated && partNumberMarker == int32(len(partData))-1 {
			t.Fatal("expected non-truncated result")
		} else if len(res.Parts) != 1 {
			t.Fatalf("expected 1 part, got %d", len(res.Parts))
		} else if *res.Parts[0].PartNumber != partNumberMarker+1 {
			t.Fatalf("expected part number %d, got %d", partNumberMarker+1, *res.Parts[0].PartNumber)
		}
		partNumberMarker = *res.Parts[0].PartNumber
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	s3Tester := NewCustomTester(t, dir, store, NewMemorySDK(), zap.NewNop())

	const (
		bucket = "complete-multipart-bucket"
		object = "object"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	part1 := bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize))
	part2 := bytes.Repeat([]byte("b"), int(s3.MinUploadPartSize))

	res, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := *res.UploadId

	up1, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 1, part1)
	if err != nil {
		t.Fatal(err)
	}
	up2, err := s3Tester.UploadPart(t.Context(), bucket, object, uploadID, 2, part2)
	if err != nil {
		t.Fatal(err)
	}

	completedParts := []s3Types.CompletedPart{
		{PartNumber: aws.Int32(1), ETag: up1.ETag},
		{PartNumber: aws.Int32(2), ETag: up2.ETag},
	}

	result, err := s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, completedParts)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.ETag == nil || *result.ETag == "" {
		t.Fatalf("unexpected completion result: %#v", result)
	}

	// ensure upload directory is removed
	if _, err := os.Stat(filepath.Join(dir, sia.MultipartDirectory, uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected multipart upload directory to be removed, got err=%v", err)
	}

	// verify object contents
	objRes, err := s3Tester.GetObject(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(objRes.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, append(part1, part2...)) {
		t.Fatalf("unexpected object data length=%d", len(data))
	}
}

func TestMultipartUploadPartCopy(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	s3Tester := NewCustomTester(t, dir, store, NewMemorySDK(), zap.NewNop())

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
	mu, err := s3Tester.CreateMultipartUpload(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	} else if mu.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	uploadID := *mu.UploadId

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
	if got := *res.CopyPartResult.ETag; got != s3.FormatETag(expectedMD5[:], 1) {
		t.Fatalf("expected ETag %q, got %q", s3.FormatETag(expectedMD5[:], 1), got)
	}

	// verify part is written to disk with the expected contents
	partDir := filepath.Join(dir, sia.MultipartDirectory, uploadID, "1")
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

	// assert [s3errs.ErrNoSuchBucket] is returned for missing bucket
	_, err = s3Tester.UploadPartCopy(t.Context(), "missing-bucket", objectSrc, bucketDst, objectDst, uploadID, 1, rnge)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrNoSuchKey] is returned for missing source object
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, "missing-object", bucketDst, objectDst, uploadID, 1, rnge)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, "nonexistent-upload", 1, rnge)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrInvalidRange] is returned for out-of-bounds range
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, 1, &s3.ObjectRange{
		Start:  int64(len(srcData)) - 1,
		Length: 2,
	})
	testutil.AssertS3Error(t, s3errs.ErrInvalidRange, err)

	// assert [s3errs.ErrEntityTooLarge] is returned for oversized range
	if err := store.PutObject(testutil.AccessKeyID, bucketSrc, objectSrc, types.Hash256{}, nil, [16]byte{}, s3.MaxUploadPartSize+1); err != nil {
		t.Fatal(err)
	}
	mu, err = s3Tester.CreateMultipartUpload(t.Context(), bucketDst, objectDst, nil)
	if err != nil {
		t.Fatal(err)
	} else if mu.UploadId == nil {
		t.Fatal("expected upload id in response")
	}
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, *mu.UploadId, 1, &s3.ObjectRange{
		Start:  0,
		Length: s3.MaxUploadPartSize + 1,
	})
	testutil.AssertS3Error(t, s3errs.ErrEntityTooLarge, err)
}
