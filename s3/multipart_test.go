package s3_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
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
	"lukechampine.com/frand"
)

func TestCreateMultipartUpload(t *testing.T) {
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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

	crc32Checksum := func(b []byte) string {
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.ChecksumIEEE(b))
		return base64.StdEncoding.EncodeToString(sum[:])
	}

	// upload a part with a checksum that does not match the data
	_, err = s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(object),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ChecksumCRC32: aws.String(crc32Checksum(frand.Bytes(100))),
	})
	testutil.AssertS3Error(t, s3errs.ErrBadDigest, err)

	// upload a part with a checksum the client computed itself
	checksummed, err := s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(object),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ChecksumCRC32: aws.String(crc32Checksum(data)),
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(checksummed.ChecksumCRC32); got != crc32Checksum(data) {
		t.Fatalf("checksum mismatch: expected %s, got %s", crc32Checksum(data), got)
	}

	// assert [s3errs.ErrInvalidRequest] is returned for two checksums
	sha256Sum := sha256.Sum256(data)
	_, err = s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(object),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(1),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumCRC32:  aws.String(crc32Checksum(data)),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(sha256Sum[:])),
	})
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

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

func TestMultipartChecksum(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	const (
		bucket = "checksum-bucket"
		object = "object"
		source = "source"
	)

	// create target bucket
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	createUpload := func(algorithm types.ChecksumAlgorithm, checksumType types.ChecksumType) (*service.CreateMultipartUploadOutput, error) {
		return s3Tester.Client().CreateMultipartUpload(t.Context(), &service.CreateMultipartUploadInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(object),
			ChecksumAlgorithm: algorithm,
			ChecksumType:      checksumType,
		})
	}

	// assert [s3errs.ErrInvalidRequest] for a type the algorithm does not support
	_, err := createUpload(types.ChecksumAlgorithmSha256, types.ChecksumTypeFullObject)
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

	// assert [s3errs.ErrInvalidRequest] for a type without an algorithm
	_, err = createUpload("", types.ChecksumTypeComposite)
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

	// an algorithm without a type takes the algorithm's default
	fullObject, err := createUpload(types.ChecksumAlgorithmCrc64nvme, "")
	if err != nil {
		t.Fatal(err)
	} else if fullObject.ChecksumAlgorithm != types.ChecksumAlgorithmCrc64nvme {
		t.Fatalf("expected algorithm %q, got %q", types.ChecksumAlgorithmCrc64nvme, fullObject.ChecksumAlgorithm)
	} else if fullObject.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeFullObject, fullObject.ChecksumType)
	}
	if err := s3Tester.AbortMultipartUpload(t.Context(), bucket, object, *fullObject.UploadId); err != nil {
		t.Fatal(err)
	}

	res, err := createUpload(types.ChecksumAlgorithmSha256, "")
	if err != nil {
		t.Fatal(err)
	} else if res.ChecksumAlgorithm != types.ChecksumAlgorithmSha256 {
		t.Fatalf("expected algorithm %q, got %q", types.ChecksumAlgorithmSha256, res.ChecksumAlgorithm)
	} else if res.ChecksumType != types.ChecksumTypeComposite {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeComposite, res.ChecksumType)
	}
	uploadID := *res.UploadId

	// the SHA256 of 1024 'A' bytes
	const partSHA256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="
	data := bytes.Repeat([]byte("A"), 1024)

	// a part carrying the upload's algorithm is echoed back
	part1, err := s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(object),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(1),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumSHA256: aws.String(partSHA256),
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(part1.ChecksumSHA256); got != partSHA256 {
		t.Fatalf("expected checksum %s, got %s", partSHA256, got)
	}

	// a part carrying no checksum still reports the upload's algorithm
	part2, err := s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(object),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(2),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}, func(o *service.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(part2.ChecksumSHA256); got != partSHA256 {
		t.Fatalf("expected checksum %s, got %s", partSHA256, got)
	}

	// assert [s3errs.ErrBadDigest] for a part whose checksum does not cover it
	_, err = s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(object),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(3),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(frand.Bytes(32))),
	})
	testutil.AssertS3Error(t, s3errs.ErrBadDigest, err)

	// assert [s3errs.ErrInvalidRequest] for a part carrying another algorithm
	var crc32Sum [4]byte
	binary.BigEndian.PutUint32(crc32Sum[:], crc32.ChecksumIEEE(data))
	_, err = s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(object),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(3),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ChecksumCRC32: aws.String(base64.StdEncoding.EncodeToString(crc32Sum[:])),
	})
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

	// a copied part is hashed with the upload's algorithm
	sourceData := frand.Bytes(6 << 20)
	if _, err := s3Tester.PutObject(t.Context(), bucket, source, bytes.NewReader(sourceData), nil); err != nil {
		t.Fatal(err)
	}
	copyRange := s3.ObjectRange{Start: 1 << 20, Length: 5 << 20}
	copied, err := s3Tester.UploadPartCopy(t.Context(), bucket, source, bucket, object, uploadID, testutil.UploadPartCopyOptions{
		PartNumber: 3,
		Range:      &copyRange,
	})
	if err != nil {
		t.Fatal(err)
	} else if copied.CopyPartResult == nil {
		t.Fatal("expected CopyPartResult in response")
	}
	copiedSum := sha256.Sum256(sourceData[copyRange.Start : copyRange.Start+copyRange.Length])
	expectedCopied := base64.StdEncoding.EncodeToString(copiedSum[:])
	if got := aws.ToString(copied.CopyPartResult.ChecksumSHA256); got != expectedCopied {
		t.Fatalf("expected checksum %s, got %s", expectedCopied, got)
	}

	// the listing reports the upload's algorithm, its type and every digest
	listed, err := s3Tester.ListParts(t.Context(), bucket, object, uploadID, nil, nil)
	if err != nil {
		t.Fatal(err)
	} else if listed.ChecksumAlgorithm != types.ChecksumAlgorithmSha256 {
		t.Fatalf("expected algorithm %q, got %q", types.ChecksumAlgorithmSha256, listed.ChecksumAlgorithm)
	} else if listed.ChecksumType != types.ChecksumTypeComposite {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeComposite, listed.ChecksumType)
	} else if len(listed.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(listed.Parts))
	}
	expectedSums := []string{partSHA256, partSHA256, expectedCopied}
	for i, part := range listed.Parts {
		if got := aws.ToString(part.ChecksumSHA256); got != expectedSums[i] {
			t.Fatalf("expected part %d checksum %s, got %s", i+1, expectedSums[i], got)
		}
	}

	createChecksumUpload := func(key string, algorithm types.ChecksumAlgorithm, checksumType types.ChecksumType) string {
		t.Helper()
		created, err := s3Tester.Client().CreateMultipartUpload(t.Context(), &service.CreateMultipartUploadInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(key),
			ChecksumAlgorithm: algorithm,
			ChecksumType:      checksumType,
		})
		if err != nil {
			t.Fatal(err)
		}
		return *created.UploadId
	}

	uploadChecksumPart := func(key, id string, partNumber int32, body []byte, apply func(*service.UploadPartInput)) *string {
		t.Helper()
		input := &service.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(id),
			PartNumber:    aws.Int32(partNumber),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		}
		if apply != nil {
			apply(input)
		}
		uploaded, err := s3Tester.Client().UploadPart(t.Context(), input, func(o *service.Options) {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		})
		if err != nil {
			t.Fatal(err)
		}
		return uploaded.ETag
	}

	completeChecksumUpload := func(key, id string, parts []types.CompletedPart, apply func(*service.CompleteMultipartUploadInput)) (*service.CompleteMultipartUploadOutput, error) {
		input := &service.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			UploadId:        aws.String(id),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}
		if apply != nil {
			apply(input)
		}
		return s3Tester.Client().CompleteMultipartUpload(t.Context(), input)
	}

	// a composite upload reports the digest over its part digests
	const (
		compositeKey    = "composite"
		compositeSHA256 = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1"
	)
	compositeID := createChecksumUpload(compositeKey, types.ChecksumAlgorithmSha256, "")
	compositeETag := uploadChecksumPart(compositeKey, compositeID, 1, data, func(in *service.UploadPartInput) {
		in.ChecksumSHA256 = aws.String(partSHA256)
	})
	compositePart := types.CompletedPart{PartNumber: aws.Int32(1), ETag: compositeETag, ChecksumSHA256: aws.String(partSHA256)}

	// assert [s3errs.ErrBadDigest] for an object checksum the parts do not produce
	_, err = completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{compositePart}, func(in *service.CompleteMultipartUploadInput) {
		in.ChecksumSHA256 = aws.String("bad")
	})
	testutil.AssertS3Error(t, s3errs.ErrBadDigest, err)

	// assert [s3errs.ErrBadDigest] for a composite part that carries no checksum
	_, err = completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{{
		PartNumber: aws.Int32(1),
		ETag:       compositeETag,
	}}, nil)
	testutil.AssertS3Error(t, s3errs.ErrBadDigest, err)

	// assert [s3errs.ErrInvalidRequest] for a part carrying another algorithm
	_, err = completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{{
		PartNumber:    aws.Int32(1),
		ETag:          compositeETag,
		ChecksumCRC32: aws.String(base64.StdEncoding.EncodeToString(crc32Sum[:])),
	}}, nil)
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

	// assert [s3errs.ErrBadDigest] for a type the upload was not created with
	_, err = completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{compositePart}, func(in *service.CompleteMultipartUploadInput) {
		in.ChecksumType = types.ChecksumTypeFullObject
	})
	testutil.AssertS3Error(t, s3errs.ErrBadDigest, err)

	// assert [s3errs.ErrInvalidRequest] for an object size the parts do not add up to
	_, err = completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{compositePart}, func(in *service.CompleteMultipartUploadInput) {
		in.MpuObjectSize = aws.Int64(5)
	})
	testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

	completedComposite, err := completeChecksumUpload(compositeKey, compositeID, []types.CompletedPart{compositePart}, func(in *service.CompleteMultipartUploadInput) {
		in.ChecksumSHA256 = aws.String(compositeSHA256)
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(completedComposite.ChecksumSHA256); got != compositeSHA256 {
		t.Fatalf("expected checksum %s, got %s", compositeSHA256, got)
	} else if completedComposite.ChecksumType != types.ChecksumTypeComposite {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeComposite, completedComposite.ChecksumType)
	}

	// a part read reports the part's digest, which carries no part count suffix
	compositeRead, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(compositeKey),
		PartNumber:   aws.Int32(1),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	compositeRead.Body.Close()
	if got := aws.ToString(compositeRead.ChecksumSHA256); got != partSHA256 {
		t.Fatalf("expected checksum %s, got %s", partSHA256, got)
	} else if compositeRead.ChecksumType != types.ChecksumTypeComposite {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeComposite, compositeRead.ChecksumType)
	}

	// the same data uploaded again reports the same ETag and checksum
	repeatID := createChecksumUpload(compositeKey, types.ChecksumAlgorithmSha256, "")
	repeatETag := uploadChecksumPart(compositeKey, repeatID, 1, data, func(in *service.UploadPartInput) {
		in.ChecksumSHA256 = aws.String(partSHA256)
	})
	repeated, err := completeChecksumUpload(compositeKey, repeatID, []types.CompletedPart{{
		PartNumber:     aws.Int32(1),
		ETag:           repeatETag,
		ChecksumSHA256: aws.String(partSHA256),
	}}, nil)
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(repeated.ETag); got != aws.ToString(completedComposite.ETag) {
		t.Fatalf("expected ETag %s, got %s", aws.ToString(completedComposite.ETag), got)
	} else if got := aws.ToString(repeated.ChecksumSHA256); got != compositeSHA256 {
		t.Fatalf("expected checksum %s, got %s", compositeSHA256, got)
	}

	// a full object upload reports the digest over the parts' bytes
	const (
		fullObjectKey   = "full-object"
		fullObjectCRC32 = "WgDhBQ=="
	)
	fullObjectData := [][]byte{
		bytes.Repeat([]byte("A"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("B"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("C"), int(s3.MinUploadPartSize)),
	}
	fullObjectSums := []string{"JRTCyQ==", "QoZTGg==", "YAgjqw=="}

	fullObjectID := createChecksumUpload(fullObjectKey, types.ChecksumAlgorithmCrc32, types.ChecksumTypeFullObject)
	fullObjectParts := make([]types.CompletedPart, len(fullObjectData))
	for i, body := range fullObjectData {
		number := int32(i + 1)
		etag := uploadChecksumPart(fullObjectKey, fullObjectID, number, body, func(in *service.UploadPartInput) {
			in.ChecksumCRC32 = aws.String(fullObjectSums[i])
		})
		fullObjectParts[i] = types.CompletedPart{
			PartNumber:    aws.Int32(number),
			ETag:          etag,
			ChecksumCRC32: aws.String(fullObjectSums[i]),
		}
	}

	completedFullObject, err := completeChecksumUpload(fullObjectKey, fullObjectID, fullObjectParts, func(in *service.CompleteMultipartUploadInput) {
		in.ChecksumCRC32 = aws.String(fullObjectCRC32)
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(completedFullObject.ChecksumCRC32); got != fullObjectCRC32 {
		t.Fatalf("expected checksum %s, got %s", fullObjectCRC32, got)
	} else if completedFullObject.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeFullObject, completedFullObject.ChecksumType)
	}

	// a part read reports the part's digest and the object's checksum type
	fullObjectPart, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(fullObjectKey),
		PartNumber:   aws.Int32(2),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	fullObjectPart.Body.Close()
	if got := aws.ToString(fullObjectPart.ChecksumCRC32); got != fullObjectSums[1] {
		t.Fatalf("expected checksum %s, got %s", fullObjectSums[1], got)
	} else if fullObjectPart.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeFullObject, fullObjectPart.ChecksumType)
	}

	// a part read without the checksum mode reports neither. the SDK sets the
	// mode itself unless response validation is turned off
	unaskedPart, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(fullObjectKey),
		PartNumber: aws.Int32(2),
	}, func(o *service.Options) {
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	if err != nil {
		t.Fatal(err)
	}
	unaskedPart.Body.Close()
	if unaskedPart.ChecksumCRC32 != nil {
		t.Fatalf("expected no checksum, got %s", *unaskedPart.ChecksumCRC32)
	} else if unaskedPart.ChecksumType != "" {
		t.Fatalf("expected no checksum type, got %q", unaskedPart.ChecksumType)
	}

	// a client range reports no digest, part count or not
	ranged, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(fullObjectKey),
		Range:        aws.String("bytes=0-99"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	ranged.Body.Close()
	if ranged.ChecksumCRC32 != nil {
		t.Fatalf("expected no checksum, got %s", *ranged.ChecksumCRC32)
	} else if ranged.ChecksumType != "" {
		t.Fatalf("expected no checksum type, got %q", ranged.ChecksumType)
	}

	// a read naming no part reports the object's digest
	fullObjectHead, err := s3Tester.Client().HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(fullObjectKey),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(fullObjectHead.ChecksumCRC32); got != fullObjectCRC32 {
		t.Fatalf("expected checksum %s, got %s", fullObjectCRC32, got)
	} else if fullObjectHead.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeFullObject, fullObjectHead.ChecksumType)
	}

	// a full object upload completes without any part checksum
	bareID := createChecksumUpload(fullObjectKey, types.ChecksumAlgorithmCrc32, types.ChecksumTypeFullObject)
	bareParts := make([]types.CompletedPart, len(fullObjectData))
	for i, body := range fullObjectData {
		number := int32(i + 1)
		bareParts[i] = types.CompletedPart{
			PartNumber: aws.Int32(number),
			ETag:       uploadChecksumPart(fullObjectKey, bareID, number, body, nil),
		}
	}

	completedBare, err := completeChecksumUpload(fullObjectKey, bareID, bareParts, nil)
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(completedBare.ChecksumCRC32); got != fullObjectCRC32 {
		t.Fatalf("expected checksum %s, got %s", fullObjectCRC32, got)
	}

	// an upload created without an algorithm stores no checksum
	const plainKey = "plain"
	plainID := createChecksumUpload(plainKey, "", "")
	plainETag := uploadChecksumPart(plainKey, plainID, 1, data, nil)
	if _, err := completeChecksumUpload(plainKey, plainID, []types.CompletedPart{{
		PartNumber:     aws.Int32(1),
		ETag:           plainETag,
		ChecksumSHA256: aws.String(partSHA256),
	}}, nil); err != nil {
		t.Fatal(err)
	}

	head, err := s3Tester.Client().HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(plainKey),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	} else if head.ChecksumSHA256 != nil {
		t.Fatalf("expected no checksum, got %s", *head.ChecksumSHA256)
	}

	// a single part object has no part digest, so its part read falls back to
	// the object's, which covers the one part in full
	const singleKey = "single"
	if _, err := s3Tester.Client().PutObject(t.Context(), &service.PutObjectInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(singleKey),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumSHA256: aws.String(partSHA256),
	}); err != nil {
		t.Fatal(err)
	}

	singleRead, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(singleKey),
		PartNumber:   aws.Int32(1),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	singleRead.Body.Close()
	if got := aws.ToString(singleRead.ChecksumSHA256); got != partSHA256 {
		t.Fatalf("expected checksum %s, got %s", partSHA256, got)
	} else if singleRead.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("expected type %q, got %q", types.ChecksumTypeFullObject, singleRead.ChecksumType)
	}
}

func TestUploadPartCopy(t *testing.T) {
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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
	completedSource, err := s3Tester.CompleteMultipartUpload(t.Context(), bucketSrc, objectSrc, id, parts)
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

	// assert [s3errs.ErrPreconditionFailed] is returned for a failed source condition
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{
		PartNumber:          2,
		SourcePreconditions: testutil.Preconditions{IfMatch: aws.String(`"wrong"`)},
	})
	testutil.AssertS3Error(t, s3errs.ErrPreconditionFailed, err)
	if _, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{
		PartNumber:          2,
		SourcePreconditions: testutil.Preconditions{IfMatch: completedSource.ETag},
	}); err != nil {
		t.Fatal(err)
	}

	// upload the second part first, copying from the source object
	res3, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{
		PartNumber: 2,
		Range: &s3.ObjectRange{
			Start:  s3.MinUploadPartSize / 2,
			Length: s3.MinUploadPartSize,
		},
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
	res6, err := s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{PartNumber: 1})
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
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, s3.NewUploadID().String(), testutil.UploadPartCopyOptions{PartNumber: math.MaxInt32})
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)

	// assert [s3errs.ErrNoSuchUpload] is returned for invalid upload id
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, s3.NewUploadID().String(), testutil.UploadPartCopyOptions{PartNumber: 1})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchUpload, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.UploadPartCopy(t.Context(), bucketSrc, objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{PartNumber: 1})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// assert [s3errs.ErrNoSuchBucket] is returned for nonexistent bucket
	_, err = s3Tester.UploadPartCopy(t.Context(), "missing-bucket", objectSrc, bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{PartNumber: 1})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// assert [s3errs.ErrNoSuchKey] is returned for nonexistent source object
	_, err = s3Tester.UploadPartCopy(t.Context(), bucketSrc, "missing-object", bucketDst, objectDst, uploadID, testutil.UploadPartCopyOptions{PartNumber: 1})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
}

func TestCompleteMultipartUpload(t *testing.T) {
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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

	// regression: GET/HEAD without partNumber must return the multipart
	// ETag with the "-N" suffix so clients don't mistake it for a plain MD5.
	headResp, err := s3Tester.Client().HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(*headResp.ETag, "-") {
		t.Fatalf("expected multipart ETag suffix, got %q", *headResp.ETag)
	} else if *headResp.ETag != expectedETag {
		t.Fatalf("expected HEAD ETag %q, got %q", expectedETag, *headResp.ETag)
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
	p3Data := []byte("additional part")
	uploadID, parts = newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p1Data, p3Data})
	outOfOrderParts := []types.CompletedPart{parts[1], parts[0], parts[2]}
	_, err = s3Tester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, outOfOrderParts)
	testutil.AssertS3Error(t, s3errs.ErrInvalidPartOrder, err)

	// assert [s3errs.ErrAccessDenied] is returned for unauthorized access
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	uploadID, parts = newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{p1Data, p2Data})
	_, err = otherTester.CompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

func TestConditionalCompleteMultipartUpload(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	const (
		bucket = "conditional-complete"
		object = "object"
	)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	complete := func(body string, p testutil.Preconditions) (*service.CompleteMultipartUploadOutput, error) {
		uploadID, parts := newTestMultipartUpload(t, s3Tester, bucket, object, [][]byte{[]byte(body)})
		return s3Tester.ConditionalCompleteMultipartUpload(t.Context(), bucket, object, uploadID, parts, p)
	}

	// an If-Match names an object to replace, so with nothing there it reports a
	// missing key rather than a failed condition, even for the wildcard
	_, err := complete("not-written", testutil.Preconditions{IfMatch: aws.String("*")})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
	_, err = complete("not-written", testutil.Preconditions{IfMatch: aws.String(`"whatever"`)})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

	initial, err := complete("initial", testutil.Preconditions{IfNoneMatch: aws.String("*")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = complete("not-written", testutil.Preconditions{IfNoneMatch: aws.String("*")})
	testutil.AssertS3Error(t, s3errs.ErrPreconditionFailed, err)
	_, err = complete("not-written", testutil.Preconditions{IfNoneMatch: initial.ETag})
	testutil.AssertS3Error(t, s3errs.ErrPreconditionFailed, err)
	_, err = complete("not-written", testutil.Preconditions{IfMatch: aws.String(`"wrong"`)})
	testutil.AssertS3Error(t, s3errs.ErrPreconditionFailed, err)

	// a non-wildcard If-None-Match holds while the current object differs from
	// the named ETag
	if _, err := complete("differs", testutil.Preconditions{IfNoneMatch: aws.String(`"whatever"`)}); err != nil {
		t.Fatal(err)
	}
	current, err := complete("updated", testutil.Preconditions{IfMatch: aws.String("*")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := complete("updated-again", testutil.Preconditions{IfMatch: current.ETag}); err != nil {
		t.Fatal(err)
	}
}

func TestListMultipartUploads(t *testing.T) {
	// prepare a tester with an additional keypair
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

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
	} else if res.StorageClass != types.StorageClassStandard {
		t.Fatalf("expected storage class STANDARD, got %q", res.StorageClass)
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
