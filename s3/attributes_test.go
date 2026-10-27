package s3_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestGetObjectAttributes(t *testing.T) {
	const (
		bucket       = "attributes-bucket"
		singleKey    = "single"
		multipartKey = "multipart"
	)

	s3Tester := testutil.NewTester(t)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	allAttributes := []types.ObjectAttributes{
		types.ObjectAttributesEtag,
		types.ObjectAttributesChecksum,
		types.ObjectAttributesObjectParts,
		types.ObjectAttributesStorageClass,
		types.ObjectAttributesObjectSize,
	}

	attributes := func(key string, requested []types.ObjectAttributes, apply func(*service.GetObjectAttributesInput)) (*service.GetObjectAttributesOutput, error) {
		input := &service.GetObjectAttributesInput{
			Bucket:           aws.String(bucket),
			Key:              aws.String(key),
			ObjectAttributes: requested,
		}
		if apply != nil {
			apply(input)
		}
		return s3Tester.Client().GetObjectAttributes(t.Context(), input)
	}

	// the SDK refuses to send some of the requests under test, so those are
	// signed and sent by hand
	rawRequest := func(path string, header http.Header) (*http.Response, []byte) {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, *s3Tester.Client().Options().BaseEndpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for name, values := range header {
			for _, value := range values {
				req.Header.Add(name, value)
			}
		}

		empty := sha256.Sum256(nil)
		payload := hex.EncodeToString(empty[:])
		req.Header.Set("X-Amz-Content-Sha256", payload)
		creds := aws.Credentials{AccessKeyID: testutil.AccessKeyID, SecretAccessKey: testutil.SecretAccessKey}
		if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, payload, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp, body
	}

	// a single PUT carrying a full object checksum
	data := bytes.Repeat([]byte("A"), 1024)
	dataMD5 := md5.Sum(data)
	dataSum := sha256.Sum256(data)
	dataSHA256 := base64.StdEncoding.EncodeToString(dataSum[:])
	if _, err := s3Tester.Client().PutObject(t.Context(), &service.PutObjectInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(singleKey),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumSHA256: aws.String(dataSHA256),
	}); err != nil {
		t.Fatal(err)
	}

	single, err := attributes(singleKey, allAttributes, nil)
	if err != nil {
		t.Fatal(err)
	} else if aws.ToInt64(single.ObjectSize) != int64(len(data)) {
		t.Fatal("unexpected", aws.ToInt64(single.ObjectSize))
	} else if got := aws.ToString(single.ETag); got != strings.Trim(s3.FormatETag(dataMD5[:], 0), `"`) {
		t.Fatal("unexpected", got)
	} else if single.StorageClass != types.StorageClassStandard {
		t.Fatal("unexpected", single.StorageClass)
	} else if single.Checksum == nil {
		t.Fatal("expected a checksum")
	} else if got := aws.ToString(single.Checksum.ChecksumSHA256); got != dataSHA256 {
		t.Fatal("unexpected", got)
	} else if single.Checksum.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatal("unexpected", single.Checksum.ChecksumType)
	} else if single.ObjectParts != nil {
		t.Fatal("expected no object parts")
	}

	// a three part composite object
	partData := [][]byte{
		bytes.Repeat([]byte("A"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("B"), int(s3.MinUploadPartSize)),
		bytes.Repeat([]byte("C"), 1024),
	}
	created, err := s3Tester.Client().CreateMultipartUpload(t.Context(), &service.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(multipartKey),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatal(err)
	}

	partSums := make([]string, len(partData))
	completedParts := make([]types.CompletedPart, len(partData))
	for i, body := range partData {
		sum := sha256.Sum256(body)
		partSums[i] = base64.StdEncoding.EncodeToString(sum[:])
		number := int32(i + 1)
		uploaded, err := s3Tester.Client().UploadPart(t.Context(), &service.UploadPartInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String(multipartKey),
			UploadId:       created.UploadId,
			PartNumber:     aws.Int32(number),
			Body:           bytes.NewReader(body),
			ContentLength:  aws.Int64(int64(len(body))),
			ChecksumSHA256: aws.String(partSums[i]),
		})
		if err != nil {
			t.Fatal(err)
		}
		completedParts[i] = types.CompletedPart{
			PartNumber:     aws.Int32(number),
			ETag:           uploaded.ETag,
			ChecksumSHA256: aws.String(partSums[i]),
		}
	}

	completed, err := s3Tester.Client().CompleteMultipartUpload(t.Context(), &service.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(multipartKey),
		UploadId:        created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		t.Fatal(err)
	}

	multipart, err := attributes(multipartKey, allAttributes, nil)
	if err != nil {
		t.Fatal(err)
	} else if multipart.Checksum == nil {
		t.Fatal("expected a checksum")
	} else if got := aws.ToString(multipart.Checksum.ChecksumSHA256); got != aws.ToString(completed.ChecksumSHA256) {
		t.Fatal("unexpected", got)
	} else if multipart.Checksum.ChecksumType != types.ChecksumTypeComposite {
		t.Fatal("unexpected", multipart.Checksum.ChecksumType)
	} else if multipart.ObjectParts == nil {
		t.Fatal("expected object parts")
	} else if aws.ToInt32(multipart.ObjectParts.TotalPartsCount) != int32(len(partData)) {
		t.Fatal("unexpected", aws.ToInt32(multipart.ObjectParts.TotalPartsCount))
	} else if aws.ToBool(multipart.ObjectParts.IsTruncated) {
		t.Fatal("expected a complete listing")
	} else if len(multipart.ObjectParts.Parts) != len(partData) {
		t.Fatal("unexpected", len(multipart.ObjectParts.Parts))
	}
	for i, part := range multipart.ObjectParts.Parts {
		if aws.ToInt32(part.PartNumber) != int32(i+1) {
			t.Fatal("unexpected", aws.ToInt32(part.PartNumber))
		} else if aws.ToInt64(part.Size) != int64(len(partData[i])) {
			t.Fatal("unexpected", aws.ToInt64(part.Size))
		} else if got := aws.ToString(part.ChecksumSHA256); got != partSums[i] {
			t.Fatal("unexpected", got)
		}
	}

	// a limited page reports where the next one starts
	page, err := attributes(multipartKey, allAttributes, func(in *service.GetObjectAttributesInput) {
		in.MaxParts = aws.Int32(2)
	})
	if err != nil {
		t.Fatal(err)
	} else if page.ObjectParts == nil {
		t.Fatal("expected object parts")
	} else if !aws.ToBool(page.ObjectParts.IsTruncated) {
		t.Fatal("expected a truncated listing")
	} else if aws.ToInt32(page.ObjectParts.MaxParts) != 2 {
		t.Fatal("unexpected", aws.ToInt32(page.ObjectParts.MaxParts))
	} else if len(page.ObjectParts.Parts) != 2 {
		t.Fatal("unexpected", len(page.ObjectParts.Parts))
	} else if got := aws.ToString(page.ObjectParts.NextPartNumberMarker); got != "2" {
		t.Fatal("unexpected", got)
	}

	rest, err := attributes(multipartKey, allAttributes, func(in *service.GetObjectAttributesInput) {
		in.PartNumberMarker = aws.String("2")
	})
	if err != nil {
		t.Fatal(err)
	} else if rest.ObjectParts == nil {
		t.Fatal("expected object parts")
	} else if aws.ToBool(rest.ObjectParts.IsTruncated) {
		t.Fatal("expected a complete listing")
	} else if len(rest.ObjectParts.Parts) != 1 {
		t.Fatal("unexpected", len(rest.ObjectParts.Parts))
	} else if aws.ToInt32(rest.ObjectParts.Parts[0].PartNumber) != 3 {
		t.Fatal("unexpected", aws.ToInt32(rest.ObjectParts.Parts[0].PartNumber))
	}

	// an attribute that was not asked for is left out, and the reported ETag
	// keeps its part count suffix without the quotes a header spells it with
	etagOnly, err := attributes(multipartKey, []types.ObjectAttributes{types.ObjectAttributesEtag}, nil)
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(etagOnly.ETag); got != strings.Trim(aws.ToString(completed.ETag), `"`) {
		t.Fatal("unexpected", got)
	} else if etagOnly.Checksum != nil {
		t.Fatal("expected no checksum")
	} else if etagOnly.ObjectSize != nil {
		t.Fatal("unexpected", aws.ToInt64(etagOnly.ObjectSize))
	} else if etagOnly.ObjectParts != nil {
		t.Fatal("expected no object parts")
	}

	// assert [s3errs.ErrInvalidArgument] for a request naming no attribute
	resp, body := rawRequest("/"+bucket+"/"+singleKey+"?attributes", nil)
	var parsed s3.ErrorResponse
	if err := xml.Unmarshal(body, &parsed); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != s3errs.ErrInvalidArgument.HTTPStatus || parsed.Code != s3errs.ErrInvalidArgument.Code {
		t.Fatal("unexpected", resp.StatusCode, parsed.Code)
	}

	// assert [s3errs.ErrInvalidArgument] for an attribute that does not exist
	resp, body = rawRequest("/"+bucket+"/"+singleKey+"?attributes", http.Header{
		"X-Amz-Object-Attributes": []string{"ETag, Nonsense"},
	})
	if err := xml.Unmarshal(body, &parsed); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != s3errs.ErrInvalidArgument.HTTPStatus || parsed.Code != s3errs.ErrInvalidArgument.Code {
		t.Fatal("unexpected", resp.StatusCode, parsed.Code)
	}

	// a delete marker is reported the way a HEAD reports it
	const versionedBucket = "versioned-attributes"
	if err := s3Tester.CreateBucket(t.Context(), versionedBucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), versionedBucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), versionedBucket, singleKey, bytes.NewReader(data), nil); err != nil {
		t.Fatal(err)
	}

	deleted, err := s3Tester.Client().DeleteObject(t.Context(), &service.DeleteObjectInput{
		Bucket: aws.String(versionedBucket),
		Key:    aws.String(singleKey),
	})
	if err != nil {
		t.Fatal(err)
	} else if !aws.ToBool(deleted.DeleteMarker) {
		t.Fatal("expected a delete marker")
	}

	marker := "/" + versionedBucket + "/" + singleKey + "?attributes&versionId=" + aws.ToString(deleted.VersionId)
	resp, _ = rawRequest(marker, http.Header{
		"X-Amz-Object-Attributes": []string{"ETag"},
	})
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatal("unexpected", resp.StatusCode)
	} else if got := resp.Header.Get("x-amz-delete-marker"); got != "true" {
		t.Fatal("unexpected", got)
	}

	// a bucket policy grants an anonymous caller the same read
	anon := s3Tester.Anonymous()
	_, err = anon.Client().GetObjectAttributes(t.Context(), &service.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(multipartKey),
		ObjectAttributes: allAttributes,
	})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::%s/*"
    }
  ]
}`, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	public, err := anon.Client().GetObjectAttributes(t.Context(), &service.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(multipartKey),
		ObjectAttributes: allAttributes,
	})
	if err != nil {
		t.Fatal(err)
	} else if public.ObjectParts == nil {
		t.Fatal("expected object parts")
	} else if len(public.ObjectParts.Parts) != len(partData) {
		t.Fatal("unexpected", len(public.ObjectParts.Parts))
	}
}
