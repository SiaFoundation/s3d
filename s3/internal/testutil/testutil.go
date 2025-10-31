package testutil

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/testutils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
)

const (
	accessKeyID     = "AKIA7GQ3XN52WQLYDHZP"
	secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// S3Tester wraps an AWS S3 client configured to talk to an in-memory S3
// backend.
type S3Tester struct {
	cfg     aws.Config
	backend *testutils.MemoryBackend
	client  *service.Client
}

// AddAccessKey adds a new keypair to the in-memory S3 backend and returns a new
// S3Tester configured to use those credentials.
func (t *S3Tester) AddAccessKey(tb testing.TB, accessKeyID, secretKey string) *S3Tester {
	err := t.backend.AddAccessKey(context.Background(), accessKeyID, secretKey)
	if err != nil {
		tb.Fatal(err)
	}
	client := service.NewFromConfig(t.cfg, func(o *service.Options) {
		*o = t.client.Options()
		o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
			},
		})
	})
	return &S3Tester{
		cfg:     t.cfg,
		backend: t.backend,
		client:  client,
	}
}

// AddObject adds an object to the in-memory S3 backend.
func (t *S3Tester) AddObject(bucket, object string, data []byte, metadata map[string]string) error {
	_, err := t.backend.PutObject(context.Background(), accessKeyID, bucket, object, bytes.NewReader(data), s3.PutObjectOptions{
		ContentLength: int64(len(data)),
		Meta:          metadata,
	})
	return err
}

// BucketLocation gets the location of an S3 bucket.
func (t *S3Tester) BucketLocation(ctx context.Context, bucket string) (string, error) {
	resp, err := t.client.GetBucketLocation(ctx, &service.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	return string(resp.LocationConstraint), err
}

// CreateBucket creates a new S3 bucket.
func (t *S3Tester) CreateBucket(ctx context.Context, bucket string) error {
	_, err := t.client.CreateBucket(ctx, &service.CreateBucketInput{
		Bucket:                    aws.String(bucket),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{},
	})
	return err
}

// DeleteBucket deletes an S3 bucket.
func (t *S3Tester) DeleteBucket(ctx context.Context, bucket string) error {
	_, err := t.client.DeleteBucket(ctx, &service.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	return err
}

// DeleteObject deletes an S3 object.
func (t *S3Tester) DeleteObject(ctx context.Context, bucket, object string) error {
	_, err := t.client.DeleteObject(ctx, &service.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	return err
}

// DeleteObjects deletes multiple S3 objects at once. If quiet is set to true,
// the response will only contain errors.
func (t *S3Tester) DeleteObjects(ctx context.Context, bucket string, objects []string, quiet *bool) (*service.DeleteObjectsOutput, error) {
	var objs []types.ObjectIdentifier
	for _, o := range objects {
		objs = append(objs, types.ObjectIdentifier{
			Key: aws.String(o),
		})
	}
	resp, err := t.client.DeleteObjects(ctx, &service.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objs,
			Quiet:   quiet,
		},
	})
	return resp, err
}

// GetObject is a convenience wrapper around the AWS SDK's GetObject API.
func (t *S3Tester) GetObject(ctx context.Context, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	resp, err := t.client.GetObject(ctx, &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Range:  rangeHeader(rnge),
	})
	if err != nil {
		return nil, err
	}
	etag := strings.Trim(*resp.ETag, `"`)
	var contentMD5 [16]byte
	_, err = hex.Decode(contentMD5[:], []byte(etag))
	if err != nil {
		return nil, fmt.Errorf("failed to decode ETag %q: %w", *resp.ETag, err)
	}
	var objRange *s3.ObjectRange
	var size int64
	if resp.ContentRange != nil {
		objRange, size, err = parseRange(*resp.ContentRange)
		if err != nil {
			return nil, err
		}
	} else {
		size = *resp.ContentLength
	}
	return &s3.Object{
		Body:         resp.Body,
		ContentMD5:   contentMD5,
		LastModified: *resp.LastModified,
		Metadata:     resp.Metadata,
		Range:        objRange,
		Size:         size,
	}, nil
}

// HeadBucket is a convenience wrapper around the AWS SDK's HeadBucket API.
func (t *S3Tester) HeadBucket(ctx context.Context, bucket string) error {
	_, err := t.client.HeadBucket(ctx, &service.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	return err
}

// HeadObject is a convenience wrapper around the AWS SDK's HeadObject API.
func (t *S3Tester) HeadObject(ctx context.Context, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	resp, err := t.client.HeadObject(ctx, &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Range:  rangeHeader(rnge),
	})
	if err != nil {
		return nil, err
	}
	etag := strings.Trim(*resp.ETag, `"`)
	var contentMD5 [16]byte
	_, err = hex.Decode(contentMD5[:], []byte(etag))
	if err != nil {
		return nil, fmt.Errorf("failed to decode ETag %q: %w", *resp.ETag, err)
	}
	var objRange *s3.ObjectRange
	var size int64
	if resp.ContentRange != nil {
		objRange, size, err = parseRange(*resp.ContentRange)
		if err != nil {
			return nil, err
		}
	} else {
		size = *resp.ContentLength
	}
	return &s3.Object{
		ContentMD5:   contentMD5,
		LastModified: *resp.LastModified,
		Metadata:     resp.Metadata,
		Range:        objRange,
		Size:         size,
	}, nil
}

// ListBuckets lists all S3 buckets of the authenticated user.
func (t *S3Tester) ListBuckets(ctx context.Context) ([]types.Bucket, error) {
	resp, err := t.client.ListBuckets(ctx, &service.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	return resp.Buckets, err
}

// ListObjectsV2 is a convenience wrapper around the AWS SDK's ListObjectsV2 API.
func (t *S3Tester) ListObjectsV2(ctx context.Context, bucket string, prefix, delimiter *string, page s3.ListObjectsPage) (*service.ListObjectsV2Output, error) {
	var maxKeys *int32
	if page.MaxKeys > 0 {
		maxKeys = aws.Int32(int32(page.MaxKeys))
	}
	resp, err := t.client.ListObjectsV2(ctx, &service.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		ContinuationToken: page.Marker,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		Prefix:            prefix,
	})
	if err != nil {
		return nil, err
	}
	for i := range resp.Contents {
		*resp.Contents[i].ETag = strings.Trim(*resp.Contents[i].ETag, `"`)
	}
	return resp, nil
}

// ListObjectVersions is a convenience wrapper around the AWS SDK's ListObjectVersions API.
func (t *S3Tester) ListObjectVersions(ctx context.Context, bucket string, prefix, delimiter *string, page s3.ListObjectsPage) (*service.ListObjectVersionsOutput, error) {
	var maxKeys *int32
	if page.MaxKeys > 0 {
		maxKeys = aws.Int32(int32(page.MaxKeys))
	}
	resp, err := t.client.ListObjectVersions(ctx, &service.ListObjectVersionsInput{
		Bucket:          aws.String(bucket),
		Delimiter:       delimiter,
		KeyMarker:       page.Marker,
		MaxKeys:         maxKeys,
		Prefix:          prefix,
		VersionIdMarker: nil, // versions not supported
	})
	if err != nil {
		return nil, err
	}
	for i := range resp.Versions {
		*resp.Versions[i].ETag = strings.Trim(*resp.Versions[i].ETag, `"`)
	}
	return resp, nil
}

// PutObject is a convenience wrapper around the AWS SDK's PutObject API.
func (t *S3Tester) PutObject(ctx context.Context, bucket, object string, r io.Reader, meta map[string]string) ([]byte, error) {
	resp, err := t.client.PutObject(ctx, &service.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		Body:     r,
		Metadata: meta,
	})
	if err != nil {
		return nil, err
	}
	etag := strings.Trim(*resp.ETag, `"`)
	hash, err := hex.DecodeString(etag)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ETag %q: %w", *resp.ETag, err)
	}
	return hash, nil
}

// NewTester creates a new S3Tester with an in-memory S3 backend and an AWS
// client configured to talk to it.
func NewTester(t testing.TB, optFns ...func(*service.Options)) *S3Tester {
	return newTesterWithTLS(t, false, optFns...)
}

// NewTesterTLS creates a new S3Tester with an in-memory S3 backend and an AWS
// client configured to talk to it over TLS.
func NewTesterTLS(t testing.TB, optFns ...func(*service.Options)) *S3Tester {
	return newTesterWithTLS(t, true, optFns...)
}

func newTesterWithTLS(t testing.TB, tls bool, optFns ...func(*service.Options)) *S3Tester {
	t.Helper()

	backend := testutils.NewMemoryBackend()
	if err := backend.AddAccessKey(t.Context(), accessKeyID, secretAccessKey); err != nil {
		t.Fatal(err)
	}

	handler := s3.New(backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(zaptest.NewLogger(t)))

	server := httptest.NewUnstartedServer(handler)
	if tls {
		server.StartTLS()
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(t.Context())
	if err != nil {
		t.Fatalf("unable to load SDK config, %v", err)
	}

	opts := []func(*service.Options){
		func(o *service.Options) {
			o.Region = "us-east-1"
			o.HTTPClient = server.Client()
			o.BaseEndpoint = &server.URL
			o.UsePathStyle = true
			o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKeyID,
					SecretAccessKey: secretAccessKey,
				},
			})
		},
	}
	opts = append(opts, optFns...)

	return &S3Tester{
		cfg:     cfg,
		backend: backend,
		client:  service.NewFromConfig(cfg, opts...),
	}
}

// AssertS3Error is a helper to check an error returned from the AWS SDK against
// an expected s3.S3Error. Unfortunately the SDK doesn't expose its internal
// error type so reflection is not an option and we need to extract the status
// code from the string.
func AssertS3Error(t testing.TB, expected s3errs.Error, got error) {
	t.Helper()
	if got == nil {
		t.Fatal("expected error, got nil")
	}

	// check status code
	AssertS3StatusCode(t, expected, got)

	// check error
	if !strings.Contains(got.Error(), expected.Code) {
		t.Fatalf("expected error code %q, got %q", expected.Code, got.Error())
	}
}

// AssertS3StatusCode is similar to AssertS3Error but meant to be used for
// responses from HEAD endpoints which can't return the full error.
func AssertS3StatusCode(t testing.TB, expected s3errs.Error, got error) {
	t.Helper()
	if got == nil {
		t.Fatal("expected error, got nil")
	}

	// check status code
	re := regexp.MustCompile(`StatusCode: (\d{3})`)
	matches := re.FindStringSubmatch(got.Error())
	if len(matches) != 2 {
		t.Fatalf("expected error to contain status code, got: %v", got)
	}
	var code int
	fmt.Sscanf(matches[1], "%d", &code)
	if code != expected.HTTPStatus {
		t.Fatalf("expected status code %d, got %d", expected.HTTPStatus, code)
	}
}

// parseRange parses a Content-Range header value from a http response.
func parseRange(s string) (_ *s3.ObjectRange, size int64, _ error) {
	var start, end int64
	if _, err := fmt.Sscanf(s, "bytes %d-%d/%d", &start, &end, &size); err != nil {
		return nil, 0, fmt.Errorf("failed to parse range %q: %w", s, err)
	}
	return &s3.ObjectRange{
		Start:  start,
		Length: end - start + 1,
	}, size, nil
}

// rangeHeader converts an s3.ObjectRangeRequest to a HTTP Range header value to
// use in AWS SDK calls.
func rangeHeader(rnge *s3.ObjectRangeRequest) *string {
	if rnge == nil {
		return nil
	}
	var s string
	if rnge.FromEnd {
		s = fmt.Sprintf("bytes=-%d", rnge.Start)
	} else if rnge.End == -1 {
		s = fmt.Sprintf("bytes=%d-", rnge.Start)
	} else {
		s = fmt.Sprintf("bytes=%d-%d", rnge.Start, rnge.End)
	}
	return &s
}
