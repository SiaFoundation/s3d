package testutil

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
)

const (
	// AccessKeyID is the access key configured for S3Tester
	AccessKeyID = "AKIA7GQ3XN52WQLYDHZP"

	// SecretAccessKey is the secret key configured for S3Tester
	SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// S3Tester wraps an AWS S3 client configured to talk to an in-memory S3
// backend.
type S3Tester struct {
	cfg     aws.Config
	backend s3.Backend
	client  *service.Client
}

// ChangeAccessKey creates a copy of the tester that uses the provided keypair
// to access the S3 API.
func (t *S3Tester) ChangeAccessKey(tb testing.TB, accessKeyID, secretKey string) *S3Tester {
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
	_, err := t.backend.PutObject(context.Background(), AccessKeyID, bucket, object, bytes.NewReader(data), s3.PutObjectOptions{
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

// CopyObject is a convenience wrapper around the AWS SDK's CopyObject API.
func (t *S3Tester) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, meta map[string]string) ([]byte, error) {
	resp, err := t.client.CopyObject(ctx, &service.CopyObjectInput{
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, url.QueryEscape(srcObject))),
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstObject),
		Metadata:   meta,
	})
	if err != nil {
		return nil, err
	}
	etag := strings.Trim(*resp.CopyObjectResult.ETag, `"`)
	hash, err := hex.DecodeString(etag)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ETag %q: %w", *resp.CopyObjectResult.ETag, err)
	}
	return hash, nil
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

// CreateMultipartUpload is a convenience wrapper around the AWS SDK's
// CreateMultipartUpload API.
func (t *S3Tester) CreateMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string) (*service.CreateMultipartUploadOutput, error) {
	return t.client.CreateMultipartUpload(ctx, &service.CreateMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		Metadata: meta,
	})
}

// ListMultipartUploads is a convenience wrapper around the AWS SDK's
// ListMultipartUploads API.
func (t *S3Tester) ListMultipartUploads(ctx context.Context, bucket string, input *service.ListMultipartUploadsInput) (*service.ListMultipartUploadsOutput, error) {
	if input == nil {
		input = &service.ListMultipartUploadsInput{}
	}
	input.Bucket = aws.String(bucket)
	return t.client.ListMultipartUploads(ctx, input)
}

// AbortMultipartUpload is a convenience wrapper around the AWS SDK's
// AbortMultipartUpload API.
func (t *S3Tester) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	_, err := t.client.AbortMultipartUpload(ctx, &service.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadID),
	})
	return err
}

// UploadPart uploads a single part for an existing multipart upload.
func (t *S3Tester) UploadPart(ctx context.Context, bucket, object, uploadID string, partNumber int32, body []byte) (*service.UploadPartOutput, error) {
	input := &service.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(object),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	return t.client.UploadPart(ctx, input)
}

// CompleteMultipartUpload is a convenience wrapper around the AWS SDK's
// CompleteMultipartUpload API.
func (t *S3Tester) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []types.CompletedPart) (*service.CompleteMultipartUploadOutput, error) {
	input := &service.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	return t.client.CompleteMultipartUpload(ctx, input)
}

type testerCfg struct {
	backend     s3.Backend
	serviceOpts []func(*service.Options)
	tls         bool
}

// TesterOption is an option for configuring the S3Tester.
type TesterOption func(*testerCfg)

// WithServiceOptions adds options to configure the AWS S3 client.
func WithServiceOptions(opts ...func(*service.Options)) TesterOption {
	return func(cfg *testerCfg) {
		cfg.serviceOpts = opts
	}
}

// WithBackend sets the S3 backend to use for the tester.
func WithBackend(backend s3.Backend) TesterOption {
	return func(cfg *testerCfg) {
		cfg.backend = backend
	}
}

// WithTLS configures the tester to use TLS.
func WithTLS() TesterOption {
	return func(cfg *testerCfg) {
		cfg.tls = true
	}
}

// NewTester creates a new S3Tester and a AWS client configured to talk to it.
func NewTester(t testing.TB, opts ...TesterOption) *S3Tester {
	t.Helper()

	cfg := &testerCfg{
		backend:     nil,
		serviceOpts: nil,
		tls:         false,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.backend == nil {
		backend := NewMemoryBackend(WithKeyPair(AccessKeyID, SecretAccessKey))
		cfg.backend = backend
	}

	handler := s3.New(cfg.backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(zaptest.NewLogger(t)))

	server := httptest.NewUnstartedServer(handler)
	if cfg.tls {
		server.StartTLS()
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)

	awsCfg, err := config.LoadDefaultConfig(t.Context())
	if err != nil {
		t.Fatalf("unable to load SDK config, %v", err)
	}

	s3Opts := []func(*service.Options){
		func(o *service.Options) {
			o.Region = "us-east-1"
			o.HTTPClient = server.Client()
			o.BaseEndpoint = &server.URL
			o.UsePathStyle = true
			o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     AccessKeyID,
					SecretAccessKey: SecretAccessKey,
				},
			})
		},
	}
	s3Opts = append(s3Opts, cfg.serviceOpts...)

	return &S3Tester{
		cfg:     awsCfg,
		backend: cfg.backend,
		client:  service.NewFromConfig(awsCfg, s3Opts...),
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
