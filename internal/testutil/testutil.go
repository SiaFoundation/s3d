package testutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

const (
	// AccessKeyID is the access key configured for S3Tester
	AccessKeyID = "AKIA7GQ3XN52WQLYDHZP"

	// SecretAccessKey is the secret key configured for S3Tester
	SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// Owner is the default owner name for objects created by S3Tester
	Owner = "s3tester"
)

// S3Tester wraps an AWS S3 client configured to talk to an S3 backend.
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

// Client returns the tester's S3 client.
func (t *S3Tester) Client() *service.Client {
	return t.client
}

// AddObject adds an object to the S3 backend.
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
	if err != nil {
		return "", err
	}
	return string(resp.LocationConstraint), nil
}

// CopyObject is a convenience wrapper around the AWS SDK's CopyObject API.
func (t *S3Tester) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, dir types.MetadataDirective, meta map[string]string) ([]byte, error) {
	resp, err := t.client.CopyObject(ctx, &service.CopyObjectInput{
		CopySource:        aws.String(fmt.Sprintf("%s/%s", srcBucket, url.QueryEscape(srcObject))),
		Bucket:            aws.String(dstBucket),
		Key:               aws.String(dstObject),
		MetadataDirective: dir,
		Metadata:          meta,
	})
	if err != nil {
		return nil, err
	}
	hash := s3.ParseETag(*resp.CopyObjectResult.ETag)
	return hash[:], nil
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
func (t *S3Tester) DeleteObjects(ctx context.Context, bucket string, objects []types.ObjectIdentifier, quiet *bool) (*service.DeleteObjectsOutput, error) {
	resp, err := t.client.DeleteObjects(ctx, &service.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objects,
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
	contentMD5 := s3.ParseETag(*resp.ETag)
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
		PartsCount:   resp.PartsCount,
	}, nil
}

// GetObjectPart is a convenience wrapper around the AWS SDK's GetObject API that
// allows specifying a part to download.
func (t *S3Tester) GetObjectPart(ctx context.Context, bucket, object string, partNumber int32) (*s3.Object, error) {
	resp, err := t.client.GetObject(ctx, &service.GetObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		PartNumber: aws.Int32(partNumber),
	})
	if err != nil {
		return nil, err
	}
	contentMD5 := s3.ParseETag(*resp.ETag)
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
		PartsCount:   resp.PartsCount,
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
	contentMD5 := s3.ParseETag(*resp.ETag)
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
		PartsCount:   resp.PartsCount,
	}, nil
}

// HeadObjectPart is a convenience wrapper around the AWS SDK's HeadObject API
// that allows specifying a part.
func (t *S3Tester) HeadObjectPart(ctx context.Context, bucket, object string, partNumber int32) (*s3.Object, error) {
	resp, err := t.client.HeadObject(ctx, &service.HeadObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		PartNumber: aws.Int32(partNumber),
	})
	if err != nil {
		return nil, err
	}
	contentMD5 := s3.ParseETag(*resp.ETag)
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
		PartsCount:   resp.PartsCount,
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

// ListObjects is a convenience wrapper around the AWS SDK's ListObjects (v1) API.
func (t *S3Tester) ListObjects(ctx context.Context, bucket string, prefix, delimiter *string, page s3.ListObjectsPage) (*service.ListObjectsOutput, error) {
	var maxKeys *int32
	if page.MaxKeys > 0 {
		maxKeys = aws.Int32(int32(page.MaxKeys))
	}
	resp, err := t.client.ListObjects(ctx, &service.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Delimiter: delimiter,
		Marker:    page.Marker,
		MaxKeys:   maxKeys,
		Prefix:    prefix,
	})
	if err != nil {
		return nil, err
	}
	for i := range resp.Contents {
		*resp.Contents[i].ETag = strings.Trim(*resp.Contents[i].ETag, `"`)
	}
	return resp, nil
}

// ListObjectsV2 is a convenience wrapper around the AWS SDK's ListObjectsV2 API.
func (t *S3Tester) ListObjectsV2(ctx context.Context, bucket string, prefix, delimiter *string, page s3.ListObjectsPage) (*service.ListObjectsV2Output, error) {
	var maxKeys *int32
	if page.MaxKeys > 0 {
		maxKeys = aws.Int32(int32(page.MaxKeys))
	}
	resp, err := t.client.ListObjectsV2(ctx, &service.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		FetchOwner:        page.FetchOwner,
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
	hash := s3.ParseETag(*resp.ETag)
	return hash[:], nil
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

// UploadPartCopy copies a single part from an existing object as part of a
// multipart upload.
func (t *S3Tester) UploadPartCopy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partNumber int32, rnge *s3.ObjectRange) (*service.UploadPartCopyOutput, error) {
	var copySourceRange *string
	if rnge != nil {
		copySourceRange = aws.String(fmt.Sprintf("bytes=%d-%d", rnge.Start, rnge.Start+rnge.Length-1))
	}
	input := &service.UploadPartCopyInput{
		CopySource:      aws.String(fmt.Sprintf("%s/%s", srcBucket, url.QueryEscape(srcObject))),
		Bucket:          aws.String(dstBucket),
		Key:             aws.String(dstObject),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int32(partNumber),
		CopySourceRange: copySourceRange,
	}
	return t.client.UploadPartCopy(ctx, input)
}

// ListParts lists uploaded parts for an in-progress multipart upload.
func (t *S3Tester) ListParts(ctx context.Context, bucket, object, uploadID string, marker *string, maxParts *int32) (*service.ListPartsOutput, error) {
	input := &service.ListPartsInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(object),
		UploadId:         aws.String(uploadID),
		PartNumberMarker: marker,
		MaxParts:         maxParts,
	}
	return t.client.ListParts(ctx, input)
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

// PutBucketLifecycleConfiguration is a convenience wrapper around the AWS SDK's
// PutBucketLifecycleConfiguration API.
func (t *S3Tester) PutBucketLifecycleConfiguration(ctx context.Context, bucket string, rules []types.LifecycleRule) error {
	_, err := t.client.PutBucketLifecycleConfiguration(ctx, &service.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	return err
}

// GetBucketLifecycleConfiguration is a convenience wrapper around the AWS SDK's
// GetBucketLifecycleConfiguration API.
func (t *S3Tester) GetBucketLifecycleConfiguration(ctx context.Context, bucket string) (*service.GetBucketLifecycleConfigurationOutput, error) {
	return t.client.GetBucketLifecycleConfiguration(ctx, &service.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
}

// DeleteBucketLifecycle is a convenience wrapper around the AWS SDK's
// DeleteBucketLifecycle API.
func (t *S3Tester) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	_, err := t.client.DeleteBucketLifecycle(ctx, &service.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucket),
	})
	return err
}

// PutBucketVersioning sets the versioning status of a bucket.
func (t *S3Tester) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	_, err := t.client.PutBucketVersioning(ctx, &service.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: status},
	})
	return err
}

// GetBucketVersioning returns the versioning status of a bucket.
func (t *S3Tester) GetBucketVersioning(ctx context.Context, bucket string) (types.BucketVersioningStatus, error) {
	resp, err := t.client.GetBucketVersioning(ctx, &service.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

// PutObjectVersion puts an object and returns the version ID assigned by the
// backend ("" when the bucket is unversioned).
func (t *S3Tester) PutObjectVersion(ctx context.Context, bucket, object string, data []byte) (string, error) {
	resp, err := t.client.PutObject(ctx, &service.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.VersionId), nil
}

// GetObjectVersion fetches a specific version of an object (the current version
// when versionID is nil) and returns its body.
func (t *S3Tester) GetObjectVersion(ctx context.Context, bucket, object string, versionID *string) ([]byte, error) {
	resp, err := t.client.GetObject(ctx, &service.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: versionID,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// DeleteObjectVersion deletes a specific version of an object (the current
// version when versionID is nil).
func (t *S3Tester) DeleteObjectVersion(ctx context.Context, bucket, object string, versionID *string) (*service.DeleteObjectOutput, error) {
	return t.client.DeleteObject(ctx, &service.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: versionID,
	})
}

// CopyObjectVersion copies a source object onto the destination key, optionally
// addressing a specific source version when srcVersionID is non-nil.
func (t *S3Tester) CopyObjectVersion(ctx context.Context, srcBucket, srcObject string, srcVersionID *string, dstBucket, dstObject string) (*service.CopyObjectOutput, error) {
	source := fmt.Sprintf("%s/%s", srcBucket, url.QueryEscape(srcObject))
	if srcVersionID != nil {
		source += "?versionId=" + url.QueryEscape(*srcVersionID)
	}
	return t.client.CopyObject(ctx, &service.CopyObjectInput{
		CopySource: aws.String(source),
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstObject),
	})
}

// ListObjectVersionsPage issues a single ListObjectVersions request from the
// given input (its Bucket is set automatically). Unlike ListObjectVersions it
// exposes the full input, including the key and version-id markers needed to
// paginate. The caller's input is not modified.
func (t *S3Tester) ListObjectVersionsPage(ctx context.Context, bucket string, in *service.ListObjectVersionsInput) (*service.ListObjectVersionsOutput, error) {
	var req service.ListObjectVersionsInput
	if in != nil {
		req = *in
	}
	req.Bucket = aws.String(bucket)
	resp, err := t.client.ListObjectVersions(ctx, &req)
	if err != nil {
		return nil, err
	}
	for i := range resp.Versions {
		if resp.Versions[i].ETag != nil {
			*resp.Versions[i].ETag = strings.Trim(*resp.Versions[i].ETag, `"`)
		}
	}
	return resp, nil
}

type testerCfg struct {
	backend     s3.Backend
	sdk         sia.SDK
	keyPairs    []keyPair
	serviceOpts []func(*service.Options)
	tls         bool
}

type keyPair struct {
	owner       string
	accessKeyID string
	secretKey   string
}

// TesterOption is an option for configuring the S3Tester.
type TesterOption func(*testerCfg)

// WithKeyPair registers an additional user and key pair with the backend. It
// has no effect when an explicit backend is passed to NewTester via
// WithBackend.
func WithKeyPair(owner, accessKeyID, secretKey string) TesterOption {
	return func(cfg *testerCfg) {
		cfg.keyPairs = append(cfg.keyPairs, keyPair{
			owner:       owner,
			accessKeyID: accessKeyID,
			secretKey:   secretKey,
		})
	}
}

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

// WithSDK sets the Sia SDK used by NewBackend. It has no effect when an
// explicit backend is passed via WithBackend.
func WithSDK(sdk sia.SDK) TesterOption {
	return func(cfg *testerCfg) {
		cfg.sdk = sdk
	}
}

// WithTLS configures the tester to use TLS.
func WithTLS() TesterOption {
	return func(cfg *testerCfg) {
		cfg.tls = true
	}
}

// Sia wraps a *sia.Sia backend created by NewBackend, additionally exposing the
// temporary data directory so tests can assert on the backend's on-disk state.
type Sia struct {
	*sia.Sia
	Dir string
}

// UploadDir returns the on-disk directory for the given multipart upload.
func (s *Sia) UploadDir(uploadID s3.UploadID) string {
	return filepath.Join(s.Dir, sia.UploadsDirectory, uploadID.String())
}

// NewBackend creates a Sia backend backed by an in-memory SDK and a SQLite
// store in a temporary directory. The default test key pair as well as any
// key pairs provided via WithKeyPair are registered with the backend.
func NewBackend(t testing.TB, opts ...TesterOption) (*Sia, *sqlite.Store) {
	t.Helper()

	cfg := &testerCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.backend != nil {
		t.Fatal("WithBackend cannot be combined with NewBackend")
	}

	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	sdk := cfg.sdk
	if sdk == nil {
		sdk = NewMemorySDK()
	}
	return &Sia{
		Sia: newSiaBackend(t, dir, store, sdk, log, cfg.keyPairs),
		Dir: dir,
	}, store
}

// NewCustomTester creates a new S3Tester using a Sia backend built from the
// provided store and SDK.
func NewCustomTester(t testing.TB, dir string, store sia.Store, sdk sia.SDK, log *zap.Logger, opts ...TesterOption) *S3Tester {
	t.Helper()

	cfg := &testerCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.backend != nil {
		t.Fatal("WithBackend cannot be combined with NewCustomTester")
	}

	backend := newSiaBackend(t, dir, store, sdk, log, cfg.keyPairs)
	return NewTester(t, append([]TesterOption{WithBackend(backend)}, opts...)...)
}

// newSiaBackend creates a Sia backend with the default test key pair and any
// additional key pairs registered in the store.
func newSiaBackend(t testing.TB, dir string, store sia.Store, sdk sia.SDK, log *zap.Logger, keyPairs []keyPair) *sia.Sia {
	t.Helper()

	// ensure the test users and access keys exist in the store
	defaultKeyPair := keyPair{owner: Owner, accessKeyID: AccessKeyID, secretKey: SecretAccessKey}
	for _, kp := range append([]keyPair{defaultKeyPair}, keyPairs...) {
		if err := store.CreateUser(kp.owner); err != nil && !errors.Is(err, sia.ErrUserAlreadyExists) {
			t.Fatal(err)
		}
		if err := store.CreateAccessKey(kp.owner, kp.accessKeyID, kp.secretKey); err != nil && !errors.Is(err, sia.ErrAccessKeyAlreadyExists) {
			t.Fatal(err)
		}
	}

	backend, err := sia.New(t.Context(), sdk, store, dir,
		sia.WithUploadDisabled(),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	return backend
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
		cfg.backend, _ = NewBackend(t, opts...)
	}

	handler := s3.New(cfg.backend,
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

// ObjectIdentifiers is a convenience function to create a slice of
// ObjectIdentifiers from object keys.
func ObjectIdentifiers(keys ...string) []types.ObjectIdentifier {
	var objs []types.ObjectIdentifier
	for _, o := range keys {
		objs = append(objs, types.ObjectIdentifier{
			Key: aws.String(o),
		})
	}
	return objs
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
