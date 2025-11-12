package sia

import (
	"context"
	"fmt"
	"io"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Option is a configuration option for the S3 API handler.
type Option func(*Sia)

// WithLogger sets the logger for the S3 API handler.
func WithLogger(logger *zap.Logger) Option {
	return func(s *Sia) {
		s.logger = logger.Named("sia")
	}
}

// Sia implements the s3.Backend interface for storing data on Sia.
type Sia struct {
	logger *zap.Logger

	accessKey string
	secretKey auth.SecretAccessKey
}

// New creates a new Sia backend instance.
func New(ctx context.Context, accessKey, secretKey string, opts ...Option) (*Sia, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("sia backend requires both access key and secret key")
	}

	sia := &Sia{
		logger:    zap.NewNop(),
		accessKey: accessKey,
		secretKey: auth.SecretAccessKey(secretKey),
	}
	for _, opt := range opts {
		opt(sia)
	}

	return sia, nil
}

// LoadSecret loads the secret key for the given access key ID. If the access
// key wasn't found, the error s3errs.ErrInvalidAccessKeyID is returned.
func (s *Sia) LoadSecret(ctx context.Context, accessKeyID string) (auth.SecretAccessKey, error) {
	if accessKeyID != s.accessKey {
		return nil, s3errs.ErrInvalidAccessKeyId
	}
	return s.secretKey, nil
}

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, meta map[string]string) (*s3.CopyObjectResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// CreateBucket creates a new bucket with the given name for the user
// identified by the given access key.
func (s *Sia) CreateBucket(ctx context.Context, accessKeyID, name string) error {
	return s3errs.ErrNotImplemented
}

// DeleteBucket deletes the bucket with the given name for the user
// identified by the given access key.
func (s *Sia) DeleteBucket(ctx context.Context, accessKeyID, name string) error {
	return s3errs.ErrNotImplemented
}

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket, object string) (*s3.DeleteObjectResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// DeleteObjects deletes multiple objects from the specified bucket for the
// user identified by the given access key.
func (s *Sia) DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []s3.ObjectID) (*s3.ObjectsDeleteResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// GetObject retrieves the object with the given key from the specified
// bucket for the user identified by the given access key. The provided
// range is either nil if no range was requested, or contains the requested,
// byte range.
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return nil, s3errs.ErrNotImplemented
}

// HeadBucket checks if the bucket with the given name exists and is
// accessible for the user identified by the given access key.
func (s *Sia) HeadBucket(ctx context.Context, accessKeyID, name string) error {
	return s3errs.ErrNotImplemented
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return nil, s3errs.ErrNotImplemented
}

// ListBuckets lists all available buckets for the user identified by the
// given access key.
func (s *Sia) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	return nil, s3errs.ErrNotImplemented
}

// ListObjects lists objects in the specified bucket for the user identified
// by the given access key. The backend should use the prefix to limit the
// contents of the bucket and sort the results into the Contents and
// CommonPrefixes fields of the returned ObjectsListResult.
func (s *Sia) ListObjects(ctx context.Context, accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// PutObject puts an object with the given key into the specified bucket.
func (s *Sia) PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts s3.PutObjectOptions) (*s3.PutObjectResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// CreateMultipartUpload creates a new multipart upload.
func (s *Sia) CreateMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, opts s3.CreateMultipartUploadOptions) (*s3.CreateMultipartUploadResult, error) {
	return nil, s3errs.ErrNotImplemented
}
