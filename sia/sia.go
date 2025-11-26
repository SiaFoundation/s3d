package sia

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
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
	sdk    SDK
	logger *zap.Logger
	store  Store

	accessKey string
	secretKey auth.SecretAccessKey
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error
	PinObject(ctx context.Context, obj sdk.Object) error
	Upload(ctx context.Context, r io.Reader) (sdk.Object, error)

	OpenSealedObject(so slabs.SealedObject) (sdk.Object, error)
	SealObject(obj sdk.Object) slabs.SealedObject
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket, name string) error
	GetObject(accessKeyID *string, bucket, object string) (slabs.SealedObject, error)
	HeadBucket(accessKeyID, bucket string) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	PutObject(accessKeyID, bucket, name string, obj slabs.SealedObject) error
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, accessKey, secretKey string, opts ...Option) (*Sia, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("sia backend requires both access key and secret key")
	}

	sia := &Sia{
		logger: zap.NewNop(),
		sdk:    sdk,
		store:  store,

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
	return slices.Clone(s.secretKey), nil
}

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// DeleteObjects deletes multiple objects from the specified bucket for the
// user identified by the given access key.
func (s *Sia) DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []s3.ObjectID) (*s3.ObjectsDeleteResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// CreateMultipartUpload creates a new multipart upload.
func (s *Sia) CreateMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, opts s3.CreateMultipartUploadOptions) (*s3.CreateMultipartUploadResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// ListMultipartUploads lists in-progress multipart uploads.
func (s *Sia) ListMultipartUploads(ctx context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions) (*s3.ListMultipartUploadsResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// AbortMultipartUpload aborts a multipart upload.
func (s *Sia) AbortMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string) error {
	return s3errs.ErrNotImplemented
}

// UploadPart uploads a single multipart part.
func (s *Sia) UploadPart(ctx context.Context, accessKeyID, bucket, object, uploadID string, r io.Reader, opts s3.UploadPartOptions) (*s3.UploadPartResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Sia) ListParts(ctx context.Context, accessKeyID, bucket, object, uploadID string, page s3.ListPartsPage) (*s3.ListPartsResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// CompleteMultipartUpload completes a multipart upload.
func (s *Sia) CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string, parts []s3.CompletedPart) (*s3.CompleteMultipartUploadResult, error) {
	return nil, s3errs.ErrNotImplemented
}
