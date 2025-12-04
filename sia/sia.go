package sia

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"slices"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
)

const (
	// MultipartDir is the directory name used for storing multipart uploads.
	MultipartDir = "multipart"
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

	directory string
	accessKey string
	secretKey auth.SecretAccessKey
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error
	Object(ctx context.Context, id types.Hash256) (sdk.Object, error)
	Upload(ctx context.Context, r io.Reader) (sdk.Object, error)
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket, name string) error
	GetObject(accessKeyID *string, bucket, object string) (*objects.Object, error)
	HeadBucket(accessKeyID, bucket string) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	PutObject(accessKeyID, bucket, name string, obj *objects.Object) error

	CreateMultipartUpload(bucket, name string, meta map[string]string) (string, error)
	AbortMultipartUpload(bucket, name, uploadID string) error
	HasMultipartUpload(bucket, name, uploadID string) error

	AddMultipartPart(uploadID string, partNumber int) error
	FinishMultipartPart(uploadID string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) error
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, directory, accessKey, secretKey string, opts ...Option) (*Sia, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("sia backend requires both access key and secret key")
	}

	sia := &Sia{
		logger: zap.NewNop(),
		sdk:    sdk,
		store:  store,

		directory: filepath.Join(directory, MultipartDir),
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
