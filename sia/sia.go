package sia

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

const (
	// MultipartDirectory is the directory name used for storing multipart
	// uploads.
	MultipartDirectory = "multipart"
)

// Option is a configuration option for the S3 API handler.
type Option func(*Sia)

// WithLogger sets the logger for the S3 API handler.
func WithLogger(logger *zap.Logger) Option {
	return func(s *Sia) {
		s.logger = logger.Named("sia")
	}
}

// WithKeyPair adds a key pair to the Sia backend.
func WithKeyPair(accessKeyID, secretKey string) func(*Sia) {
	return func(mb *Sia) {
		mb.accessKeys[accessKeyID] = auth.SecretAccessKey(secretKey)
	}
}

// Sia implements the s3.Backend interface for storing data on Sia.
type Sia struct {
	sdk    SDK
	logger *zap.Logger
	store  Store

	unpinMu    sync.Mutex
	directory  string
	accessKeys map[string]auth.SecretAccessKey
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	DeleteObject(ctx context.Context, id types.Hash256) error
	Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error
	Object(ctx context.Context, id types.Hash256) (sdk.Object, error)
	Upload(ctx context.Context, r io.Reader) (sdk.Object, error)
	SealObject(obj sdk.Object) slabs.SealedObject
	UnsealObject(sealed slabs.SealedObject) (sdk.Object, error)
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (types.Hash256, bool, error)
	GetObject(accessKeyID *string, bucket, object string, partNumber *int32) (*objects.Object, error)
	HeadBucket(accessKeyID, bucket string) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error)
	ObjectRefCount(objectID types.Hash256) (int64, error)
	PutObject(accessKeyID, bucket, name string, obj *objects.Object, updateModTime bool) (types.Hash256, bool, error)
	AbortMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error)
	CreateMultipartUpload(bucket, name string, uploadID s3.UploadID, meta map[string]string) error
	CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, objectID types.Hash256, contentMD5 [16]byte, contentLength int64) (types.Hash256, bool, error)
	HasMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	ListMultipartUploads(bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error)
	ListParts(bucket, name string, uploadID s3.UploadID, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error)
	MultipartParts(bucket, name string, uploadID s3.UploadID) ([]objects.Part, error)
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, directory string, opts ...Option) (*Sia, error) {
	directory = filepath.Join(directory, MultipartDirectory)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create multipart upload directory: %w", err)
	}

	sia := &Sia{
		logger: zap.NewNop(),
		sdk:    sdk,
		store:  store,

		directory:  directory,
		accessKeys: make(map[string]auth.SecretAccessKey),
	}
	for _, opt := range opts {
		opt(sia)
	}
	if len(sia.accessKeys) == 0 {
		return nil, fmt.Errorf("sia backend requires both access key and secret key")
	}

	return sia, nil
}

// tryUnpinObject re-checks the reference count for the given object ID under
// the unpin mutex and deletes it from the indexer if no references remain.
// It skips the zero hash (empty objects are never pinned).
func (s *Sia) tryUnpinObject(ctx context.Context, objectID types.Hash256) {
	if objectID == (types.Hash256{}) {
		return // empty objects are never pinned
	}

	s.unpinMu.Lock()
	count, err := s.store.ObjectRefCount(objectID)
	if err != nil {
		s.unpinMu.Unlock()
		s.logger.Error("failed to check object ref count", zap.Error(err), zap.Stringer("objectID", objectID))
		return
	}
	if count > 0 {
		s.unpinMu.Unlock()
		return
	}
	s.unpinMu.Unlock()

	if err := s.sdk.DeleteObject(ctx, objectID); err != nil {
		s.logger.Error("failed to unpin object from indexer", zap.Error(err), zap.Stringer("objectID", objectID))
	}
}

// LoadSecret loads the secret key for the given access key ID. If the access
// key wasn't found, the error s3errs.ErrInvalidAccessKeyID is returned.
func (s *Sia) LoadSecret(ctx context.Context, accessKeyID string) (auth.SecretAccessKey, error) {
	secret, ok := s.accessKeys[accessKeyID]
	if !ok {
		return nil, s3errs.ErrInvalidAccessKeyId
	}
	return slices.Clone(secret), nil
}
