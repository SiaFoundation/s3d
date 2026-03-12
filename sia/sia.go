package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

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
	CopyObject(srcBucket, srcName, dstBucket, dstName string, meta map[string]string, replace bool) (*objects.Object, error)
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) error
	GetObject(accessKeyID *string, bucket, object string, partNumber *int32) (*objects.Object, error)
	HeadBucket(accessKeyID, bucket string) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error)
	OrphanedObjects() ([]types.Hash256, error)
	PutObject(accessKeyID, bucket, name string, obj *objects.Object, updateModTime bool) error
	RemoveOrphanedObject(objectID types.Hash256) error
	AbortMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error)
	CreateMultipartUpload(bucket, name string, uploadID s3.UploadID, meta map[string]string) error
	CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, objectID types.Hash256, contentMD5 [16]byte, contentLength int64) error
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

	go sia.processOrphans(ctx)

	return sia, nil
}

// processOrphans periodically processes orphaned objects.
func (s *Sia) processOrphans(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	// process once immediately at startup
	s.ProcessOrphans(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.ProcessOrphans(ctx)
		}
	}
}

// ProcessOrphans unpins orphaned objects from the indexer and removes them
// from the orphaned_objects table.
func (s *Sia) ProcessOrphans(ctx context.Context) {
	orphans, err := s.store.OrphanedObjects()
	if err != nil {
		s.logger.Error("failed to fetch orphaned objects", zap.Error(err))
		return
	}
	for _, id := range orphans {
		if err := s.sdk.DeleteObject(ctx, id); err != nil && !errors.Is(err, slabs.ErrObjectNotFound) {
			s.logger.Error("failed to unpin object from indexer", zap.Error(err), zap.Stringer("objectID", &id))
			continue
		}
		if err := s.store.RemoveOrphanedObject(id); err != nil {
			s.logger.Error("failed to remove orphaned object", zap.Error(err), zap.Stringer("objectID", &id))
		}
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
