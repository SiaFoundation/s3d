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
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

const (
	// MultipartDirectory is the directory name used for storing multipart
	// uploads.
	MultipartDirectory = "multipart"

	// PackingDirectory is the directory name used for storing small objects
	// on disk before they are packed and uploaded to Sia together to minimize
	// waste.
	PackingDirectory = "packing"
)

// Option is a configuration option for the Sia backend.
type Option func(*Sia)

// WithLogger sets the logger for the Sia backend.
func WithLogger(logger *zap.Logger) Option {
	return func(s *Sia) {
		s.logger = logger.Named("sia")
	}
}

// WithKeyPair adds a key pair to the Sia backend.
func WithKeyPair(accessKeyID, secretKey string) Option {
	return func(s *Sia) {
		s.accessKeys[accessKeyID] = auth.SecretAccessKey(secretKey)
	}
}

// WithPackingWaste sets the maximum percentage of wasted space tolerated per
// slab. Objects whose upload would waste more than this fraction of a slab are
// written to disk and batched together. Pass 0 to disable packing.
func WithPackingWaste(pct float64) Option {
	return func(s *Sia) {
		s.packingWastePct = pct
	}
}

// Sia implements the s3.Backend interface for storing data on Sia.
type Sia struct {
	sdk   SDK
	store Store

	multipartDir string
	packingDir   string
	accessKeys   map[string]auth.SecretAccessKey

	slabSize        int64
	packingWastePct float64
	triggerPackChan chan struct{}

	logger *zap.Logger
	tg     *threadgroup.ThreadGroup
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	DeleteObject(ctx context.Context, id types.Hash256) error
	Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error
	Object(ctx context.Context, id types.Hash256) (sdk.Object, error)
	SlabSize() (int64, error)
	Upload(ctx context.Context, r io.Reader) (sdk.Object, error)
	UploadPacked() (PackedUpload, error)
	PinObject(ctx context.Context, obj sdk.Object) error
	SealObject(obj sdk.Object) slabs.SealedObject
	UnsealObject(sealed slabs.SealedObject) (sdk.Object, error)
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	CopyObject(srcBucket, srcName, dstBucket, dstName string, meta map[string]string, replace bool, dstFilename *string) (*objects.Object, *string, error)
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (*string, error)
	FinalizeObject(bucket, name, expectedFilename string, objectID types.Hash256, siaObject slabs.SealedObject) error
	GetObject(accessKeyID *string, bucket, object string, partNumber *int32) (*objects.Object, error)
	HeadBucket(accessKeyID, bucket string) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error)
	ObjectsForPacking() ([]objects.PackedObject, error)
	OrphanedObjects(limit int) ([]types.Hash256, error)
	PutObject(bucket, name string, obj *objects.Object, updateModTime bool) (*string, error)
	RemoveOrphanedObject(objectID types.Hash256) error
	AbortMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error)
	CreateMultipartUpload(bucket, name string, uploadID s3.UploadID, meta map[string]string) error
	CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, objectID types.Hash256, contentMD5 [16]byte, contentLength int64, filename *string) (*string, error)
	HasMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	ListMultipartUploads(bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error)
	ListParts(bucket, name string, uploadID s3.UploadID, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error)
	MultipartParts(bucket, name string, uploadID s3.UploadID) ([]objects.Part, error)
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, directory string, opts ...Option) (*Sia, error) {
	multipartDir := filepath.Join(directory, MultipartDirectory)
	if err := os.MkdirAll(multipartDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create multipart upload directory: %w", err)
	}

	packingDir := filepath.Join(directory, PackingDirectory)
	if err := os.MkdirAll(packingDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create packing directory: %w", err)
	}

	s := &Sia{
		logger:          zap.NewNop(),
		sdk:             sdk,
		store:           store,
		tg:              threadgroup.New(),
		multipartDir:    multipartDir,
		packingDir:      packingDir,
		packingWastePct: DefaultPackingWastePct,
		accessKeys:      make(map[string]auth.SecretAccessKey),
	}
	for _, opt := range opts {
		opt(s)
	}

	// calculate slab size from the SDK's configured redundancy settings
	if s.packingWastePct > 0 {
		slabSize, err := s.sdk.SlabSize()
		if err != nil {
			return nil, fmt.Errorf("failed to determine slab size: %w", err)
		}
		s.slabSize = slabSize
		s.triggerPackChan = make(chan struct{}, 1)
	}

	if len(s.accessKeys) == 0 {
		return nil, fmt.Errorf("sia backend requires both access key and secret key")
	}

	ctx, cancel, err := s.tg.AddContext(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()
		s.processOrphansLoop(ctx)
	}()

	if s.packingWastePct > 0 {
		ctx, cancel, err := s.tg.AddContext(ctx)
		if err != nil {
			return nil, err
		}
		go func() {
			defer cancel()
			s.packingLoop(ctx)
		}()
	}

	// TODO: add loop to cleanup orphaned packed objects on disk

	return s, nil
}

// Close shuts down the Sia backend and waits for background goroutines.
func (s *Sia) Close() error {
	s.tg.Stop()
	return nil
}

// processOrphansLoop periodically processes orphaned objects.
func (s *Sia) processOrphansLoop(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()

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
// from the orphaned_objects table in batches.
//
// NOTE: there is no race condition with re-uploaded objects here because
// re-uploading an object always creates a new ID. The only way to create
// duplicate IDs is via copying, and once an object is orphaned it can no
// longer be copied.
func (s *Sia) ProcessOrphans(ctx context.Context) {
	const batchSize = 100
	var totalUnpinned int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		orphans, err := s.store.OrphanedObjects(batchSize)
		if err != nil {
			s.logger.Error("failed to fetch orphaned objects", zap.Error(err))
			return
		}
		for _, id := range orphans {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := s.sdk.DeleteObject(ctx, id); err != nil && !errors.Is(err, slabs.ErrObjectNotFound) {
				s.logger.Error("failed to unpin object from indexer", zap.Error(err), zap.Stringer("objectID", &id))
				return
			}
			if err := s.store.RemoveOrphanedObject(id); err != nil {
				s.logger.Error("failed to remove orphaned object", zap.Error(err), zap.Stringer("objectID", &id))
				return
			}
			totalUnpinned++
		}
		if len(orphans) < batchSize {
			break
		}
	}
	if totalUnpinned > 0 {
		s.logger.Info("processed orphaned objects", zap.Int("unpinned", totalUnpinned))
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
