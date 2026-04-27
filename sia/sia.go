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
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const (
	// UploadsDirectory is the directory name used for storing pending uploads.
	UploadsDirectory = "uploads"
)

// ErrNoAccessKey is returned when no access key is provided to the Sia backend.
var ErrNoAccessKey = errors.New("sia backend requires at least one access key")

// Option is a configuration option for the S3 API handler.
type Option func(*Sia)

// WithLogger sets the logger for the Sia backend.
func WithLogger(logger *zap.Logger) Option {
	return func(s *Sia) {
		s.logger = logger.Named("sia")
	}
}

// WithUploadWaste sets the maximum percentage of wasted space tolerated per
// slab.
func WithUploadWaste(pct float64) Option {
	return func(s *Sia) {
		s.uploadWastePct = pct
	}
}

// WithUploadDisabled disables the background upload loop.
func WithUploadDisabled() Option {
	return func(s *Sia) {
		s.uploadDisabled = true
	}
}

// WithKeyPair adds a key pair to the Sia backend.
func WithKeyPair(accessKeyID, secretKey string) func(*Sia) {
	return func(mb *Sia) {
		if accessKeyID == "" || secretKey == "" {
			return
		}
		mb.accessKeys[accessKeyID] = auth.SecretAccessKey(secretKey)
	}
}

// Sia implements the s3.Backend interface for storing data on Sia.
type Sia struct {
	sdk   SDK
	store Store

	directory  string
	accessKeys map[string]auth.SecretAccessKey

	slabSize       int64
	uploadWastePct float64
	uploadDisabled bool

	tg     *threadgroup.ThreadGroup
	logger *zap.Logger
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	DeleteObject(ctx context.Context, id types.Hash256) error
	Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error
	ObjectEvents(ctx context.Context, cursor sdk.ObjectsCursor, limit int) ([]sdk.ObjectEvent, error)
	SlabSize() (int64, error)
	UploadPacked() (PackedUpload, error)
	PinObject(ctx context.Context, obj sdk.Object) error
	SealObject(obj sdk.Object) sdk.SealedObject
	UnsealObject(sealed sdk.SealedObject) (sdk.Object, error)
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	CopyObject(srcBucket, srcName, dstBucket, dstName string, meta map[string]string, replace bool) (*objects.Object, error)
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (*string, error)
	GetObject(accessKeyID *string, bucket, object string, partNumber *int32) (*objects.Object, error)
	HeadBucket(accessKeyID, bucket string) error
	ObjectsCursor() (sdk.ObjectsCursor, error)
	SetObjectsCursor(cursor sdk.ObjectsCursor) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error)
	ObjectParts(bucket, name string) ([]objects.Part, error)
	ObjectsForUpload() ([]objects.ObjectForUpload, error)
	OrphanedObjects(limit int) ([]types.Hash256, error)
	PutObject(accessKeyID, bucket, name string, contentMD5 [16]byte, meta map[string]string, length int64, fileName *string, updateModTime bool) error
	MarkObjectUploaded(bucket, name string, contentMD5 [16]byte, sealed sdk.SealedObject) error
	UpdateSiaObject(siaObject objects.SiaObject) (bool, error)
	RemoveOrphanedObject(objectID types.Hash256) error
	AbortMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error)
	CreateMultipartUpload(bucket, name string, uploadID s3.UploadID, meta map[string]string) error
	CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, contentMD5 [16]byte, contentLength int64) error
	HasMultipartUpload(bucket, name string, uploadID s3.UploadID) error
	ListMultipartUploads(bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error)
	ListParts(bucket, name string, uploadID s3.UploadID, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error)
	MultipartParts(bucket, name string, uploadID s3.UploadID) ([]objects.Part, error)
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, directory string, opts ...Option) (*Sia, error) {
	sia := &Sia{
		sdk:   sdk,
		store: store,

		directory:      directory,
		accessKeys:     make(map[string]auth.SecretAccessKey),
		uploadWastePct: DefaultUploadWastePct,

		logger: zap.NewNop(),
		tg:     threadgroup.New(),
	}
	for _, opt := range opts {
		opt(sia)
	}
	if len(sia.accessKeys) == 0 {
		return nil, ErrNoAccessKey
	} else if sia.uploadWastePct <= 0 {
		return nil, errors.New("upload waste percentage must be greater than 0")
	}

	dir := filepath.Join(sia.directory, UploadsDirectory)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	// initialize slab size if the upload loop is enabled
	slabSize, err := sia.sdk.SlabSize()
	if err != nil {
		return nil, fmt.Errorf("failed to determine slab size: %w", err)
	}
	sia.slabSize = slabSize

	// TODO: clean up orphaned uploads and multipart uploads in uploads
	// directory on startup

	launchBgLoop := func(loopFn func(context.Context)) error {
		ctx, cancel, err := sia.tg.AddContext(ctx)
		if err != nil {
			return err
		}
		go func() {
			defer cancel()
			loopFn(ctx)
		}()
		return nil
	}

	launchBgLoop(sia.processOrphansLoop)
	launchBgLoop(sia.syncMetadataLoop)
	launchBgLoop(sia.uploadLoop)

	return sia, nil
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

// syncMetadataLoop periodically syncs object metadata from the indexer.
func (s *Sia) syncMetadataLoop(ctx context.Context) {
	t := time.NewTicker(24 * time.Hour)
	defer t.Stop()

	// sync once on startup
	s.syncMetadata(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.syncMetadata(ctx)
		}
	}
}

// syncMetadata fetches object events from the indexer since the last sync
// and applies metadata updates to local objects.
func (s *Sia) syncMetadata(ctx context.Context) {
	const batchSize = 100

	// fetch the cursor
	cursor, err := s.store.ObjectsCursor()
	if err != nil {
		s.logger.Error("failed to get objects cursor", zap.Error(err))
		return
	}

	// fetch and apply events
	var synced int
	for ctx.Err() == nil {
		events, err := s.sdk.ObjectEvents(ctx, cursor, batchSize)
		if err != nil {
			s.logger.Error("failed to fetch object events", zap.Error(err))
			break
		} else if len(events) == 0 {
			break
		}

		var failed bool
		for _, ev := range events {
			if ev.Deleted {
				s.logger.Debug("skipping deleted object event", zap.Stringer("objectID", &ev.Key))
				continue
			} else if ev.Object == nil {
				s.logger.Warn("skipping event with nil object", zap.Stringer("objectID", &ev.Key))
				continue
			}

			sealed := s.sdk.SealObject(*ev.Object)
			updated, err := s.store.UpdateSiaObject(objects.SiaObject{ID: sealed.ID(), Sealed: sealed})
			if err != nil {
				s.logger.Error("failed to update Sia object in store",
					zap.Stringer("objectID", ev.Object.ID()),
					zap.Error(err))
				failed = true
				break
			} else if updated {
				synced++
			}
		}
		if failed {
			break
		}

		// advance the cursor to the last event
		last := events[len(events)-1]
		cursor = sdk.ObjectsCursor{After: last.UpdatedAt, Key: last.Key}
		if err := s.store.SetObjectsCursor(cursor); err != nil {
			s.logger.Error("failed to update objects cursor", zap.Error(err))
			break
		}
	}

	if synced > 0 {
		s.logger.Info("synced object metadata", zap.Int("synced", synced))
	}
}
