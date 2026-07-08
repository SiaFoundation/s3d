package sia

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SiaFoundation/s3d/build"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// orphanLoopInterval is the interval at which the background loop for
	// processing orphaned objects runs.
	orphanLoopInterval = time.Hour

	// defaultLifecycleLoopInterval is the default interval at which the
	// background lifecycle loop runs.
	defaultLifecycleLoopInterval = time.Hour

	// defaultLifecycleDayDuration is the default wall-clock duration treated as
	// a single "day" when evaluating lifecycle Days windows.
	defaultLifecycleDayDuration = 24 * time.Hour

	// defaultDiskUsageTimeout is the maximum time a request waits for disk
	// space to become available before failing with ErrSlowDown.
	defaultDiskUsageTimeout = 2 * time.Minute

	// UploadsDirectory is the directory name used for storing pending uploads.
	UploadsDirectory = "uploads"
)

var (
	// ErrUserAlreadyExists is returned when creating a user that already exists.
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrAccessKeyAlreadyExists is returned when creating an access key that
	// already exists.
	ErrAccessKeyAlreadyExists = errors.New("access key already exists")
)

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

// WithLifecycleLoopInterval sets how often the background lifecycle loop
// evaluates bucket lifecycle rules.
func WithLifecycleLoopInterval(d time.Duration) Option {
	return func(s *Sia) {
		if d > 0 {
			s.lifecycleLoopInterval = d
		}
	}
}

// WithLifecycleDayDuration sets the wall-clock duration treated as a single
// "day" when evaluating lifecycle Days windows. It defaults to 24 hours; tests
// use a shorter value to exercise expiration without waiting real days.
func WithLifecycleDayDuration(d time.Duration) Option {
	return func(s *Sia) {
		if d > 0 {
			s.lifecycleDayDuration = d
		}
	}
}

// WithDiskUsageLimit sets the maximum number of bytes that can be stored on
// disk pending upload to Sia. When the limit is reached, new uploads block
// until existing data has been offloaded. A value of 0 disables the limit.
func WithDiskUsageLimit(limit uint64) Option {
	return func(s *Sia) {
		s.diskUsageLimit = limit
	}
}

// Sia implements the s3.Backend interface for storing data on Sia.
type Sia struct {
	sdk   SDK
	store Store

	directory string

	slabSize         int64
	diskUsageLimit   uint64
	diskUsageTimeout time.Duration

	diskUsageMu   sync.Mutex
	diskUsageWake chan struct{}
	diskUsage     uint64

	uploadMu          sync.Mutex
	uploadDisabled    bool
	uploadOptimalSize int64
	uploadWastePct    float64

	lifecycleLoopInterval time.Duration
	lifecycleDayDuration  time.Duration

	pinMu   sync.Mutex
	pinWake chan struct{}

	lockedUploadsMu sync.Mutex
	lockedUploads   map[string]*lockedUpload

	failedUploads atomic.Int64

	tg     *threadgroup.ThreadGroup
	logger *zap.Logger
}

// SDK describes the SDK used to interact with Sia.
type SDK interface {
	Account(ctx context.Context) (app.AccountResponse, error)
	DeleteObject(ctx context.Context, id types.Hash256) error
	Download(obj sdk.Object, rnge *s3.ObjectRange) (io.ReadCloser, error)
	ObjectEvents(ctx context.Context, cursor slabs.Cursor, limit int) ([]sdk.ObjectEvent, error)
	OptimalDataSize() (int64, error)
	Upload(ctx context.Context, obj *sdk.Object, r io.Reader) error
	UploadPacked() (PackedUpload, error)
	PinObject(ctx context.Context, obj sdk.Object) error
	PruneSlabs(ctx context.Context, opts ...api.URLQueryParameterOption) error
	SealObject(obj sdk.Object) sdk.SealedObject
	UnsealObject(sealed sdk.SealedObject) (sdk.Object, error)
}

// AccessKeyInfo contains metadata about an access key.
type AccessKeyInfo struct {
	AccessKeyID string
	SecretKey   string
	UserName    string
}

// Store represents the storage backend used by the Sia backend.
type Store interface {
	// user and access key management
	CreateUser(name string) error
	DeleteUser(name string) error
	ListUsers() ([]string, error)
	CreateAccessKey(userName, accessKeyID, secretKey string) error
	DeleteAccessKey(accessKeyID string) error
	ListAccessKeys(userName *string) ([]AccessKeyInfo, error)
	LoadSecret(accessKeyID string) (string, error)
	UserNameForAccessKey(accessKeyID string) (string, error)

	AllFilenames() ([]string, error)
	CopyObject(accessKeyID, srcBucket, srcName string, srcVersion s3.VersionRequest, dstBucket, dstName string, opts s3.CopyObjectOptions) (*s3.CopyObjectResult, objects.OrphanedFile, error)
	CreateBucket(accessKeyID, bucket string) error
	DeleteBucket(accessKeyID, bucket string) error
	DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (string, bool, objects.OrphanedFile, error)
	GetObject(accessKeyID, bucket, object string, version s3.VersionRequest, partNumber *int32) (*objects.Object, error)
	DiskUsage() (uint64, error)
	HeadBucket(accessKeyID, bucket string) error
	GetBucketVersioning(accessKeyID, bucket string) (string, error)
	PutBucketVersioning(accessKeyID, bucket, status string) error
	ObjectsCursor() (slabs.Cursor, error)
	SetObjectsCursor(cursor slabs.Cursor) error
	ListBuckets(accessKeyID string) ([]s3.BucketInfo, error)
	ListObjects(accessKeyID, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error)
	ListObjectVersions(accessKeyID, bucket string, prefix s3.Prefix, page s3.ListObjectVersionsPage) (*s3.ObjectVersionsListResult, error)
	ObjectPartsByName(bucket, name, versionID string) ([]objects.Part, error)
	ObjectsForUpload() ([]objects.ObjectForUpload, error)
	OrphanedObjects(limit int) ([]types.Hash256, error)
	PutObject(accessKeyID, bucket, name string, opts objects.PutOptions) (string, objects.OrphanedFile, error)
	MarkObjectUploaded(bucket, name, versionID string, contentMD5 [16]byte, sealed sdk.SealedObject, pinBefore time.Time) error
	MarkObjectPinned(siaObjectID types.Hash256) ([]objects.OrphanedFile, error)
	ScheduleObjectForReupload(siaObjectID types.Hash256) error
	ObjectsForPinning(now time.Time, limit int) ([]objects.UnpinnedObject, error)
	NextPinningAttempt() (time.Time, bool, error)
	RescheduleUnpinnedObject(siaObjectID types.Hash256, nextAttemptAt time.Time) error
	UpdateSiaObjects(siaObjects []objects.SiaObject) (int64, error)
	RemoveOrphanedObject(objectID types.Hash256) error
	AbortMultipartUpload(accessKeyID, bucket, name string, uploadID s3.UploadID) (int64, error)
	AddMultipartPart(accessKeyID, bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, int64, error)
	CreateMultipartUpload(accessKeyID, bucket, name string, uploadID s3.UploadID, meta map[string]string) error
	CompleteMultipartUpload(accessKeyID, bucket, name string, uploadID s3.UploadID, contentMD5 [16]byte, contentLength int64, preconditions s3.ObjectPreconditions) (string, objects.OrphanedFile, error)
	HasMultipartUpload(accessKeyID, bucket, name string, uploadID s3.UploadID) (hasParts bool, err error)
	ListMultipartUploads(accessKeyID, bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error)
	ListParts(accessKeyID, bucket, name string, uploadID s3.UploadID, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error)
	MultipartParts(accessKeyID, bucket, name string, uploadID s3.UploadID) ([]objects.Part, error)
	UploadStats() (s3.UploadStats, error)

	PutBucketLifecycleConfiguration(accessKeyID, bucket, config string) error
	GetBucketLifecycleConfiguration(accessKeyID, bucket string) (string, error)
	DeleteBucketLifecycleConfiguration(accessKeyID, bucket string) error
	AllBucketLifecycleConfigurations() ([]BucketLifecycleConfiguration, error)
	AbortMultipartUploads(bucket string, prefix string, before time.Time, limit int) ([]AbortedUpload, error)
	ExpireObjects(bucket string, prefix string, before time.Time, limit int) (int, []objects.OrphanedFile, error)

	Backup(ctx context.Context, destPath string) error
	CreateSnapshot() (objects.Snapshot, error)
	MarkSnapshotPinned(id int64, objectID types.Hash256) error
	DeleteSnapshot(id int64) error
	DeleteSnapshotsBySiaObject(objectIDs []types.Hash256) (int64, error)
	DBVersion() int64
}

// New creates a new Sia backend instance.
func New(ctx context.Context, sdk SDK, store Store, directory string, opts ...Option) (*Sia, error) {
	sia := &Sia{
		sdk:   sdk,
		store: store,

		directory:             directory,
		uploadWastePct:        DefaultUploadWastePct,
		lifecycleLoopInterval: defaultLifecycleLoopInterval,
		lifecycleDayDuration:  defaultLifecycleDayDuration,
		diskUsageTimeout:      defaultDiskUsageTimeout,
		lockedUploads:         make(map[string]*lockedUpload),

		logger: zap.NewNop(),
		tg:     threadgroup.New(),
	}
	sia.diskUsageWake = make(chan struct{})
	sia.pinWake = make(chan struct{}, 1)
	for _, opt := range opts {
		opt(sia)
	}
	if sia.uploadWastePct <= 0 {
		return nil, errors.New("upload waste percentage must be greater than 0")
	}

	dir := filepath.Join(sia.directory, UploadsDirectory)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}
	diskUsage, err := sia.store.DiskUsage()
	if err != nil {
		return nil, fmt.Errorf("failed to determine disk usage: %w", err)
	}
	sia.diskUsage = diskUsage

	// initialize optimal upload size
	optimalSize, err := sia.sdk.OptimalDataSize()
	if err != nil {
		return nil, fmt.Errorf("failed to determine optimal upload size: %w", err)
	}
	sia.uploadOptimalSize = optimalSize

	// clean up any orphaned uploads on startup
	deleted, err := sia.deleteOrphanedUploads()
	if err != nil {
		return nil, fmt.Errorf("failed to delete orphaned uploads: %w", err)
	} else if deleted > 0 {
		sia.logger.Info("removed orphaned uploads", zap.Int("removed", deleted))
	}

	// remove snapshot backups left behind by a crash
	tmpFiles, err := filepath.Glob(filepath.Join(sia.directory, "snapshot-*.tmp*"))
	if err != nil {
		return nil, fmt.Errorf("failed to list leftover snapshot backups: %w", err)
	}
	for _, fp := range tmpFiles {
		if err := os.Remove(fp); err != nil {
			return nil, fmt.Errorf("failed to remove leftover snapshot backup %q: %w", fp, err)
		}
	}
	if len(tmpFiles) > 0 {
		sia.logger.Info("removed leftover snapshot backups", zap.Int("removed", len(tmpFiles)))
	}

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

	if err := errors.Join(
		launchBgLoop(sia.processOrphansLoop),
		launchBgLoop(sia.syncMetadataLoop),
		launchBgLoop(sia.uploadLoop),
		launchBgLoop(sia.lifecycleLoop),
		launchBgLoop(sia.pinLoop),
	); err != nil {
		return nil, err
	}

	return sia, nil
}

// Close shuts down the Sia backend and waits for background goroutines.
func (s *Sia) Close() error {
	s.tg.Stop()
	return nil
}

// CreateSnapshot records a snapshot, backs up the database to a temporary
// file, compresses it, uploads it to Sia as a tagged snapshot object, pins it,
// and records the object ID on the snapshot. The temporary files are removed
// once the upload completes. On failure the snapshot and any pinned object are
// rolled back.
func (s *Sia) CreateSnapshot(ctx context.Context) (_ s3.Snapshot, err error) {
	snap, err := s.store.CreateSnapshot()
	if err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to create snapshot: %w", err)
	}

	var pinned types.Hash256
	defer func() {
		if err == nil {
			return
		}
		if pinned != (types.Hash256{}) {
			// the unpin must run even when the failure is a cancelled request
			// context, otherwise the object stays pinned with no local record
			if dErr := s.sdk.DeleteObject(context.WithoutCancel(ctx), pinned); dErr != nil && !isObjectNotFound(dErr) {
				s.logger.Error("failed to unpin snapshot object during rollback", zap.Stringer("objectID", &pinned), zap.Error(dErr))
			}
		}
		if dErr := s.store.DeleteSnapshot(snap.ID); dErr != nil {
			s.logger.Error("failed to roll back snapshot", zap.Int64("snapshotID", snap.ID), zap.Error(dErr))
		}
	}()

	removeFile := func(path string) {
		if rErr := os.Remove(path); rErr != nil && !errors.Is(rErr, os.ErrNotExist) {
			s.logger.Warn("failed to remove snapshot backup file", zap.String("path", path), zap.Error(rErr))
		}
	}

	tmp := filepath.Join(s.directory, "snapshot-"+hex.EncodeToString(frand.Bytes(8))+".tmp")
	if err := s.store.Backup(ctx, tmp); err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to create backup: %w", err)
	}
	defer removeFile(tmp)

	// compress the backup before upload since database files compress well
	// and Sia storage is paid per byte
	tmpGz := tmp + ".gz"
	if err := gzipFile(tmp, tmpGz); err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to compress backup: %w", err)
	}
	defer removeFile(tmpGz)

	meta, err := json.Marshal(objects.SnapshotMetadata{
		Type:        objects.SnapshotType,
		CreatedAt:   snap.CreatedAt,
		DBVersion:   s.store.DBVersion(),
		Encoding:    objects.SnapshotEncodingGzip,
		ObjectCount: snap.ObjectCount,
		S3DVersion:  build.Version(),
	})
	if err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	f, err := os.Open(tmpGz)
	if err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to open snapshot backup: %w", err)
	}
	defer f.Close()

	obj := sdk.NewEmptyObject()
	obj.UpdateMetadata(meta)
	if err := s.sdk.Upload(ctx, &obj, f); err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to upload snapshot: %w", err)
	}
	pinned = obj.ID()

	if err := s.sdk.PinObject(ctx, obj); err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to pin snapshot: %w", err)
	}

	if err := s.store.MarkSnapshotPinned(snap.ID, pinned); err != nil {
		return s3.Snapshot{}, fmt.Errorf("failed to record snapshot object: %w", err)
	}
	return s3.Snapshot{
		ID:          snap.ID,
		CreatedAt:   snap.CreatedAt,
		SiaObjectID: pinned,
		ObjectCount: snap.ObjectCount,
	}, nil
}

// gzipFile compresses the file at src into a new file at dst.
func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination: %w", err)
	}
	defer out.Close()

	gw := gzip.NewWriter(out)
	if _, err := io.Copy(gw, in); err != nil {
		return fmt.Errorf("failed to compress: %w", err)
	} else if err := gw.Close(); err != nil {
		return fmt.Errorf("failed to flush compressed data: %w", err)
	}
	return out.Close()
}

// isObjectNotFound reports whether err indicates the object does not exist on
// the indexer. The SDK transports errors over HTTP, so the sentinel is matched
// by string.
func isObjectNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), slabs.ErrObjectNotFound.Error())
}

// processOrphansLoop periodically processes orphaned objects.
func (s *Sia) processOrphansLoop(ctx context.Context) {
	t := time.NewTicker(orphanLoopInterval)
	defer t.Stop()

	for {
		s.ProcessOrphans(ctx)
		if ctx.Err() != nil {
			return
		}

		s.logger.Info("pruning orphaned slabs")
		start := time.Now()
		if err := s.sdk.PruneSlabs(ctx, api.WithBefore(time.Now().Add(-time.Hour))); err != nil {
			s.logger.Error("failed to prune slabs after processing orphans", zap.Error(err))
		} else {
			s.logger.Info("finished pruning orphaned slabs from Sia network", zap.Duration("elapsed", time.Since(start)))
		}

		select {
		case <-ctx.Done():
			return
		case <-t.C:
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

			if err := s.sdk.DeleteObject(ctx, id); err != nil && !isObjectNotFound(err) {
				s.logger.Error("failed to unpin object from indexer", zap.Error(err), zap.Stringer("objectID", &id))
				return
			}
			if err := s.store.RemoveOrphanedObject(id); err != nil {
				s.logger.Error("failed to remove orphaned object", zap.Error(err), zap.Stringer("objectID", &id))
				return
			}
			s.logger.Debug("processed orphaned object", zap.Stringer("objectID", &id))
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

// UserInfo returns user information for the given access key ID.
func (s *Sia) UserInfo(_ context.Context, accessKeyID string) (*s3.UserInfo, error) {
	name, err := s.store.UserNameForAccessKey(accessKeyID)
	if err != nil {
		return nil, err
	}
	return &s3.UserInfo{
		ID:          name,
		DisplayName: name,
	}, nil
}

// LoadSecret loads the secret key for the given access key ID.
func (s *Sia) LoadSecret(_ context.Context, accessKeyID string) (auth.SecretAccessKey, error) {
	secret, err := s.store.LoadSecret(accessKeyID)
	if err != nil {
		return nil, err
	}
	return auth.SecretAccessKey(secret), nil
}

func (s *Sia) deleteOrphanedUploads() (int, error) { //nolint:revive
	// fetch all files on disk
	entries, err := os.ReadDir(s.uploadDir())
	if err != nil {
		s.logger.Error("failed to read uploads directory", zap.Error(err))
		return 0, err
	}

	// fetch all filenames from store
	filenames, err := s.store.AllFilenames()
	if err != nil {
		s.logger.Error("failed to fetch filenames from store", zap.Error(err))
		return 0, err
	}

	// build lookup table
	lookup := make(map[string]struct{})
	for _, filename := range filenames {
		lookup[filename] = struct{}{}
	}

	// remove unreferenced files
	var removed int
	for _, entry := range entries {
		if _, ok := lookup[entry.Name()]; !ok {
			path := filepath.Join(s.uploadDir(), entry.Name())
			s.logger.Warn("removing orphaned upload", zap.String("path", path))
			if err := s.removeUpload(path); err != nil {
				s.logger.Error("failed to remove orphaned upload", zap.String("path", path), zap.Error(err))
				continue
			}
			removed++
		}
	}
	return removed, nil
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
func (s *Sia) syncMetadata(ctx context.Context) { //nolint:revive
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

		var batch []objects.SiaObject
		var deletedIDs []types.Hash256
		for _, ev := range events {
			if ev.Deleted {
				deletedIDs = append(deletedIDs, ev.Key)
				continue
			} else if ev.Object == nil {
				s.logger.Warn("skipping event with nil object", zap.Stringer("objectID", &ev.Key))
				continue
			}

			sealed := s.sdk.SealObject(*ev.Object)
			batch = append(batch, objects.SiaObject{ID: sealed.ID(), Sealed: sealed})
		}

		if len(batch) > 0 {
			n, err := s.store.UpdateSiaObjects(batch)
			if err != nil {
				s.logger.Error("failed to batch update Sia objects", zap.Error(err))
				break
			}
			synced += int(n)
		}

		// drop snapshots whose backup object was deleted on the indexer so
		// they stop withholding orphans
		if len(deletedIDs) > 0 {
			n, err := s.store.DeleteSnapshotsBySiaObject(deletedIDs)
			if err != nil {
				s.logger.Error("failed to delete snapshots for deleted objects", zap.Error(err))
				break
			} else if n > 0 {
				s.logger.Info("deleted snapshots for deleted objects", zap.Int64("deleted", n))
			}
		}

		// advance the cursor to the last event
		last := events[len(events)-1]
		cursor = slabs.Cursor{After: last.UpdatedAt, Key: last.Key}
		if err := s.store.SetObjectsCursor(cursor); err != nil {
			s.logger.Error("failed to update objects cursor", zap.Error(err))
			break
		}
	}

	if synced > 0 {
		s.logger.Info("synced object metadata", zap.Int("synced", synced))
	}
}
