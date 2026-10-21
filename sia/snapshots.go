package sia

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

// remoteSnapshotBatchSize is the number of object events fetched per request
// while enumerating the account.
const remoteSnapshotBatchSize = api.MaxLimit

// RemoteSnapshot is a snapshot stored on the Sia network.
type RemoteSnapshot struct {
	ObjectID types.Hash256
	Metadata objects.SnapshotMetadata

	object sdk.Object
}

// RestoreCleanup reports what the first start after a restore removed and which lost objects it kept.
type RestoreCleanup struct {
	Removed  []objects.ObjectForUpload
	Retained []objects.ObjectForUpload
	Aborted  []s3.UploadID
	Parts    []LostPart
}

// LostPart identifies a multipart part whose file was not found on disk after a restore.
type LostPart struct {
	UploadID   s3.UploadID
	PartNumber int
}

func (s *Sia) completeRestore() error {
	if restored, err := s.store.Restored(); err != nil {
		return fmt.Errorf("failed to check for a restore: %w", err)
	} else if !restored {
		return nil
	}

	onDisk := make(map[string]struct{})
	err := filepath.WalkDir(s.uploadDir(), func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.uploadDir(), path)
		if err != nil {
			return err
		}
		onDisk[filepath.ToSlash(rel)] = struct{}{}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk uploads directory: %w", err)
	}

	cleanup, err := s.store.CompleteRestore(onDisk, time.Now().Add(pinDeadline))
	if err != nil {
		return fmt.Errorf("failed to remove lost uploads: %w", err)
	}
	for _, obj := range cleanup.Removed {
		s.logger.Warn("removed object whose data was lost in the restore",
			zap.String("bucket", obj.Bucket),
			zap.String("name", obj.Name),
			zap.String("versionID", s3.FormatVersion(obj.VersionID)))
	}
	for _, obj := range cleanup.Retained {
		s.logger.Warn("kept object whose data was lost in the restore, its lock forbids deletion",
			zap.String("bucket", obj.Bucket),
			zap.String("name", obj.Name),
			zap.String("versionID", s3.FormatVersion(obj.VersionID)))
	}
	for _, id := range cleanup.Aborted {
		s.logger.Warn("aborted multipart upload whose parts were lost in the restore", zap.Stringer("uploadID", id))
	}
	for _, part := range cleanup.Parts {
		s.logger.Warn("removed multipart part whose file was lost in the restore",
			zap.Stringer("uploadID", part.UploadID),
			zap.Int("partNumber", part.PartNumber))
	}
	s.logger.Info("completed restore",
		zap.Int("removedObjects", len(cleanup.Removed)),
		zap.Int("retainedObjects", len(cleanup.Retained)),
		zap.Int("abortedUploads", len(cleanup.Aborted)),
		zap.Int("removedParts", len(cleanup.Parts)))
	return nil
}

// ListRemoteSnapshots enumerates every object in the account and returns those
// tagged as valid snapshots, newest first.
func ListRemoteSnapshots(ctx context.Context, sdk SDK) ([]RemoteSnapshot, error) {
	found := make(map[types.Hash256]RemoteSnapshot)

	var cursor slabs.Cursor
	for {
		events, err := sdk.ObjectEvents(ctx, cursor, remoteSnapshotBatchSize)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch object events: %w", err)
		} else if len(events) == 0 {
			break
		}

		for _, ev := range events {
			// an object can appear more than once with a later timestamp, so
			// the newest event for a key wins
			if ev.Deleted {
				delete(found, ev.Key)
				continue
			} else if ev.Object == nil {
				continue
			}

			meta, ok := snapshotMetadata(ev.Object)
			if !ok || meta.Validate() != nil {
				continue
			}
			found[ev.Key] = RemoteSnapshot{
				ObjectID: ev.Key,
				Metadata: meta,
				object:   *ev.Object,
			}
		}

		last := events[len(events)-1]
		cursor = slabs.Cursor{After: last.UpdatedAt, Key: last.Key}
	}

	snapshots := make([]RemoteSnapshot, 0, len(found))
	for _, snap := range found {
		snapshots = append(snapshots, snap)
	}
	slices.SortFunc(snapshots, func(a, b RemoteSnapshot) int {
		if !a.Metadata.CreatedAt.Equal(b.Metadata.CreatedAt) {
			return b.Metadata.CreatedAt.Compare(a.Metadata.CreatedAt)
		} else if c := cmp.Compare(b.Metadata.Generation, a.Metadata.Generation); c != 0 {
			return c
		}
		return bytes.Compare(a.ObjectID[:], b.ObjectID[:])
	})
	return snapshots, nil
}

// FetchRemoteSnapshot retrieves a single snapshot by its Sia object ID without
// enumerating the account.
func FetchRemoteSnapshot(ctx context.Context, sdk SDK, objectID types.Hash256) (RemoteSnapshot, error) {
	obj, err := sdk.Object(ctx, objectID)
	if err != nil {
		return RemoteSnapshot{}, fmt.Errorf("failed to fetch object: %w", err)
	}

	meta, ok := snapshotMetadata(&obj)
	if !ok {
		return RemoteSnapshot{}, fmt.Errorf("object %v is not a snapshot", objectID)
	} else if err := meta.Validate(); err != nil {
		return RemoteSnapshot{}, fmt.Errorf("object %v has invalid snapshot metadata: %w", objectID, err)
	}
	return RemoteSnapshot{ObjectID: objectID, Metadata: meta, object: obj}, nil
}

// DownloadSnapshot downloads a snapshot's Sia object and writes the
// decompressed database image to w. Cancelling ctx aborts the download.
func DownloadSnapshot(ctx context.Context, sdk SDK, snap RemoteSnapshot, w io.Writer) error {
	if snap.Metadata.Encoding != objects.SnapshotEncodingGzip {
		return fmt.Errorf("unsupported snapshot encoding %q", snap.Metadata.Encoding)
	}

	r, err := sdk.Download(snap.object, nil)
	if err != nil {
		return fmt.Errorf("failed to download snapshot object: %w", err)
	}
	defer r.Close()

	// the SDK reader takes no context, so closing it is what unblocks a read
	// already in flight
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			r.Close()
		case <-done:
		}
	}()

	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to decompress snapshot: %w", err)
	}
	defer gz.Close()

	if _, err := io.Copy(w, gz); err != nil {
		return errors.Join(fmt.Errorf("failed to write snapshot image: %w", err), ctx.Err())
	}
	return ctx.Err()
}
