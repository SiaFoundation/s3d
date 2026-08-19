package sia

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
)

// remoteSnapshotBatchSize is the number of object events fetched per request
// while enumerating the account. Recovery has to read every object in the
// account to find the snapshot tag, since that tag is inside client encrypted
// metadata the indexer cannot filter on, so this bounds the number of round
// trips. It is the indexer's maximum accepted limit: a larger value is rejected
// with a 400 rather than clamped.
const remoteSnapshotBatchSize = api.MaxLimit

// RemoteSnapshot is a snapshot backup object stored on the Sia network.
type RemoteSnapshot struct {
	ObjectID types.Hash256
	Metadata objects.SnapshotMetadata

	object sdk.Object
}

// ListRemoteSnapshots enumerates every object in the account and returns those
// tagged as valid snapshot backups, oldest first. It needs only the app key, so
// it is the recovery path when the local database is gone.
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
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].Metadata.CreatedAt.Equal(snapshots[j].Metadata.CreatedAt) {
			return snapshots[i].Metadata.Generation < snapshots[j].Metadata.Generation
		}
		return snapshots[i].Metadata.CreatedAt.Before(snapshots[j].Metadata.CreatedAt)
	})
	return snapshots, nil
}

// FetchRemoteSnapshot retrieves a single snapshot by its Sia object ID without
// enumerating the account. Enumeration has to read and decrypt every object to
// find the snapshot tag, so its cost grows with the account; this does not. The
// object ID is returned when a snapshot is created, which makes it the token to
// keep for recovery.
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

// DownloadSnapshot downloads a snapshot's backup object and writes the
// decompressed database image to w.
func DownloadSnapshot(sdk SDK, snap RemoteSnapshot, w io.Writer) error {
	if snap.Metadata.Encoding != objects.SnapshotEncodingGzip {
		return fmt.Errorf("unsupported snapshot encoding %q", snap.Metadata.Encoding)
	}

	r, err := sdk.Download(snap.object, nil)
	if err != nil {
		return fmt.Errorf("failed to download snapshot object: %w", err)
	}
	defer r.Close()

	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to decompress snapshot: %w", err)
	}
	defer gz.Close()

	if _, err := io.Copy(w, gz); err != nil {
		return fmt.Errorf("failed to write snapshot image: %w", err)
	}
	return nil
}
