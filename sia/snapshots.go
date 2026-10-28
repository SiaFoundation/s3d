package sia

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

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

// snapshotSchedule is the local time of day at which a snapshot is taken and
// the number of scheduled snapshots kept, zero keeps all of them.
type snapshotSchedule struct {
	hour   int
	minute int
	keep   int
}

// RemoteSnapshot is a snapshot stored on the Sia network.
type RemoteSnapshot struct {
	ObjectID types.Hash256
	Metadata objects.SnapshotMetadata

	object *sdk.Object
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
				delete(found, ev.ID)
				continue
			} else if ev.Object == nil {
				continue
			}

			meta, ok := snapshotMetadata(ev.Object)
			if !ok || meta.Validate() != nil {
				continue
			}
			found[ev.ID] = RemoteSnapshot{
				ObjectID: ev.ID,
				Metadata: meta,
				object:   ev.Object,
			}
		}

		last := events[len(events)-1]
		cursor = slabs.Cursor{After: last.UpdatedAt, Key: last.ID}
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

	meta, ok := snapshotMetadata(obj)
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

	r, err := sdk.Download(ctx, snap.object, nil)
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

// dueAt returns the most recent instant at the scheduled time of day at or
// before now.
func (sched snapshotSchedule) dueAt(now time.Time) time.Time {
	due := time.Date(now.Year(), now.Month(), now.Day(), sched.hour, sched.minute, 0, 0, now.Location())
	if due.After(now) {
		// a time of day inside a daylight saving gap normalizes to a later
		// clock, which AddDate would carry into the previous day
		due = time.Date(now.Year(), now.Month(), now.Day()-1, sched.hour, sched.minute, 0, 0, now.Location())
	}
	return due
}

// snapshotsLoop takes snapshots on the configured schedule.
func (s *Sia) snapshotsLoop(ctx context.Context) {
	if s.snapshotSchedule == nil {
		return
	}
	sched := *s.snapshotSchedule
	t := time.NewTicker(scheduledSnapshotInterval)
	defer t.Stop()

	for {
		s.runScheduledSnapshot(ctx, time.Now(), sched)

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runScheduledSnapshot takes a snapshot when none was taken since the
// schedule's last slot and applies the retention count.
func (s *Sia) runScheduledSnapshot(ctx context.Context, now time.Time, sched snapshotSchedule) {
	if !now.Before(s.snapshotRetryAt) {
		s.takeScheduledSnapshot(ctx, now, sched)
	}
	s.pruneScheduledSnapshots(ctx, sched.keep)
}

// takeScheduledSnapshot takes a snapshot when none was taken since the
// schedule's last slot.
func (s *Sia) takeScheduledSnapshot(ctx context.Context, now time.Time, sched snapshotSchedule) {
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		s.logger.Error("failed to list snapshots", zap.Error(err))
		return
	}
	// the listing is newest first
	var newest time.Time
	for _, snap := range snapshots {
		if snap.Scheduled {
			newest = snap.CreatedAt
			break
		}
	}
	if !newest.Before(sched.dueAt(now)) {
		return
	}

	if _, err := s.createSnapshot(ctx, true); err != nil {
		s.snapshotRetryAt = now.Add(scheduledSnapshotRetryDelay)
		s.logger.Error("failed to take scheduled snapshot", zap.Error(err))
	}
}

// pruneScheduledSnapshots deletes every scheduled snapshot past the newest
// keep once object metadata is synced.
func (s *Sia) pruneScheduledSnapshots(ctx context.Context, keep int) {
	if keep == 0 {
		return
	} else if !s.synced.Load() {
		s.logger.Debug("deferring snapshot retention until object metadata is synced")
		return
	}
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		s.logger.Error("failed to list snapshots", zap.Error(err))
		return
	}
	var kept int
	for _, snap := range snapshots {
		if ctx.Err() != nil {
			return
		} else if !snap.Scheduled {
			continue
		} else if kept++; kept <= keep {
			continue
		}
		if err := s.DeleteSnapshot(ctx, snap.SiaObjectID); err != nil {
			s.logger.Error("failed to prune scheduled snapshot", zap.Stringer("objectID", &snap.SiaObjectID), zap.Error(err))
		}
	}
}
