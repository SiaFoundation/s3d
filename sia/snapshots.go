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

// SnapshotSchedule returns the snapshot schedule.
func (s *Sia) SnapshotSchedule(_ context.Context) (s3.SnapshotSchedule, error) {
	return s.store.SnapshotSchedule()
}

// UpdateSnapshotSchedule replaces the snapshot schedule and reschedules the
// next snapshot.
func (s *Sia) UpdateSnapshotSchedule(_ context.Context, sched s3.SnapshotSchedule) error {
	if err := s.store.UpdateSnapshotSchedule(sched); err != nil {
		return err
	}
	s.wakeSnapshotLoop()
	return nil
}

// wakeSnapshotLoop signals the snapshot loop to reread the schedule without
// waiting for its timer.
func (s *Sia) wakeSnapshotLoop() {
	select {
	case s.snapshotWake <- struct{}{}:
	default:
	}
}

// snapshotsLoop takes snapshots on the stored schedule and applies its
// retention.
func (s *Sia) snapshotsLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	pruneTicker := time.NewTicker(scheduledSnapshotPruneInterval)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pruneTicker.C:
			s.pruneScheduledSnapshots(ctx, time.Now())
			continue
		case <-timer.C:
		case <-s.snapshotWake:
		}
		if next := s.runScheduledSnapshot(ctx, time.Now()); next.IsZero() {
			timer.Stop()
		} else {
			timer.Reset(time.Until(next))
		}
	}
}

// runScheduledSnapshot takes a snapshot when the schedule is due and returns
// when it should run next, zero until the loop is woken.
func (s *Sia) runScheduledSnapshot(ctx context.Context, now time.Time) time.Time {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	sched, err := s.store.SnapshotSchedule()
	if err != nil {
		s.logger.Error("failed to load snapshot schedule", zap.Error(err))
		return now.Add(scheduledSnapshotRetryDelay)
	} else if sched.Expression == "" {
		return time.Time{}
	}
	cronSched, err := s3.ParseSnapshotSchedule(sched.Expression)
	if err != nil {
		s.logger.Error("failed to parse snapshot schedule", zap.Error(err))
		return now.Add(scheduledSnapshotRetryDelay)
	}
	if !s.synced.Load() {
		return time.Time{}
	}
	// the backoff was recorded against the schedule that failed, one the
	// operator has changed since is attempted right away
	if sched.Expression != s.snapshotRetrySched.Expression || !sched.Since.Equal(s.snapshotRetrySched.Since) {
		s.snapshotRetryAt = time.Time{}
	}
	if next := cronSched.Next(sched.Since.In(now.Location())); next.IsZero() {
		return time.Time{}
	} else if s.snapshotRetryAt.After(now) {
		return s.snapshotRetryAt
	} else if next.After(now) {
		return next
	}

	_, err = s.createSnapshot(ctx, true)
	if err != nil {
		s.logger.Error("failed to take scheduled snapshot", zap.Error(err))
	} else if err = s.store.UpdateSnapshotScheduleSince(sched, now); err != nil {
		s.logger.Error("failed to advance snapshot schedule", zap.Error(err))
	}
	if err != nil {
		s.snapshotRetryAt = now.Add(scheduledSnapshotRetryDelay)
		s.snapshotRetrySched = sched
		return s.snapshotRetryAt
	}
	return cronSched.Next(now)
}

// pruneScheduledSnapshots deletes the scheduled snapshots the retention counts
// do not keep, once object metadata is synced.
func (s *Sia) pruneScheduledSnapshots(ctx context.Context, now time.Time) {
	if !s.synced.Load() {
		s.logger.Debug("deferring scheduled snapshot pruning until object metadata is synced")
		return
	}
	sched, err := s.store.SnapshotSchedule()
	if err != nil {
		s.logger.Error("failed to load snapshot schedule", zap.Error(err))
		return
	} else if sched.Expression == "" {
		return
	}
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		s.logger.Error("failed to list snapshots", zap.Error(err))
		return
	}
	for _, snap := range expiredScheduledSnapshots(snapshots, sched, now.Location()) {
		if ctx.Err() != nil {
			return
		} else if err := s.DeleteSnapshot(ctx, snap.SiaObjectID); err != nil {
			s.logger.Error("failed to prune scheduled snapshot", zap.Stringer("objectID", &snap.SiaObjectID), zap.Error(err))
		}
	}
}

// expiredScheduledSnapshots returns the scheduled snapshots, listed newest
// first, that the retention counts do not keep. Each count keeps the newest
// snapshot of that many calendar days, ISO weeks or months in loc, all zero
// counts keep every scheduled snapshot.
func expiredScheduledSnapshots(snapshots []s3.Snapshot, sched s3.SnapshotSchedule, loc *time.Location) []s3.Snapshot {
	if sched.KeepDaily == 0 && sched.KeepWeekly == 0 && sched.KeepMonthly == 0 {
		return nil
	}
	periods := []struct {
		keep int
		key  func(time.Time) [3]int
	}{
		{sched.KeepDaily, func(t time.Time) [3]int { return [3]int{t.Year(), int(t.Month()), t.Day()} }},
		{sched.KeepWeekly, func(t time.Time) [3]int { y, w := t.ISOWeek(); return [3]int{y, w, 0} }},
		{sched.KeepMonthly, func(t time.Time) [3]int { return [3]int{t.Year(), int(t.Month()), 0} }},
	}
	kept := make(map[types.Hash256]bool)
	for _, p := range periods {
		var last [3]int
		var n int
		for _, snap := range snapshots {
			if !snap.Scheduled {
				continue
			}
			key := p.key(snap.CreatedAt.In(loc))
			if n > 0 && key == last {
				continue
			} else if n == p.keep {
				break
			}
			kept[snap.SiaObjectID] = true
			last, n = key, n+1
		}
	}

	var expired []s3.Snapshot
	for _, snap := range snapshots {
		if snap.Scheduled && !kept[snap.SiaObjectID] {
			expired = append(expired, snap)
		}
	}
	return expired
}
