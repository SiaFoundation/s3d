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
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/robfig/cron/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

// remoteSnapshotBatchSize is the number of object events fetched per request
// while enumerating the account.
const remoteSnapshotBatchSize = api.MaxLimit

// SnapshotRetention is the number of calendar days, weeks and months of
// scheduled snapshots kept. All zero keeps every scheduled snapshot.
type SnapshotRetention struct {
	KeepDaily   int
	KeepWeekly  int
	KeepMonthly int
}

// Validate rejects a negative count.
func (r SnapshotRetention) Validate() error {
	if r.KeepDaily < 0 || r.KeepWeekly < 0 || r.KeepMonthly < 0 {
		return errors.New("snapshot retention counts must not be negative")
	}
	return nil
}

// ParseSnapshotSchedule parses a standard five field cron expression or one of
// the @hourly, @daily, @weekly, @monthly and @yearly descriptors. The schedule
// fires in the location of the time it is evaluated against.
func ParseSnapshotSchedule(expr string) (cron.Schedule, error) {
	// the parser panics on a bare time zone prefix and accepts a zero @every
	// interval, which would fire on every tick
	if expr = strings.TrimSpace(expr); strings.Contains(expr, "=") || strings.HasPrefix(expr, "@every") {
		return nil, fmt.Errorf("invalid snapshot schedule %q, expected a cron expression or a descriptor like @daily", expr)
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot schedule %q: %w", expr, err)
	} else if sched.Next(time.Now()).IsZero() {
		return nil, fmt.Errorf("invalid snapshot schedule %q, it never fires", expr)
	}
	return sched, nil
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

// snapshotsLoop takes snapshots on the stored schedule and applies its
// retention.
func (s *Sia) snapshotsLoop(ctx context.Context) {
	t := time.NewTicker(scheduledSnapshotInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		s.runScheduledSnapshot(ctx, time.Now())
	}
}

// runScheduledSnapshot takes a snapshot when the stored schedule fired since
// the last one and prunes the scheduled snapshots retention no longer keeps,
// once object metadata is synced.
func (s *Sia) runScheduledSnapshot(ctx context.Context, now time.Time) {
	expr, retention, err := s.store.SnapshotSchedule()
	if err != nil {
		s.logger.Error("failed to load snapshot schedule", zap.Error(err))
		return
	} else if expr == "" {
		return
	}
	sched, err := ParseSnapshotSchedule(expr)
	if err != nil {
		s.logger.Error("failed to parse snapshot schedule", zap.Error(err))
		return
	}
	if !s.synced.Load() {
		s.logger.Debug("deferring scheduled snapshots until object metadata is synced")
		return
	}
	if !now.Before(s.snapshotRetryAt) {
		s.takeScheduledSnapshot(ctx, now, sched)
	}
	s.pruneScheduledSnapshots(ctx, now, retention)
}

// takeScheduledSnapshot takes a snapshot when the schedule fired since the
// newest scheduled snapshot was taken.
func (s *Sia) takeScheduledSnapshot(ctx context.Context, now time.Time, sched cron.Schedule) {
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		s.logger.Error("failed to list snapshots", zap.Error(err))
		return
	}
	// the listing is newest first
	for _, snap := range snapshots {
		if !snap.Scheduled {
			continue
		} else if next := sched.Next(snap.CreatedAt.In(now.Location())); next.IsZero() || next.After(now) {
			return
		}
		break
	}

	if _, err := s.createSnapshot(ctx, true); err != nil {
		s.snapshotRetryAt = now.Add(scheduledSnapshotRetryDelay)
		s.logger.Error("failed to take scheduled snapshot", zap.Error(err))
	}
}

// pruneScheduledSnapshots deletes the scheduled snapshots the retention counts
// do not keep.
func (s *Sia) pruneScheduledSnapshots(ctx context.Context, now time.Time, retention SnapshotRetention) {
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		s.logger.Error("failed to list snapshots", zap.Error(err))
		return
	}
	for _, snap := range expiredScheduledSnapshots(snapshots, retention, now.Location()) {
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
func expiredScheduledSnapshots(snapshots []s3.Snapshot, retention SnapshotRetention, loc *time.Location) []s3.Snapshot {
	if retention == (SnapshotRetention{}) {
		return nil
	}
	periods := []struct {
		keep int
		key  func(time.Time) [3]int
	}{
		{retention.KeepDaily, func(t time.Time) [3]int { return [3]int{t.Year(), int(t.Month()), t.Day()} }},
		{retention.KeepWeekly, func(t time.Time) [3]int { y, w := t.ISOWeek(); return [3]int{y, w, 0} }},
		{retention.KeepMonthly, func(t time.Time) [3]int { return [3]int{t.Year(), int(t.Month()), 0} }},
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
