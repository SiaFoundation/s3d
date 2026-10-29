package sia

import (
	"context"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

// PinThreads exports pinThreads for testing.
const PinThreads = pinThreads

// PruneSlabsInterval exports pruneSlabsInterval for testing.
const PruneSlabsInterval = pruneSlabsInterval

// SnapshotConfirmDelay exports snapshotConfirmDelay for testing.
const SnapshotConfirmDelay = snapshotConfirmDelay

// ScheduledSnapshotRetryDelay exports scheduledSnapshotRetryDelay for testing.
const ScheduledSnapshotRetryDelay = scheduledSnapshotRetryDelay

// SetDiskUsageTimeout overrides the disk usage timeout for testing.
func (s *Sia) SetDiskUsageTimeout(d time.Duration) { //nolint:revive
	s.diskUsageTimeout = d
}

// ApplyLifecycleRules exports applyLifecycleRules for testing.
func (s *Sia) ApplyLifecycleRules(ctx context.Context, now time.Time) { //nolint:revive
	s.applyLifecycleRules(ctx, now)
}

// SyncMetadata exports syncMetadata for testing.
func (s *Sia) SyncMetadata(ctx context.Context) { //nolint:revive
	s.syncMetadata(ctx)
}

// UploadObjects runs a single upload cycle for testing.
func (s *Sia) UploadObjects(ctx context.Context) { //nolint:revive
	s.uploadObjects(ctx, false)
}

// PinObjects runs a single pin cycle for testing.
func (s *Sia) PinObjects(ctx context.Context) error { //nolint:revive
	return s.performObjectPinning(ctx)
}

// DeleteOrphanedUploads exports deleteOrphanedUploads for testing.
func (s *Sia) DeleteOrphanedUploads() (int, error) { //nolint:revive
	return s.deleteOrphanedUploads()
}

// ProcessSnapshotDeletions exports processSnapshotDeletions for testing.
func (s *Sia) ProcessSnapshotDeletions(ctx context.Context, now time.Time) { //nolint:revive
	s.processSnapshotDeletions(ctx, now)
}

// CreateScheduledSnapshot exports createSnapshot with scheduled set for testing.
func (s *Sia) CreateScheduledSnapshot(ctx context.Context) (s3.Snapshot, error) { //nolint:revive
	return s.createSnapshot(ctx, true)
}

// RunScheduledSnapshot exports runScheduledSnapshot for testing.
func (s *Sia) RunScheduledSnapshot(ctx context.Context, now time.Time) { //nolint:revive
	s.runScheduledSnapshot(ctx, now)
}

// ExpiredScheduledSnapshots exports expiredScheduledSnapshots for testing.
func ExpiredScheduledSnapshots(snapshots []s3.Snapshot, sched SnapshotSchedule, loc *time.Location) []s3.Snapshot {
	return expiredScheduledSnapshots(snapshots, sched, loc)
}
