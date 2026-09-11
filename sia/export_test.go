package sia

import (
	"context"
	"time"
)

// PruneSlabsInterval exports pruneSlabsInterval for testing.
const PruneSlabsInterval = pruneSlabsInterval

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
func (s *Sia) ProcessSnapshotDeletions(ctx context.Context) { //nolint:revive
	s.processSnapshotDeletions(ctx)
}

// SetSnapshotConfirmDelay overrides the snapshot deletion confirmation delay
// for testing.
func (s *Sia) SetSnapshotConfirmDelay(d time.Duration) { //nolint:revive
	s.snapshotConfirmDelay.Store(int64(d))
}

// SetSnapshotObserveTimeout overrides how long CreateSnapshot waits for the
// sync loop to observe the pin for testing.
func (s *Sia) SetSnapshotObserveTimeout(d time.Duration) { //nolint:revive
	s.snapshotObserveTimeout.Store(int64(d))
}
