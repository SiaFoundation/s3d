package sia

import (
	"context"
	"time"
)

// OrphanLoopInterval exports orphanLoopInterval for testing.
const OrphanLoopInterval = orphanLoopInterval

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
	s.uploadObjects(ctx)
}

// PinObjects runs a single pin cycle for testing.
func (s *Sia) PinObjects(ctx context.Context) error { //nolint:revive
	return s.performObjectPinning(ctx)
}

// DeleteOrphanedUploads exports deleteOrphanedUploads for testing.
func (s *Sia) DeleteOrphanedUploads() (int, error) { //nolint:revive
	return s.deleteOrphanedUploads()
}
