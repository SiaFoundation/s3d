package sia

import "context"

// OrphanLoopInterval exports orphanLoopInterval for testing.
const OrphanLoopInterval = orphanLoopInterval

// SyncMetadata exports syncMetadata for testing.
func (s *Sia) SyncMetadata(ctx context.Context) { //nolint:revive
	s.syncMetadata(ctx)
}

// UploadObjects runs a single upload cycle for testing.
func (s *Sia) UploadObjects(ctx context.Context) { //nolint:revive
	s.uploadObjects(ctx, false)
}

// DeleteOrphanedUploads exports deleteOrphanedUploads for testing.
func (s *Sia) DeleteOrphanedUploads() (int, error) { //nolint:revive
	return s.deleteOrphanedUploads()
}
