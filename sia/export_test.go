package sia

import "context"

// SyncMetadata exports syncMetadataIter for testing.
func (s *Sia) SyncMetadata(ctx context.Context) {
	s.syncMetadataIter(ctx)
}
