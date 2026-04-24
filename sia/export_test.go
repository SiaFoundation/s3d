package sia

import "context"

// SyncMetadata exports syncMetadata for testing.
//
//nolint:revive // test export intentionally shadows unexported method
func (s *Sia) SyncMetadata(ctx context.Context) {
	s.syncMetadata(ctx)
}
