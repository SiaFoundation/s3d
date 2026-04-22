package sia

import "context"

// PackObjects runs a single packing cycle for testing.
func (s *Sia) PackObjects(ctx context.Context) {
	s.packObjectsIter(ctx)
}
