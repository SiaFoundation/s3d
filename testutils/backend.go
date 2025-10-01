package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

// MemoryBackend is an in-memory implementation of the s3 backend for testing.
type MemoryBackend struct {
	buckets map[string]struct{}
}

// NewMemoryBackend creates a new MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		buckets: make(map[string]struct{}),
	}
}

// CreateBucket creates a new bucket if it doesn't exist yet and returns an
// error otherwise.
func (b *MemoryBackend) CreateBucket(ctx context.Context, name string) error {
	if _, exists := b.buckets[name]; exists {
		return fmt.Errorf("bucket %q already exists", name)
	}
	b.buckets[name] = struct{}{}
	return nil
}

// ListBuckets lists all available buckets.
func (b *MemoryBackend) ListBuckets(ctx context.Context) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	for name := range b.buckets {
		buckets = append(buckets, s3.BucketInfo{
			Name:         name,
			CreationDate: time.Now().UTC(),
		})
	}
	return buckets, nil
}
