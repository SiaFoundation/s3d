package testutils

import (
	"context"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

// MemoryBackend is an in-memory implementation of the s3 backend for testing.
type MemoryBackend struct {
	buckets    map[string]struct{}
	accessKeys map[string]string
}

// NewMemoryBackend creates a new MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		accessKeys: make(map[string]string),
		buckets:    make(map[string]struct{}),
	}
}

// AddAccessKey adds a new access key to the backend for authentication.
func (b *MemoryBackend) AddAccessKey(ctx context.Context, accessKeyID, secretAccessKey string) error {
	if _, exists := b.accessKeys[accessKeyID]; exists {
		return errors.New("access key already exists")
	}
	b.accessKeys[accessKeyID] = secretAccessKey
	return nil
}

// CreateBucket creates a new bucket if it doesn't exist yet and returns an
// error otherwise.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (b *MemoryBackend) CreateBucket(ctx context.Context, name string) error {
	if _, exists := b.buckets[name]; exists {
		// NOTE: Since we don't have multi-user support, all buckets are always
		// owned by the caller. If that changes, we need to extend the check and
		// return ErrBucketExists instead.
		return s3.ErrBucketAlreadyOwnedByYou
	}
	b.buckets[name] = struct{}{}
	return nil
}

// ListBuckets lists all available buckets.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
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
