package sia

import (
	"context"

	"github.com/SiaFoundation/s3d/s3"
)

// CreateBucket creates a new bucket with the given name for the user
// identified by the given access key.
func (s *Sia) CreateBucket(ctx context.Context, accessKeyID, name string) error {
	return s.store.CreateBucket(accessKeyID, name)
}

// DeleteBucket deletes the bucket with the given name for the user
// identified by the given access key.
func (s *Sia) DeleteBucket(ctx context.Context, accessKeyID, name string) error {
	return s.store.DeleteBucket(accessKeyID, name)
}

// HeadBucket checks if the bucket with the given name exists and is
// accessible for the user identified by the given access key.
func (s *Sia) HeadBucket(ctx context.Context, accessKeyID, name string) error {
	return s.store.HeadBucket(accessKeyID, name)
}

// ListBuckets lists all available buckets for the user identified by the
// given access key.
func (s *Sia) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	return s.store.ListBuckets(accessKeyID)
}

// PutBucketVersioning sets the versioning state of the bucket.
func (s *Sia) PutBucketVersioning(ctx context.Context, accessKeyID, bucket, status string) error {
	return s.store.PutBucketVersioning(accessKeyID, bucket, status)
}

// GetBucketVersioning returns the versioning state of the bucket.
func (s *Sia) GetBucketVersioning(ctx context.Context, accessKeyID, bucket string) (string, error) {
	return s.store.GetBucketVersioning(accessKeyID, bucket)
}
