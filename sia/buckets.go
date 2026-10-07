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

// DeleteBucket deletes the bucket with the given name.
func (s *Sia) DeleteBucket(ctx context.Context, name string) error {
	return s.store.DeleteBucket(name)
}

// ListBuckets lists all available buckets for the user identified by the
// given access key.
func (s *Sia) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	return s.store.ListBuckets(accessKeyID)
}

// BucketAccessInfo returns the bucket owner and stored policy document.
func (s *Sia) BucketAccessInfo(ctx context.Context, bucket string) (s3.BucketAccessInfo, error) {
	return s.store.BucketAccessInfo(bucket)
}

// PutBucketPolicy sets the policy of the bucket, replacing any existing one.
func (s *Sia) PutBucketPolicy(ctx context.Context, bucket, document string) error {
	return s.store.PutBucketPolicy(bucket, document)
}

// DeleteBucketPolicy removes the policy of the bucket.
func (s *Sia) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return s.store.DeleteBucketPolicy(bucket)
}

// PutBucketVersioning sets the versioning state of the bucket.
func (s *Sia) PutBucketVersioning(ctx context.Context, bucket, status string) error {
	return s.store.PutBucketVersioning(bucket, status)
}

// GetBucketVersioning returns the versioning state of the bucket.
func (s *Sia) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	return s.store.GetBucketVersioning(bucket)
}
