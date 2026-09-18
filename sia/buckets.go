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

// HeadBucket checks that the bucket exists and is readable by the given access
// key, which is nil for an anonymous caller.
func (s *Sia) HeadBucket(ctx context.Context, accessKeyID *string, name string) error {
	return s.store.HeadBucket(accessKeyID, name)
}

// ListBuckets lists all available buckets for the user identified by the
// given access key.
func (s *Sia) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	return s.store.ListBuckets(accessKeyID)
}

// AssertBucketOwner checks that the given access key owns the bucket.
func (s *Sia) AssertBucketOwner(ctx context.Context, accessKeyID, bucket string) error {
	return s.store.AssertBucketOwner(accessKeyID, bucket)
}

// PutBucketPolicy sets the policy of the bucket, replacing any existing one.
func (s *Sia) PutBucketPolicy(ctx context.Context, accessKeyID, bucket string, policy s3.BucketPolicy) error {
	return s.store.PutBucketPolicy(accessKeyID, bucket, policy)
}

// GetBucketPolicy returns the policy of the bucket.
func (s *Sia) GetBucketPolicy(ctx context.Context, accessKeyID, bucket string) (s3.BucketPolicy, error) {
	return s.store.GetBucketPolicy(accessKeyID, bucket)
}

// DeleteBucketPolicy removes the policy of the bucket.
func (s *Sia) DeleteBucketPolicy(ctx context.Context, accessKeyID, bucket string) error {
	return s.store.DeleteBucketPolicy(accessKeyID, bucket)
}

// PutBucketVersioning sets the versioning state of the bucket.
func (s *Sia) PutBucketVersioning(ctx context.Context, accessKeyID, bucket, status string) error {
	return s.store.PutBucketVersioning(accessKeyID, bucket, status)
}

// GetBucketVersioning returns the versioning state of the bucket.
func (s *Sia) GetBucketVersioning(ctx context.Context, accessKeyID, bucket string) (string, error) {
	return s.store.GetBucketVersioning(accessKeyID, bucket)
}
