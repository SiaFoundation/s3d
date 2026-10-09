package sia

import (
	"context"

	"github.com/SiaFoundation/s3d/s3"
)

// PutBucketObjectLockConfiguration turns on object lock for the bucket and
// replaces its default retention rule.
func (s *Sia) PutBucketObjectLockConfiguration(ctx context.Context, accessKeyID, bucket string, config s3.ObjectLockConfiguration) error {
	return s.store.PutBucketObjectLockConfiguration(accessKeyID, bucket, config)
}

// GetBucketObjectLockConfiguration returns the object lock configuration of the
// bucket.
func (s *Sia) GetBucketObjectLockConfiguration(ctx context.Context, accessKeyID, bucket string) (s3.ObjectLockConfiguration, error) {
	return s.store.GetBucketObjectLockConfiguration(accessKeyID, bucket)
}
