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

// PutObjectRetention replaces the retention on the addressed version.
func (s *Sia) PutObjectRetention(ctx context.Context, accessKeyID, bucket, object string, version s3.VersionRequest, retention s3.ObjectLockState, bypass bool) error {
	return s.store.PutObjectRetention(accessKeyID, bucket, object, version, retention, bypass)
}

// GetObjectRetention returns the retention on the addressed version.
func (s *Sia) GetObjectRetention(ctx context.Context, accessKeyID, bucket, object string, version s3.VersionRequest) (s3.ObjectLockState, error) {
	return s.store.GetObjectRetention(accessKeyID, bucket, object, version)
}

// PutObjectLegalHold turns the legal hold on the addressed version on or off.
func (s *Sia) PutObjectLegalHold(ctx context.Context, accessKeyID, bucket, object string, version s3.VersionRequest, status string) error {
	return s.store.PutObjectLegalHold(accessKeyID, bucket, object, version, status)
}

// GetObjectLegalHold returns the legal hold on the addressed version.
func (s *Sia) GetObjectLegalHold(ctx context.Context, accessKeyID, bucket, object string, version s3.VersionRequest) (string, error) {
	return s.store.GetObjectLegalHold(accessKeyID, bucket, object, version)
}
