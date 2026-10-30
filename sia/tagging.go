package sia

import (
	"context"

	"github.com/SiaFoundation/s3d/s3"
)

// GetBucketTagging returns the tag set of the bucket.
func (s *Sia) GetBucketTagging(ctx context.Context, bucket s3.BucketID) (s3.TagSet, error) {
	return s.store.GetBucketTagging(bucket)
}

// PutBucketTagging replaces the tag set of the bucket.
func (s *Sia) PutBucketTagging(ctx context.Context, bucket s3.BucketID, tags s3.TagSet) error {
	return s.store.PutBucketTagging(bucket, tags)
}

// DeleteBucketTagging removes the tag set of the bucket.
func (s *Sia) DeleteBucketTagging(ctx context.Context, bucket s3.BucketID) error {
	return s.store.DeleteBucketTagging(bucket)
}

// GetObjectTagging returns the tag set of the addressed version.
func (s *Sia) GetObjectTagging(ctx context.Context, bucket s3.BucketID, object string, version s3.VersionRequest) (s3.ObjectTagging, error) {
	return s.store.GetObjectTagging(bucket, object, version)
}

// PutObjectTagging replaces the tag set of the addressed version.
func (s *Sia) PutObjectTagging(ctx context.Context, bucket s3.BucketID, object string, version s3.VersionRequest, tags s3.TagSet) error {
	return s.store.PutObjectTagging(bucket, object, version, tags)
}

// DeleteObjectTagging removes the tag set of the addressed version.
func (s *Sia) DeleteObjectTagging(ctx context.Context, bucket s3.BucketID, object string, version s3.VersionRequest) error {
	return s.store.DeleteObjectTagging(bucket, object, version)
}
