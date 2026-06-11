package sia

import (
	"context"
	"encoding/xml"
	"fmt"
	"path/filepath"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.uber.org/zap"
)

// BucketLifecycleConfiguration pairs a bucket with its serialized lifecycle
// configuration. The bucket ID is used to apply rules so a bucket deleted and
// recreated under the same name mid-run cannot have stale rules applied to it;
// the name is informational.
type BucketLifecycleConfiguration struct {
	BucketID      int64
	Bucket        string
	Configuration string
}

// AbortedUpload identifies a multipart upload aborted by a lifecycle rule
// along with the on-disk size of its parts.
type AbortedUpload struct {
	UploadID s3.UploadID
	Size     int64
}

// OrphanedFile identifies an on-disk file orphaned by lifecycle expiration.
type OrphanedFile struct {
	Filename string
	Size     int64
}

// PutBucketLifecycleConfiguration stores the lifecycle configuration for a
// bucket, replacing any existing configuration.
func (s *Sia) PutBucketLifecycleConfiguration(_ context.Context, accessKeyID, bucket string, config s3.LifecycleConfiguration) error {
	// the namespace attribute is added when the configuration is read back
	config.Xmlns = ""
	buf, err := xml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal lifecycle configuration: %w", err)
	}
	return s.store.PutBucketLifecycleConfiguration(accessKeyID, bucket, string(buf))
}

// GetBucketLifecycleConfiguration returns the lifecycle configuration for a
// bucket.
func (s *Sia) GetBucketLifecycleConfiguration(_ context.Context, accessKeyID, bucket string) (s3.LifecycleConfiguration, error) {
	raw, err := s.store.GetBucketLifecycleConfiguration(accessKeyID, bucket)
	if err != nil {
		return s3.LifecycleConfiguration{}, err
	}
	var config s3.LifecycleConfiguration
	if err := xml.Unmarshal([]byte(raw), &config); err != nil {
		return s3.LifecycleConfiguration{}, fmt.Errorf("failed to unmarshal lifecycle configuration: %w", err)
	}
	return config, nil
}

// DeleteBucketLifecycleConfiguration removes the lifecycle configuration for a
// bucket.
func (s *Sia) DeleteBucketLifecycleConfiguration(_ context.Context, accessKeyID, bucket string) error {
	return s.store.DeleteBucketLifecycleConfiguration(accessKeyID, bucket)
}

// lifecycleLoop periodically applies bucket lifecycle rules.
func (s *Sia) lifecycleLoop(ctx context.Context) {
	t := time.NewTicker(s.lifecycleLoopInterval)
	defer t.Stop()

	for {
		s.applyLifecycleRules(ctx, time.Now())

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// applyLifecycleRules evaluates every bucket's lifecycle configuration once,
// aborting incomplete multipart uploads and expiring objects as configured.
// Rule cutoffs are computed relative to now.
func (s *Sia) applyLifecycleRules(ctx context.Context, now time.Time) {
	configs, err := s.store.AllBucketLifecycleConfigurations()
	if err != nil {
		s.logger.Error("failed to load lifecycle configurations", zap.Error(err))
		return
	}

	for _, bc := range configs {
		if ctx.Err() != nil {
			return
		}

		var config s3.LifecycleConfiguration
		if err := xml.Unmarshal([]byte(bc.Configuration), &config); err != nil {
			s.logger.Error("failed to parse lifecycle configuration", zap.String("bucket", bc.Bucket), zap.Error(err))
			continue
		}

		for _, rule := range config.Rules {
			if !rule.Enabled() {
				continue
			}
			prefix := rule.EffectivePrefix()

			if rule.AbortIncompleteMultipartUpload != nil {
				s.abortIncompleteUploads(ctx, bc, prefix, rule.AbortIncompleteMultipartUpload.AbortCutoff(now, s.lifecycleDayDuration))
			}
			if cutoff, ok := rule.Expiration.ExpiryCutoff(now, s.lifecycleDayDuration); ok {
				s.expireObjects(ctx, bc, prefix, cutoff)
			}
		}
	}
}

// abortIncompleteUploads aborts incomplete multipart uploads matching prefix in
// the given bucket that were initiated at or before the cutoff.
func (s *Sia) abortIncompleteUploads(ctx context.Context, bc BucketLifecycleConfiguration, prefix string, before time.Time) {
	// batchSize bounds how many uploads are aborted per store transaction.
	const batchSize = 100
	var aborted int
	for ctx.Err() == nil {
		uploads, err := s.store.AbortMultipartUploads(bc.BucketID, prefix, before, batchSize)
		if err != nil {
			s.logger.Error("failed to abort multipart uploads", zap.String("bucket", bc.Bucket), zap.Error(err))
			return
		}
		for _, u := range uploads {
			s.cleanupOrphan(s.multipartUploadPath(u.UploadID.String()), u.Size)
		}
		aborted += len(uploads)
		if len(uploads) < batchSize {
			break
		}
	}
	if aborted > 0 {
		s.logger.Info("aborted incomplete multipart uploads via lifecycle rule",
			zap.String("bucket", bc.Bucket), zap.String("prefix", prefix), zap.Int("count", aborted))
	}
}

// expireObjects deletes objects matching prefix in the given bucket that were
// last modified at or before the cutoff.
func (s *Sia) expireObjects(ctx context.Context, bc BucketLifecycleConfiguration, prefix string, before time.Time) {
	// batchSize bounds how many objects are expired per store transaction.
	const batchSize = 100
	var expired int
	for ctx.Err() == nil {
		deleted, orphans, err := s.store.ExpireObjects(bc.BucketID, prefix, before, batchSize)
		if err != nil {
			s.logger.Error("failed to expire objects", zap.String("bucket", bc.Bucket), zap.Error(err))
			return
		}
		for _, o := range orphans {
			s.cleanupOrphan(filepath.Join(s.uploadDir(), o.Filename), o.Size)
		}
		expired += deleted
		if deleted < batchSize {
			break
		}
	}
	if expired > 0 {
		s.logger.Info("expired objects via lifecycle rule",
			zap.String("bucket", bc.Bucket), zap.String("prefix", prefix), zap.Int("count", expired))
	}
}
