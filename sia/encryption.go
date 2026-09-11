package sia

import (
	"context"
	"encoding/xml"
	"fmt"

	"github.com/SiaFoundation/s3d/s3"
)

// PutBucketEncryptionConfiguration stores the default encryption configuration
// for a bucket, replacing any existing configuration.
func (s *Sia) PutBucketEncryptionConfiguration(_ context.Context, accessKeyID, bucket string, config s3.ServerSideEncryptionConfiguration) error {
	// the namespace attribute is added when the configuration is read back
	config.Xmlns = ""
	buf, err := xml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal encryption configuration: %w", err)
	}
	return s.store.PutBucketEncryptionConfiguration(accessKeyID, bucket, string(buf))
}

// GetBucketEncryptionConfiguration returns the default encryption configuration
// for a bucket.
func (s *Sia) GetBucketEncryptionConfiguration(_ context.Context, accessKeyID, bucket string) (s3.ServerSideEncryptionConfiguration, error) {
	raw, err := s.store.GetBucketEncryptionConfiguration(accessKeyID, bucket)
	if err != nil {
		return s3.ServerSideEncryptionConfiguration{}, err
	}
	var config s3.ServerSideEncryptionConfiguration
	if err := xml.Unmarshal([]byte(raw), &config); err != nil {
		return s3.ServerSideEncryptionConfiguration{}, fmt.Errorf("failed to unmarshal encryption configuration: %w", err)
	}
	return config, nil
}

// DeleteBucketEncryptionConfiguration removes the default encryption
// configuration for a bucket.
func (s *Sia) DeleteBucketEncryptionConfiguration(_ context.Context, accessKeyID, bucket string) error {
	return s.store.DeleteBucketEncryptionConfiguration(accessKeyID, bucket)
}
