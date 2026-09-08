package sqlite

import (
	"database/sql"
	"errors"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// PutBucketEncryptionConfiguration stores the serialized default encryption
// configuration for a bucket, replacing any existing configuration.
func (s *Store) PutBucketEncryptionConfiguration(accessKeyID, bucket, config string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`
			INSERT INTO bucket_encryption_configurations (bucket_id, configuration)
			VALUES ($1, $2)
			ON CONFLICT(bucket_id) DO UPDATE SET
				configuration = EXCLUDED.configuration
		`, bid, config)
		return err
	})
}

// GetBucketEncryptionConfiguration returns the serialized default encryption
// configuration for a bucket, or
// ErrServerSideEncryptionConfigurationNotFoundError if none is set.
func (s *Store) GetBucketEncryptionConfiguration(accessKeyID, bucket string) (config string, err error) {
	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		err = tx.QueryRow(`SELECT configuration FROM bucket_encryption_configurations WHERE bucket_id = $1`, bid).Scan(&config)
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrServerSideEncryptionConfigurationNotFoundError
		}
		return err
	})
	return
}

// DeleteBucketEncryptionConfiguration removes the default encryption
// configuration for a bucket. It is not an error if no configuration exists.
func (s *Store) DeleteBucketEncryptionConfiguration(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM bucket_encryption_configurations WHERE bucket_id = $1`, bid)
		return err
	})
}
