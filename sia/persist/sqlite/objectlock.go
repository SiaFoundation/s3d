package sqlite

import (
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// GetBucketObjectLockConfiguration returns the object lock configuration of the
// bucket.
func (s *Store) GetBucketObjectLockConfiguration(accessKeyID, bucket string) (config s3.ObjectLockConfiguration, err error) {
	err = s.transaction(func(tx *txn) error {
		config = s3.ObjectLockConfiguration{} // reset if the transaction retries

		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		var enabled bool
		var mode string
		var days *int
		var years *int
		err = tx.QueryRow(`SELECT object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years
			FROM buckets WHERE id = $1`, bid).Scan(&enabled, &mode, &days, &years)
		if err != nil {
			return err
		} else if !enabled {
			return s3errs.ErrObjectLockConfigurationNotFoundError
		}

		config.ObjectLockEnabled = s3.ObjectLockEnabled
		if mode != "" {
			config.Rule = &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Mode:  mode,
					Days:  days,
					Years: years,
				},
			}
		}
		return nil
	})
	return
}

// PutBucketObjectLockConfiguration turns on object lock for the bucket and
// replaces its default retention rule. A configuration carrying no rule clears
// the default retention.
func (s *Store) PutBucketObjectLockConfiguration(accessKeyID, bucket string, config s3.ObjectLockConfiguration) error {
	return s.transaction(func(tx *txn) error {
		bid, status, err := bucketIDAndVersioning(tx, accessKeyID, bucket)
		if err != nil {
			return err
		} else if status != s3.VersioningStatusEnabled {
			return s3errs.ErrInvalidBucketState
		}

		if err := config.Validate(); err != nil {
			return err
		}

		var mode string
		var days *int
		var years *int
		if config.Rule != nil {
			mode = config.Rule.DefaultRetention.Mode
			days = config.Rule.DefaultRetention.Days
			years = config.Rule.DefaultRetention.Years
		}

		_, err = tx.Exec(`UPDATE buckets SET object_lock_enabled = TRUE, default_retention_mode = $1,
			default_retention_days = $2, default_retention_years = $3 WHERE id = $4`, mode, days, years, bid)
		return err
	})
}

// bucketObjectLockEnabled reports whether the bucket has object lock turned on.
func bucketObjectLockEnabled(tx *txn, bid int64) (enabled bool, err error) {
	err = tx.QueryRow(`SELECT object_lock_enabled FROM buckets WHERE id = $1`, bid).Scan(&enabled)
	return
}
