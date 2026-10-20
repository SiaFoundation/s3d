package sqlite

import (
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// errObjectLockNotEnabled is returned for a request lock against a bucket that
// does not have object lock enabled.
var errObjectLockNotEnabled = fmt.Errorf("bucket does not have object lock enabled: %w", s3errs.ErrInvalidRequest)

// GetBucketObjectLockConfiguration returns the object lock configuration of the
// bucket.
func (s *Store) GetBucketObjectLockConfiguration(accessKeyID, bucket string) (config s3.ObjectLockConfiguration, err error) {
	err = s.transaction(func(tx *txn) error {
		config = s3.ObjectLockConfiguration{} // reset if the transaction retries

		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		enabled, mode, days, years, err := bucketObjectLock(tx, bid)
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

// effectiveObjectLock resolves the lock to stamp on a new version of an object
// in bucket bid. An explicit request lock wins over the bucket's default
// retention, which supplies a retention period and never a legal hold. Any
// request lock against a bucket without object lock enabled is refused.
func effectiveObjectLock(tx *txn, bid int64, req *s3.ObjectLockState) (lock objectLock, err error) {
	enabled, defaultMode, days, years, err := bucketObjectLock(tx, bid)
	if err != nil {
		return
	} else if !enabled {
		if req != nil {
			err = errObjectLockNotEnabled
		}
		return
	}

	lock = objectLockColumns(req)
	if lock.Mode == "" && defaultMode != "" {
		expiry := time.Now().UTC()
		if days != nil {
			expiry = expiry.AddDate(0, 0, *days)
		} else if years != nil {
			expiry = expiry.AddDate(*years, 0, 0)
		}
		ms := expiry.UnixMilli()
		lock.Mode = defaultMode
		lock.RetainUntil = &ms
	}
	return
}

// objectLock holds the lock columns of a version row.
type objectLock struct {
	Mode        string
	RetainUntil *int64
	LegalHold   string
}

// objectLockColumns flattens a request lock into the columns a row holds. It is
// the inverse of [storedObjectLock].
func objectLockColumns(req *s3.ObjectLockState) (lock objectLock) {
	if req == nil {
		return
	}
	if req.Retained() {
		ms := req.RetainUntil.UnixMilli()
		lock.Mode = req.Mode
		lock.RetainUntil = &ms
	}
	lock.LegalHold = req.LegalHold
	return
}

// storedObjectLock rebuilds the lock state held in a row's columns, returning
// nil when the row carries none.
func storedObjectLock(lock objectLock) *s3.ObjectLockState {
	if lock.Mode == "" && lock.LegalHold == "" {
		return nil
	}
	state := s3.ObjectLockState{Mode: lock.Mode, LegalHold: lock.LegalHold}
	if lock.RetainUntil != nil {
		state.RetainUntil = time.UnixMilli(*lock.RetainUntil).UTC()
	}
	return &state
}

// bucketObjectLock reads the bucket's object lock configuration columns.
func bucketObjectLock(tx *txn, bid int64) (enabled bool, mode string, days *int, years *int, err error) {
	err = tx.QueryRow(`SELECT object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years
		FROM buckets WHERE id = $1`, bid).Scan(&enabled, &mode, &days, &years)
	return
}

// bucketObjectLockEnabled reports whether the bucket has object lock turned on.
func bucketObjectLockEnabled(tx *txn, bid int64) (enabled bool, err error) {
	err = tx.QueryRow(`SELECT object_lock_enabled FROM buckets WHERE id = $1`, bid).Scan(&enabled)
	return
}
