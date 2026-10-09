package sqlite

import (
	"database/sql"
	"errors"
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

// objectLockRow reads the lock state of the addressed version, resolving the
// current version when the request addresses none.
func objectLockRow(tx *txn, bid int64, name string, version s3.VersionRequest) (versionID, mode string, retainUntil *int64, legalHold string, err error) {
	var isDeleteMarker bool
	if version.Specified {
		versionID = version.ID
		err = tx.QueryRow(`SELECT is_delete_marker, object_lock_mode, object_lock_retain_until, object_lock_legal_hold
			FROM objects WHERE bucket_id = $1 AND name = $2 AND version_id = $3`,
			bid, name, versionID).Scan(&isDeleteMarker, &mode, &retainUntil, &legalHold)
	} else {
		err = tx.QueryRow(`SELECT version_id, is_delete_marker, object_lock_mode, object_lock_retain_until, object_lock_legal_hold
			FROM objects WHERE bucket_id = $1 AND name = $2 AND is_latest = TRUE`,
			bid, name).Scan(&versionID, &isDeleteMarker, &mode, &retainUntil, &legalHold)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if version.Specified {
			return "", "", nil, "", s3errs.ErrNoSuchVersion
		}
		return "", "", nil, "", s3errs.ErrNoSuchKey
	} else if err != nil {
		return "", "", nil, "", err
	}

	if isDeleteMarker {
		if version.Specified {
			return "", "", nil, "", s3errs.ErrMethodNotAllowed
		}
		return "", "", nil, "", s3errs.ErrNoSuchKey
	}
	return versionID, mode, retainUntil, legalHold, nil
}

// lockedBucketID resolves a bucket the caller owns and which has object lock
// enabled.
func lockedBucketID(tx *txn, accessKeyID, bucket string) (int64, error) {
	bid, err := bucketID(tx, accessKeyID, bucket)
	if err != nil {
		return 0, err
	} else if enabled, err := bucketObjectLockEnabled(tx, bid); err != nil {
		return 0, err
	} else if !enabled {
		return 0, errObjectLockNotEnabled
	}
	return bid, nil
}

// checkRetentionTransition reports whether a version's retention may move to
// next.
func checkRetentionTransition(mode string, retainUntil *int64, next s3.ObjectLockState, bypass bool) error {
	if mode == "" {
		return nil
	}
	current := time.UnixMilli(*retainUntil)
	if current.Before(time.Now()) {
		return nil
	}
	if next.Mode == mode && !next.RetainUntil.Before(current) {
		return nil
	}

	if mode == s3.ObjectLockModeCompliance {
		return fmt.Errorf("a COMPLIANCE retention cannot be shortened, cleared or changed: %w", s3errs.ErrAccessDenied)
	} else if !bypass {
		return fmt.Errorf("weakening a GOVERNANCE retention requires bypass: %w", s3errs.ErrAccessDenied)
	}
	return nil
}

// PutObjectRetention replaces the retention on the addressed version. A zero
// retention clears it.
func (s *Store) PutObjectRetention(accessKeyID, bucket, name string, version s3.VersionRequest, retention s3.ObjectLockState, bypass bool) error {
	return s.transaction(func(tx *txn) error {
		bid, err := lockedBucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		versionID, mode, retainUntil, _, err := objectLockRow(tx, bid, name, version)
		if err != nil {
			return err
		} else if err := checkRetentionTransition(mode, retainUntil, retention, bypass); err != nil {
			return err
		}

		next := objectLockColumns(&retention)
		_, err = tx.Exec(`UPDATE objects SET object_lock_mode = $1, object_lock_retain_until = $2
			WHERE bucket_id = $3 AND name = $4 AND version_id = $5`, next.Mode, next.RetainUntil, bid, name, versionID)
		return err
	})
}

// GetObjectRetention returns the retention on the addressed version.
func (s *Store) GetObjectRetention(accessKeyID, bucket, name string, version s3.VersionRequest) (state s3.ObjectLockState, err error) {
	err = s.transaction(func(tx *txn) error {
		state = s3.ObjectLockState{} // reset if the transaction retries

		bid, err := lockedBucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		_, mode, retainUntil, _, err := objectLockRow(tx, bid, name, version)
		if err != nil {
			return err
		} else if mode == "" {
			return s3errs.ErrNoSuchObjectLockConfiguration
		}
		state = s3.ObjectLockState{Mode: mode, RetainUntil: time.UnixMilli(*retainUntil).UTC()}
		return nil
	})
	return
}

// PutObjectLegalHold turns the legal hold on the addressed version on or off.
func (s *Store) PutObjectLegalHold(accessKeyID, bucket, name string, version s3.VersionRequest, status string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := lockedBucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		versionID, _, _, _, err := objectLockRow(tx, bid, name, version)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`UPDATE objects SET object_lock_legal_hold = $1
			WHERE bucket_id = $2 AND name = $3 AND version_id = $4`, status, bid, name, versionID)
		return err
	})
}

// GetObjectLegalHold returns the legal hold on the addressed version, or "" when
// none was ever applied.
func (s *Store) GetObjectLegalHold(accessKeyID, bucket, name string, version s3.VersionRequest) (status string, err error) {
	err = s.transaction(func(tx *txn) error {
		status = "" // reset if the transaction retries

		bid, err := lockedBucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, _, _, status, err = objectLockRow(tx, bid, name, version)
		return err
	})
	return
}

// checkObjectLock refuses to destroy a version that a retention or a legal hold
// still protects. A GOVERNANCE retention yields to bypass, a COMPLIANCE one
// never does, and a legal hold yields to nothing.
func checkObjectLock(tx *txn, bid int64, name, version string, bypass bool) error {
	var mode string
	var legalHold string
	var retainUntil *int64
	err := tx.QueryRow(`SELECT object_lock_mode, object_lock_retain_until, object_lock_legal_hold
		FROM objects WHERE bucket_id = $1 AND name = $2 AND version_id = $3`, bid, name, version).Scan(&mode, &retainUntil, &legalHold)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // nothing to protect, the caller reports the miss
	} else if err != nil {
		return err
	}

	if legalHold == s3.LegalHoldOn {
		return fmt.Errorf("version is under a legal hold: %w", s3errs.ErrAccessDenied)
	}
	if mode == "" || retainUntil == nil || time.UnixMilli(*retainUntil).Before(time.Now()) {
		return nil
	}
	if mode == s3.ObjectLockModeCompliance {
		return fmt.Errorf("version is under a COMPLIANCE retention: %w", s3errs.ErrAccessDenied)
	} else if !bypass {
		return fmt.Errorf("version is under a GOVERNANCE retention: %w", s3errs.ErrAccessDenied)
	}
	return nil
}
