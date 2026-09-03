package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// CreateBucket creates a new bucket owned by the user associated with the
// given access key.
func (s *Store) CreateBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		uid, err := userIDForAccessKey(tx, accessKeyID)
		if err != nil {
			return err
		}

		res, err := tx.Exec("INSERT INTO buckets (name, created_at, user_id) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING", bucket, sqlTime(time.Now()), uid)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			// bucket already exists, check ownership
			var ownerID int64
			if err := tx.QueryRow("SELECT user_id FROM buckets WHERE name = $1", bucket).Scan(&ownerID); err != nil {
				return err
			} else if ownerID == uid {
				return s3errs.ErrBucketAlreadyOwnedByYou
			}
			return s3errs.ErrBucketAlreadyExists
		}
		return nil
	})
}

// DeleteBucket deletes a bucket if it is empty and owned by the requesting
// user.
func (s *Store) DeleteBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		var inUse bool
		err = tx.QueryRow(`
			SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1)
				OR EXISTS(SELECT 1 FROM multipart_uploads WHERE bucket_id = $1)`, bid).Scan(&inUse)
		if err != nil {
			return err
		} else if inUse {
			return s3errs.ErrBucketNotEmpty
		}
		_, err = tx.Exec("DELETE FROM buckets WHERE id = $1", bid)
		return err
	})
}

// HeadBucket verifies that the bucket exists and is owned by the user
// associated with the given access key.
func (s *Store) HeadBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		_, err := bucketID(tx, accessKeyID, bucket)
		return err
	})
}

// ListBuckets lists all buckets owned by the user associated with the given
// access key.
func (s *Store) ListBuckets(accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	err := s.transaction(func(tx *txn) error {
		buckets = buckets[:0] // reuse same slice if transaction retries

		uid, err := userIDForAccessKey(tx, accessKeyID)
		if err != nil {
			return err
		}

		rows, err := tx.Query("SELECT name, created_at FROM buckets WHERE user_id = $1", uid)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var createdAt time.Time
			var name string
			if err := rows.Scan(&name, (*sqlTime)(&createdAt)); err != nil {
				return err
			}
			buckets = append(buckets, s3.BucketInfo{
				Name:         name,
				CreationDate: s3.NewContentTime(createdAt),
			})
		}
		return rows.Err()
	})
	return buckets, err
}

// GetBucketVersioning returns the versioning status of the bucket. The status
// is one of "" (never configured), "Enabled" or "Suspended".
func (s *Store) GetBucketVersioning(accessKeyID, bucket string) (status string, err error) {
	err = s.transaction(func(tx *txn) error {
		_, status, err = bucketIDAndVersioning(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// PutBucketVersioning sets the versioning status of the bucket to status, which
// must be "Enabled" or "Suspended".
func (s *Store) PutBucketVersioning(accessKeyID, bucket, status string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET versioning_status = $1 WHERE id = $2`, status, bid)
		return err
	})
}

// bucketVersioning returns the versioning status stored for the bucket (one of
// "", s3.VersioningStatusEnabled or s3.VersioningStatusSuspended), which drives the write
// and delete state machine in versioning.go.
func bucketVersioning(tx *txn, bid int64) (status string, err error) {
	err = tx.QueryRow(`SELECT versioning_status FROM buckets WHERE id = $1`, bid).Scan(&status)
	return
}

// bucketIDAndVersioning returns the bucket ID and versioning status after
// verifying ownership with the given access key.
func bucketIDAndVersioning(tx *txn, accessKeyID, bucket string) (bid int64, status string, err error) {
	bid, err = bucketID(tx, accessKeyID, bucket)
	if err != nil {
		return 0, "", err
	}
	status, err = bucketVersioning(tx, bid)
	return bid, status, err
}

// PutBucketPolicy stores the bucket's policy, replacing any existing one.
func (s *Store) PutBucketPolicy(accessKeyID, bucket string, policy s3.BucketPolicy) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET policy = $1, public_actions = $2 WHERE id = $3`, policy.Document, policy.Public, bid)
		return err
	})
}

// GetBucketPolicy returns the bucket's policy, or ErrNoSuchBucketPolicy if the
// bucket has none.
func (s *Store) GetBucketPolicy(accessKeyID, bucket string) (policy s3.BucketPolicy, err error) {
	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		err = tx.QueryRow(`SELECT policy, public_actions FROM buckets WHERE id = $1`, bid).Scan(&policy.Document, &policy.Public)
		if err != nil {
			return err
		} else if policy.Document == "" {
			return s3errs.ErrNoSuchBucketPolicy
		}
		return nil
	})
	if err != nil {
		return s3.BucketPolicy{}, err
	}
	return policy, nil
}

// DeleteBucketPolicy removes the bucket's policy. It is not an error if the
// bucket has none.
func (s *Store) DeleteBucketPolicy(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET policy = '', public_actions = 0 WHERE id = $1`, bid)
		return err
	})
}

// bucketRead is a bucket resolved for reading.
type bucketRead struct {
	id         int64
	owner      string
	versioning string
}

// userInfo returns the bucket's owner, which a listing reports in place of the
// caller.
func (b bucketRead) userInfo() *s3.UserInfo {
	return &s3.UserInfo{ID: b.owner, DisplayName: b.owner}
}

// bucketForRead resolves a bucket for a read. The caller must own the bucket,
// as in [bucketID], or its policy must grant action to everyone, which covers
// signed requests from other users as well as unsigned ones. An anonymous
// caller gets ErrAccessDenied whether or not the bucket exists, so it cannot
// probe for private buckets.
func bucketForRead(tx *txn, accessKeyID *string, bucket string, action s3.PolicyActions) (bucketRead, error) {
	var b bucketRead
	var ownerID int64
	var granted s3.PolicyActions
	err := tx.QueryRow(`
		SELECT b.id, b.user_id, u.name, b.public_actions, b.versioning_status
		FROM buckets b
		INNER JOIN users u ON u.id = b.user_id
		WHERE b.name = $1`, bucket).Scan(&b.id, &ownerID, &b.owner, &granted, &b.versioning)
	if errors.Is(err, sql.ErrNoRows) {
		if accessKeyID == nil {
			return bucketRead{}, s3errs.ErrAccessDenied
		}
		return bucketRead{}, s3errs.ErrNoSuchBucket
	} else if err != nil {
		return bucketRead{}, err
	}

	if accessKeyID != nil {
		uid, err := userIDForAccessKey(tx, *accessKeyID)
		if err != nil {
			return bucketRead{}, err
		} else if ownerID == uid {
			return b, nil
		}
	}
	if !granted.Allows(action) {
		return bucketRead{}, s3errs.ErrAccessDenied
	}
	return b, nil
}

// bucketID returns the ID of the bucket with the given name if the user
// associated with the given access key owns it. Returns ErrNoSuchBucket if
// the bucket does not exist, or ErrAccessDenied if it exists but is owned by
// a different user.
func bucketID(t *txn, accessKeyID, bucket string) (int64, error) {
	uid, err := userIDForAccessKey(t, accessKeyID)
	if err != nil {
		return 0, err
	}

	var bid, ownerID int64
	err = t.QueryRow(`SELECT id, user_id FROM buckets WHERE name = $1`, bucket).Scan(&bid, &ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, s3errs.ErrNoSuchBucket
	} else if err != nil {
		return 0, err
	} else if ownerID != uid {
		return 0, s3errs.ErrAccessDenied
	}
	return bid, nil
}

// bucketIDByName returns the ID of the bucket with the given name regardless
// of ownership. It is intended for internal callers like the upload loop and
// metadata sync paths that have no access key.
func bucketIDByName(t *txn, bucket string) (bid int64, err error) {
	err = t.QueryRow(`SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bid)
	if errors.Is(err, sql.ErrNoRows) {
		err = s3errs.ErrNoSuchBucket
	}
	return
}
