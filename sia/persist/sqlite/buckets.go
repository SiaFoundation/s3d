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
