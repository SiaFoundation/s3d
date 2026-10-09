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

// DeleteBucket deletes a bucket if it is empty.
func (s *Store) DeleteBucket(bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
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
func (s *Store) GetBucketVersioning(bucket string) (status string, err error) {
	err = s.transaction(func(tx *txn) error {
		_, status, err = bucketIDAndVersioning(tx, bucket)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// PutBucketVersioning sets the versioning status of the bucket to status, which
// must be "Enabled" or "Suspended".
func (s *Store) PutBucketVersioning(bucket, status string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET versioning_status = $1 WHERE id = $2`, status, bid)
		return err
	})
}

// bucketIDAndVersioning returns the bucket ID and versioning status (one of "",
// s3.VersioningStatusEnabled or s3.VersioningStatusSuspended), which drives the
// write and delete state machine in versioning.go.
func bucketIDAndVersioning(tx *txn, bucket string) (bid int64, status string, err error) {
	err = tx.QueryRow(`SELECT id, versioning_status FROM buckets WHERE name = $1`, bucket).Scan(&bid, &status)
	if errors.Is(err, sql.ErrNoRows) {
		err = s3errs.ErrNoSuchBucket
	}
	return
}

// PutBucketPolicy stores the bucket's policy document, replacing any existing
// one.
func (s *Store) PutBucketPolicy(bucket, document string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET policy = $1 WHERE id = $2`, document, bid)
		return err
	})
}

// DeleteBucketPolicy removes the bucket's policy. It is not an error if the
// bucket has none.
func (s *Store) DeleteBucketPolicy(bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE buckets SET policy = '' WHERE id = $1`, bid)
		return err
	})
}

// BucketAccessInfo returns the bucket owner and the stored policy document.
func (s *Store) BucketAccessInfo(bucket string) (info s3.BucketAccessInfo, err error) {
	err = s.transaction(func(tx *txn) error {
		var owner string
		err := tx.QueryRow(`
			SELECT u.name, b.policy
			FROM buckets b
			INNER JOIN users u ON u.id = b.user_id
			WHERE b.name = $1`, bucket).Scan(&owner, &info.PolicyDocument)
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchBucket
		} else if err != nil {
			return err
		}
		info.Owner = &s3.UserInfo{ID: owner, DisplayName: owner}
		return nil
	})
	return
}

// bucketID returns the ID of the bucket with the given name.
func bucketID(t *txn, bucket string) (bid int64, err error) {
	err = t.QueryRow(`SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bid)
	if errors.Is(err, sql.ErrNoRows) {
		err = s3errs.ErrNoSuchBucket
	}
	return
}
