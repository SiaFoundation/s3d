package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// CreateBucket creates a new bucket.
func (s *Store) CreateBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("INSERT INTO buckets (name, created_at) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING", bucket, time.Now().Unix())
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			// NOTE: since we don't support multiple accounts yet, any existing
			// bucket must be owned by the same user
			return s3errs.ErrBucketAlreadyOwnedByYou
		}
		return err
	})
}

// DeleteBucket deletes a bucket if it is empty.
func (s *Store) DeleteBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		var inUse bool
		err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1)", bid).Scan(&inUse)
		if err != nil {
			return err
		} else if inUse {
			return s3errs.ErrBucketNotEmpty
		}
		_, err = tx.Exec("DELETE FROM buckets WHERE id = $1", bid)
		return err
	})
}

// HeadBucket checks if a bucket exists and returns an error otherwise.
func (s *Store) HeadBucket(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		_, err := bucketID(tx, bucket)
		return err
	})
}

// ListBuckets lists all buckets.
func (s *Store) ListBuckets(accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	err := s.transaction(func(tx *txn) error {
		rows, err := tx.Query("SELECT name, created_at FROM buckets")
		if err != nil {
			return err
		}
		for rows.Next() {
			var createdAt int64
			var name string
			if err := rows.Scan(&name, &createdAt); err != nil {
				return err
			}
			buckets = append(buckets, s3.BucketInfo{
				Name:         name,
				CreationDate: s3.NewContentTime(time.Unix(createdAt, 0)),
			})
		}
		return rows.Close()
	})
	return buckets, err
}

// bucketID returns the ID of the bucket with the given name or an error if it
// does not exist.
func bucketID(t *txn, bucket string) (bucketID int64, err error) {
	err = t.QueryRow(`SELECT id FROM buckets WHERE buckets.name = $1`, bucket).Scan(&bucketID)
	if errors.Is(err, sql.ErrNoRows) {
		err = s3errs.ErrNoSuchBucket
	}
	return
}
