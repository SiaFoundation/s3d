package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func (s *Store) CreateBucket(accessKeyID, bucket string) error {
	return s.transaction(func(t *txn) error {
		res, err := t.Exec("INSERT INTO buckets (name, created_at) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING", bucket, time.Now().Unix())
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

func (s *Store) DeleteBucket(accessKeyID, bucket string) error {
	return s.transaction(func(t *txn) error {
		bid, err := bucketID(t, bucket)
		if err != nil {
			return err
		}
		var inUse bool
		err = t.QueryRow("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1)", bid).Scan(&inUse)
		if err != nil {
			return err
		} else if inUse {
			return s3errs.ErrBucketNotEmpty
		}
		_, err = t.Exec("DELETE FROM buckets WHERE id = $1", bid)
		return err
	})
}

func (s *Store) HeadBucket(accessKeyID, bucket string) error {
	return s.transaction(func(t *txn) error {
		_, err := bucketID(t, bucket)
		return err
	})
}

func (s *Store) ListBuckets(accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	err := s.transaction(func(t *txn) error {
		rows, err := t.Query("SELECT name, created_at FROM buckets")
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

func bucketID(t *txn, bucket string) (int64, error) {
	var bucketID int64
	err := t.QueryRow(`SELECT id FROM buckets WHERE buckets.name = $1`, bucket).Scan(&bucketID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, s3errs.ErrNoSuchBucket
	}
	return bucketID, nil
}
