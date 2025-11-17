package sqlite

import (
	"database/sql"
	"errors"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func (s *Store) CreateBucket(bucket string) error {
	return s.transaction(func(t *txn) error {
		res, err := t.Exec("INSERT INTO buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", bucket)
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

func (s *Store) DeleteBucket(bucket string) error {
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

func bucketID(t *txn, bucket string) (int64, error) {
	var bucketID int64
	err := t.QueryRow(`SELECT id FROM buckets WHERE buckets.name = $1`, bucket).Scan(&bucketID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, s3errs.ErrNoSuchBucket
	}
	return bucketID, nil
}
