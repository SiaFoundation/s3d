package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
)

// PutBucketLifecycleConfiguration stores the serialized lifecycle configuration
// for a bucket, replacing any existing configuration.
func (s *Store) PutBucketLifecycleConfiguration(accessKeyID, bucket, config string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`
			INSERT INTO bucket_lifecycle_configurations (bucket_id, configuration)
			VALUES ($1, $2)
			ON CONFLICT(bucket_id) DO UPDATE SET
				configuration = EXCLUDED.configuration
		`, bid, config)
		return err
	})
}

// GetBucketLifecycleConfiguration returns the serialized lifecycle
// configuration for a bucket, or ErrNoSuchLifecycleConfiguration if none is set.
func (s *Store) GetBucketLifecycleConfiguration(accessKeyID, bucket string) (config string, err error) {
	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		err = tx.QueryRow(`SELECT configuration FROM bucket_lifecycle_configurations WHERE bucket_id = $1`, bid).Scan(&config)
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchLifecycleConfiguration
		}
		return err
	})
	return
}

// DeleteBucketLifecycleConfiguration removes the lifecycle configuration for a
// bucket. It is not an error if no configuration exists.
func (s *Store) DeleteBucketLifecycleConfiguration(accessKeyID, bucket string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM bucket_lifecycle_configurations WHERE bucket_id = $1`, bid)
		return err
	})
}

// AllBucketLifecycleConfigurations returns the lifecycle configuration for every
// bucket that has one. It is intended for the background lifecycle loop and
// performs no ownership checks.
func (s *Store) AllBucketLifecycleConfigurations() (configs []sia.BucketLifecycleConfiguration, err error) {
	err = s.transaction(func(tx *txn) error {
		configs = nil
		rows, err := tx.Query(`
			SELECT buckets.id, buckets.name, blc.configuration
			FROM bucket_lifecycle_configurations blc
			JOIN buckets ON buckets.id = blc.bucket_id
		`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c sia.BucketLifecycleConfiguration
			if err := rows.Scan(&c.BucketID, &c.Bucket, &c.Configuration); err != nil {
				return err
			}
			configs = append(configs, c)
		}
		return rows.Err()
	})
	return
}

// AbortMultipartUploads deletes up to limit incomplete multipart uploads in the
// bucket identified by bucketID that match prefix and were initiated at or
// before the cutoff. It returns the removed uploads and the on-disk size of
// their parts so the caller can clean up the upload directories. It performs
// no ownership checks.
func (s *Store) AbortMultipartUploads(bucketID int64, prefix string, before time.Time, limit int) (aborted []sia.AbortedUpload, err error) {
	err = s.transaction(func(tx *txn) error {
		aborted = nil

		where := []string{"bucket_id = ?", "created_at <= ?"}
		args := []any{bucketID, sqlTime(before)}
		if prefix != "" {
			where = append(where, "name >= ? AND name < ?")
			args = append(args, prefix, prefix+"\xFF")
		}
		args = append(args, limit)

		query := fmt.Sprintf(`
			SELECT upload_id, COALESCE((
				SELECT SUM(content_length) FROM multipart_parts
				WHERE multipart_parts.upload_id = multipart_uploads.upload_id
			), 0)
			FROM multipart_uploads
			WHERE %s
			LIMIT ?`, strings.Join(where, " AND "))

		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		var found []sia.AbortedUpload
		for rows.Next() {
			var u sia.AbortedUpload
			if err := rows.Scan((*sqlUploadID)(&u.UploadID), &u.Size); err != nil {
				return err
			}
			found = append(found, u)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, u := range found {
			if _, err := tx.Exec(`DELETE FROM multipart_uploads WHERE upload_id = $1`, sqlUploadID(u.UploadID)); err != nil {
				return err
			}
		}
		if len(found) > 0 {
			if err := incrementStat(tx, statMultipartUploads, -int64(len(found))); err != nil {
				return err
			}
		}
		aborted = found
		return nil
	})
	return
}

// ExpireObjects deletes up to limit objects in the bucket identified by
// bucketID that match prefix and were last modified at or before the cutoff.
// Deleted objects backed by Sia data are inserted into orphaned_objects; objects still
// pending on disk have their newly-unreferenced filenames returned for cleanup.
// It returns the number of objects deleted and performs no ownership checks.
func (s *Store) ExpireObjects(bucketID int64, prefix string, before time.Time, limit int) (deleted int, orphans []sia.OrphanedFile, err error) {
	err = s.transaction(func(tx *txn) error {
		deleted = 0
		orphans = nil

		where := []string{"bucket_id = ?", "updated_at <= ?"}
		args := []any{bucketID, sqlTime(before)}
		if prefix != "" {
			where = append(where, "name >= ? AND name < ?")
			args = append(args, prefix, prefix+"\xFF")
		}
		args = append(args, limit)

		query := fmt.Sprintf(`SELECT name FROM objects WHERE %s LIMIT ?`, strings.Join(where, " AND "))
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, name := range names {
			oldID, filename, size, err := deleteObject(tx, bucketID, name)
			if err != nil {
				return err
			}
			if oldID != nil {
				if err := insertOrphan(tx, *oldID); err != nil {
					return err
				}
			}
			orphanFile, orphanSize, err := newOrphanedFile(tx, filename, size)
			if err != nil {
				return err
			}
			if orphanFile != "" {
				orphans = append(orphans, sia.OrphanedFile{Filename: orphanFile, Size: orphanSize})
			}
		}
		deleted = len(names)
		return nil
	})
	return
}
