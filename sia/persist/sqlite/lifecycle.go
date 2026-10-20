package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
)

// expiringVersion identifies one row of the objects table selected for
// expiration.
type expiringVersion struct {
	name    string
	version string
}

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
			SELECT buckets.name, blc.configuration
			FROM bucket_lifecycle_configurations blc
			JOIN buckets ON buckets.id = blc.bucket_id
		`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c sia.BucketLifecycleConfiguration
			if err := rows.Scan(&c.Bucket, &c.Configuration); err != nil {
				return err
			}
			configs = append(configs, c)
		}
		return rows.Err()
	})
	return
}

// AbortMultipartUploads deletes up to limit incomplete multipart uploads in the
// named bucket that match prefix and were initiated at or before the cutoff. It
// returns the removed uploads and the on-disk size of their parts so the caller
// can clean up the upload directories. It performs no ownership checks.
func (s *Store) AbortMultipartUploads(bucket string, prefix string, before time.Time, limit int) (aborted []sia.AbortedUpload, err error) {
	err = s.transaction(func(tx *txn) error {
		aborted = nil

		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}

		where := []string{"bucket_id = ?", "created_at <= ?"}
		args := []any{bid, sqlTime(before)}
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

// ExpireObjects expires up to limit objects whose current version matches the
// prefix and size bounds and was last modified at or before the cutoff,
// applying a versioning-aware delete (see [deleteCurrentObject]) to each.
// Noncurrent versions are left to [Store.ExpireNoncurrentVersions]. Returns the
// number of objects expired and performs no ownership checks.
func (s *Store) ExpireObjects(bucket string, prefix string, sizeGreaterThan, sizeLessThan *int64, before time.Time, limit int) (deleted int, orphans []objects.OrphanedFile, err error) {
	err = s.transaction(func(tx *txn) error {
		deleted = 0
		orphans = nil

		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}
		status, err := bucketVersioning(tx, bid)
		if err != nil {
			return err
		}

		// only the current version of each key, and only objects (not existing
		// delete markers), so the cutoff applies to what a reader would see.
		where := []string{
			"o.bucket_id = ?",
			"o.is_latest = TRUE",
			"o.is_delete_marker = FALSE",
			"o.updated_at <= ?",
		}
		args := []any{bid, sqlTime(before)}
		if prefix != "" {
			where = append(where, "o.name >= ? AND o.name < ?")
			args = append(args, prefix, prefix+"\xFF")
		}
		if sizeGreaterThan != nil {
			where = append(where, "o.size > ?")
			args = append(args, *sizeGreaterThan)
		}
		if sizeLessThan != nil {
			where = append(where, "o.size < ?")
			args = append(args, *sizeLessThan)
		}
		args = append(args, limit)

		query := fmt.Sprintf(`SELECT o.name FROM objects o WHERE %s LIMIT ?`, strings.Join(where, " AND "))
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
			res, err := deleteCurrentObject(tx, bid, name, status, s3.ObjectID{Key: name})
			if errors.Is(err, sql.ErrNoRows) {
				continue // current version vanished concurrently; nothing to do
			} else if err != nil {
				return err
			}
			if res.orphanFile.Filename != "" {
				orphans = append(orphans, res.orphanFile)
			}
		}
		// the candidate count, not the removed count, the caller ends the sweep
		// when a batch comes back short
		deleted = len(names)
		return nil
	})
	return
}

// ExpireNoncurrentVersions expires up to limit noncurrent versions in bucket
// whose key matches the prefix, whose size matches the bounds, that became
// noncurrent at or before the cutoff, and that sit behind more than retain
// newer noncurrent versions of the same key. A retain of 0 applies no such
// bound. Returns the number of versions expired and performs no ownership
// checks.
func (s *Store) ExpireNoncurrentVersions(bucket string, prefix string, sizeGreaterThan, sizeLessThan *int64, retain int, before time.Time, limit int) (deleted int, orphans []objects.OrphanedFile, err error) {
	err = s.transaction(func(tx *txn) error {
		deleted = 0
		orphans = nil

		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}

		// the ranking spans every noncurrent version of a matching key, so
		// retain counts versions the cutoff and size bounds exclude
		inner := []string{"o.bucket_id = ?", "o.is_latest = FALSE"}
		args := []any{bid}
		if prefix != "" {
			inner = append(inner, "o.name >= ? AND o.name < ?")
			args = append(args, prefix, prefix+"\xFF")
		}

		outer := []string{"rn > ?", "noncurrent_at <= ?"}
		args = append(args, retain, sqlTime(before))
		if sizeGreaterThan != nil {
			outer = append(outer, "size > ?")
			args = append(args, *sizeGreaterThan)
		}
		if sizeLessThan != nil {
			outer = append(outer, "size < ?")
			args = append(args, *sizeLessThan)
		}
		args = append(args, limit)

		query := fmt.Sprintf(`
			SELECT name, version_id FROM (
				SELECT o.name, o.version_id, o.size, o.noncurrent_at,
					ROW_NUMBER() OVER (PARTITION BY o.bucket_id, o.name ORDER BY o.seq DESC) AS rn
				FROM objects o WHERE %s
			) WHERE %s
			LIMIT ?`, strings.Join(inner, " AND "), strings.Join(outer, " AND "))

		versions, err := selectExpiringVersions(tx, query, args...)
		if err != nil {
			return err
		}
		return expireVersions(tx, bid, versions, &deleted, &orphans)
	})
	return
}

// ExpireDeleteMarkers removes up to limit delete markers in bucket whose key
// matches the prefix, that were created at or before the cutoff, and that are
// the last remaining version of their key. Returns the number removed and
// performs no ownership checks.
func (s *Store) ExpireDeleteMarkers(bucket string, prefix string, before time.Time, limit int) (deleted int, orphans []objects.OrphanedFile, err error) {
	err = s.transaction(func(tx *txn) error {
		deleted = 0
		orphans = nil

		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}

		where := []string{
			"o.bucket_id = ?",
			"o.is_latest = TRUE",
			"o.is_delete_marker = TRUE",
			"o.updated_at <= ?",
			// an expired object delete marker is one with no other version left
			`NOT EXISTS (SELECT 1 FROM objects x
				WHERE x.bucket_id = o.bucket_id AND x.name = o.name AND x.version_id != o.version_id)`,
		}
		args := []any{bid, sqlTime(before)}
		if prefix != "" {
			where = append(where, "o.name >= ? AND o.name < ?")
			args = append(args, prefix, prefix+"\xFF")
		}
		args = append(args, limit)

		query := fmt.Sprintf(`SELECT o.name, o.version_id FROM objects o WHERE %s LIMIT ?`, strings.Join(where, " AND "))
		versions, err := selectExpiringVersions(tx, query, args...)
		if err != nil {
			return err
		}
		return expireVersions(tx, bid, versions, &deleted, &orphans)
	})
	return
}

// selectExpiringVersions runs a query selecting a name and version_id pair per row.
func selectExpiringVersions(tx *txn, query string, args ...any) ([]expiringVersion, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []expiringVersion
	for rows.Next() {
		var v expiringVersion
		if err := rows.Scan(&v.name, &v.version); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

// expireVersions deletes each version, orphaning its backing data, and reports
// how many were removed. A version that vanished concurrently is skipped.
func expireVersions(tx *txn, bid int64, versions []expiringVersion, deleted *int, orphans *[]objects.OrphanedFile) error {
	for _, v := range versions {
		row, found, err := deleteObject(tx, bid, v.name, v.version, false)
		if err != nil {
			return fmt.Errorf("failed to delete version: %w", err)
		} else if !found {
			continue
		}
		orphan, err := orphanDeleted(tx, row)
		if err != nil {
			return fmt.Errorf("failed to orphan deleted version: %w", err)
		}
		if orphan.Filename != "" {
			*orphans = append(*orphans, orphan)
		}
		*deleted++
	}
	return nil
}
