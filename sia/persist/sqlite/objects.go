package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
)

// DiskUsage returns the total bytes currently held on disk in the uploads
// directory, across objects with a staged filename (pending upload or uploaded
// but not yet pinned) and in-progress multipart parts. Objects sharing a
// filename (e.g. via CopyObject) are counted once.
func (s *Store) DiskUsage() (usage uint64, err error) {
	err = s.transaction(func(tx *txn) error {
		var objectsSize, partsSize uint64
		err := tx.QueryRow(`
			SELECT COALESCE(SUM(size), 0)
			FROM (SELECT MAX(size) AS size FROM objects WHERE filename IS NOT NULL GROUP BY filename)
		`).Scan(&objectsSize)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT COALESCE(SUM(content_length), 0) FROM multipart_parts`).Scan(&partsSize); err != nil {
			return err
		}
		usage = objectsSize + partsSize
		return nil
	})
	return
}

// DeleteObject deletes the object with the given bucket and name if it exists
// and all provided preconditions match. If the deleted object's ID has no
// remaining references, it is inserted into the orphaned_objects table. If the
// object hasn't been uploaded yet, its filename is returned for cleanup.
func (s *Store) DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (orphanFile string, orphanSize int64, _ error) {
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		// delete the row and return its values for precondition checks and
		// orphan detection; the transaction rolls back if preconditions fail
		var deletedID sql.Null[sqlHash256]
		var filename *string
		var contentMD5 [16]byte
		var size int64
		var updatedAt time.Time
		err = tx.QueryRow(`
			DELETE FROM objects WHERE bucket_id = $1 AND name = $2
			RETURNING filename, sia_object_id, content_md5, size, updated_at
		`, bid, objectID.Key).Scan(&filename, &deletedID, (*sqlMD5)(&contentMD5), &size, (*sqlTime)(&updatedAt))
		if errors.Is(err, sql.ErrNoRows) {
			return nil // object doesn't exist, nothing to delete
		} else if err != nil {
			return err
		}

		if objectID.ETag != nil && *objectID.ETag != s3.FormatETag(contentMD5[:], 0) {
			return s3errs.ErrPreconditionFailed
		}
		if objectID.Size != nil && *objectID.Size != size {
			return s3errs.ErrPreconditionFailed
		}
		if objectID.LastModifiedTime != nil && !updatedAt.Truncate(time.Second).Equal(objectID.LastModifiedTime.StdTime()) {
			return s3errs.ErrPreconditionFailed
		}

		// an object is pending (filename, no sia_object_id), uploaded
		// (sia_object_id, possibly still keeping its filename as a backup
		// until pinned) or empty.
		if filename != nil && !deletedID.Valid {
			if err := removePendingObject(tx, size); err != nil {
				return err
			}
		}
		if deletedID.Valid {
			if err := removeUploadedObject(tx, size); err != nil {
				return err
			}
		}

		orphanFile, orphanSize, err = newOrphanedFile(tx, filename, size)
		if err != nil {
			return err
		}

		if deletedID.Valid {
			return insertOrphan(tx, types.Hash256(deletedID.V))
		}
		return nil
	})
	return orphanFile, orphanSize, err
}

// GetObject retrieves the object with the given bucket and name.
func (s *Store) GetObject(accessKeyID, bucket, name string, partNumber *int32) (*objects.Object, error) {
	var obj objects.Object
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		return getObject(tx, &obj, bid, name, partNumber)
	}); errors.Is(err, sql.ErrNoRows) {
		return nil, s3errs.ErrNoSuchKey
	} else if err != nil {
		return nil, err
	}

	return &obj, nil
}

func getObject(tx *txn, obj *objects.Object, bid int64, name string, partNumber *int32) error {
	// get parts count from the objects table
	err := tx.QueryRow(`
		SELECT parts_count
		FROM objects
		WHERE bucket_id = $1 AND name = $2
	`, bid, name).Scan(&obj.PartsCount)
	if err != nil {
		return err
	}

	// return full object if no part specified
	if partNumber == nil || obj.PartsCount == 0 {
		if obj.PartsCount == 0 && partNumber != nil && *partNumber != 1 {
			return s3errs.ErrInvalidPart
		}
		var objectID sql.Null[sqlHash256]
		var siaObj sql.Null[sqlSiaObject]
		err := tx.QueryRow(`
			SELECT filename, sia_object_id, metadata, updated_at, size, content_md5, sia_object
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).Scan(&obj.FileName, &objectID, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Length, (*sqlMD5)(&obj.ContentMD5), &siaObj)
		obj.Size = obj.Length
		if objectID.Valid && siaObj.Valid {
			obj.SiaObject = &objects.SiaObject{
				ID:     types.Hash256(objectID.V),
				Sealed: sdk.SealedObject(siaObj.V),
			}
		}
		return err
	}

	// return error if part number is invalid
	if *partNumber > int32(obj.PartsCount) {
		return s3errs.ErrInvalidPart
	}

	// part specified, return part info
	var partObjID sql.Null[sqlHash256]
	var siaObj sql.Null[sqlSiaObject]
	err = tx.QueryRow(`
		SELECT o.filename, o.sia_object_id, o.sia_object, o.metadata, o.updated_at, o.size, p.offset, p.content_length, p.content_md5, o.sia_object
		FROM object_parts p
		JOIN objects o ON o.bucket_id = p.bucket_id AND o.name = p.name
		WHERE o.bucket_id = $1 AND o.name = $2 AND p.part_number = $3
	`, bid, name, *partNumber).Scan(&obj.FileName, &partObjID, &siaObj, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Size, &obj.Offset, &obj.Length, (*sqlMD5)(&obj.ContentMD5), &siaObj)
	if partObjID.Valid && siaObj.Valid {
		obj.SiaObject = &objects.SiaObject{
			ID:     types.Hash256(partObjID.V),
			Sealed: sdk.SealedObject(siaObj.V),
		}
	}
	return err
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists. If the overwritten object's ID has no
// remaining references, it is inserted into the orphaned_objects table. If
// the overwrite leaves a previously pending file unreferenced, its filename
// is returned so the caller can remove it from disk and release its disk usage.
func (s *Store) PutObject(accessKeyID, bucket, name string, contentMD5 [16]byte, meta map[string]string, length int64, fileName *string) (orphanFile string, orphanSize int64, _ error) {
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		orphanFile, orphanSize, err = putObject(tx, bid, name, contentMD5, meta, length, 0, fileName, nil)
		return err
	})
	return orphanFile, orphanSize, err
}

// MarkObjectUploaded transitions a pending upload to an uploaded-but-not-yet-
// pinned object by setting sia_object_id and sia_object on the objects row and
// upserting a corresponding unpinned_objects row keyed by sia_object_id. The
// filename is intentionally kept set so the file on disk remains available as
// a backup until the pin completes. When several objects share the same
// sia_object_id (e.g. dedup or a CopyObject of a not-yet-pinned source) they
// share a single unpinned_objects row whose pin_before is the latest deadline
// seen. Returns ErrObjectNotFound if no pending object exists for the bucket
// and name or ErrObjectModified if the stored content MD5 does not match the
// provided contentMD5.
func (s *Store) MarkObjectUploaded(bucket, name string, contentMD5 [16]byte, sealed sdk.SealedObject, pinBefore time.Time) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}

		var storedMD5 [16]byte
		var filename *string
		var size int64
		err = tx.QueryRow(`
			SELECT content_md5, filename, size FROM objects
			WHERE bucket_id = $1 AND name = $2 AND sia_object_id IS NULL
		`, bid, name).Scan((*sqlMD5)(&storedMD5), &filename, &size)
		if errors.Is(err, sql.ErrNoRows) {
			return objects.ErrObjectNotFound
		} else if err != nil {
			return err
		} else if storedMD5 != contentMD5 {
			return objects.ErrObjectModified
		}

		// keep the filename set so the file on disk remains available as a
		// backup until the pin completes.
		if _, err := tx.Exec(`
			UPDATE objects
			SET sia_object_id = $1, sia_object = $2
			WHERE bucket_id = $3 AND name = $4 AND sia_object_id IS NULL
		`, sqlHash256(sealed.ID()), sqlSiaObject(sealed), bid, name); err != nil {
			return err
		}

		// the object transitioned from pending to uploaded.
		if filename != nil {
			if err := removePendingObject(tx, size); err != nil {
				return err
			}
		}
		if err := addUploadedObject(tx, size); err != nil {
			return err
		}

		// upsert the unpinned_objects row, counting a new unpinned object only
		// when a row is actually inserted; copies sharing a sia_object_id reuse
		// the existing row (and an upsert reports a changed row either way, so
		// detect the insert by checking for the row beforehand).
		var exists bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM unpinned_objects WHERE sia_object_id = $1)`, sqlHash256(sealed.ID())).Scan(&exists); err != nil {
			return err
		}
		if _, err := tx.Exec(`
			INSERT INTO unpinned_objects (sia_object_id, pin_before)
			VALUES ($1, $2)
			ON CONFLICT (sia_object_id) DO UPDATE
				SET pin_before = max(unpinned_objects.pin_before, excluded.pin_before)
		`, sqlHash256(sealed.ID()), sqlTime(pinBefore)); err != nil {
			return err
		}
		if !exists {
			return incrementStat(tx, statUnpinnedObjects, 1)
		}
		return nil
	})
}

// MarkObjectPinned completes the upload lifecycle for a Sia object that has
// been successfully pinned in the indexer: the unpinned_objects row is removed
// and filename is cleared on every objects row referencing the sia_object_id
// (e.g. copies share one pin row). Filenames that are no longer referenced by
// any objects row are returned for cleanup by the caller. If no
// unpinned_objects row exists the object was deleted while the pin was in
// flight, so the sia_object_id is recorded in orphaned_objects for the orphan
// loop to unpin; the pin must not be silently dropped since inserting into
// orphaned_objects is the only mechanism that unpins objects.
func (s *Store) MarkObjectPinned(siaObjectID types.Hash256) (orphans []objects.OrphanedFile, _ error) {
	err := s.transaction(func(tx *txn) error {
		orphans = nil // reset per transaction attempt

		res, err := tx.Exec(`DELETE FROM unpinned_objects WHERE sia_object_id = $1`, sqlHash256(siaObjectID))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			// no unpinned_objects row means the object was deleted while the
			// pin was in flight; mark the sia_object_id as orphaned so the
			// orphan loop can unpin it.
			return insertOrphan(tx, siaObjectID)
		}

		// the object is now pinned; it stays counted as uploaded but is no
		// longer unpinned.
		if err := incrementStat(tx, statUnpinnedObjects, -1); err != nil {
			return err
		}

		// snapshot the filenames currently referenced by this sia_object_id
		// so we can check their orphan status after the clear; copies share
		// the same filename so dedup before we collect.
		seen := make(map[string]int64)
		rows, err := tx.Query(`
			SELECT filename, size FROM objects
			WHERE sia_object_id = $1 AND filename IS NOT NULL
		`, sqlHash256(siaObjectID))
		if err != nil {
			return err
		}
		for rows.Next() {
			var fn string
			var size int64
			if err := rows.Scan(&fn, &size); err != nil {
				rows.Close()
				return err
			}
			if _, ok := seen[fn]; !ok {
				seen[fn] = size
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		if _, err := tx.Exec(`UPDATE objects SET filename = NULL WHERE sia_object_id = $1`, sqlHash256(siaObjectID)); err != nil {
			return err
		}

		for fn, size := range seen {
			name := fn
			orphan, orphanSize, err := newOrphanedFile(tx, &name, size)
			if err != nil {
				return err
			}
			if orphan != "" {
				orphans = append(orphans, objects.OrphanedFile{Filename: orphan, Size: orphanSize})
			}
		}
		return nil
	})
	return orphans, err
}

// ScheduleObjectForReupload reverts every objects row referencing the given
// sia_object_id back to the pending-upload state and removes the
// unpinned_objects row. The old sia_object_id is recorded in orphaned_objects:
// an earlier pin attempt may have succeeded in the indexer without
// MarkObjectPinned having committed, and the re-upload always produces a new
// id, so the old one is never referenced again. Returns ErrObjectNotFound if
// no unpinned_objects row exists for the sia_object_id.
func (s *Store) ScheduleObjectForReupload(siaObjectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`DELETE FROM unpinned_objects WHERE sia_object_id = $1`, sqlHash256(siaObjectID))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return objects.ErrObjectNotFound
		}
		if err := incrementStat(tx, statUnpinnedObjects, -1); err != nil {
			return err
		}

		// the rows about to revert are currently counted as uploaded; once
		// their sia_object_id is cleared they become pending again (their
		// backup files are still on disk).
		var count, totalSize int64
		if err := tx.QueryRow(`
			SELECT COUNT(*), COALESCE(SUM(size), 0) FROM objects WHERE sia_object_id = $1
		`, sqlHash256(siaObjectID)).Scan(&count, &totalSize); err != nil {
			return err
		}

		if _, err := tx.Exec(`
			UPDATE objects SET sia_object_id = NULL, sia_object = NULL
			WHERE sia_object_id = $1
		`, sqlHash256(siaObjectID)); err != nil {
			return err
		}

		if count > 0 {
			if err := incrementStat(tx, statUploadedObjects, -count); err != nil {
				return err
			}
			if err := incrementStat(tx, statUploadedSize, -totalSize); err != nil {
				return err
			}
			if err := incrementStat(tx, statPendingObjects, count); err != nil {
				return err
			}
			if err := incrementStat(tx, statPendingSize, totalSize); err != nil {
				return err
			}
		}

		return insertOrphan(tx, siaObjectID)
	})
}

// ObjectsForPinning returns up to limit unpinned objects whose next_attempt_at
// is at or before now, in ascending next_attempt_at order. Rows whose
// sia_object_id is no longer referenced by any objects row are skipped — the
// pin loop is not responsible for cleaning those up.
func (s *Store) ObjectsForPinning(now time.Time, limit int) ([]objects.UnpinnedObject, error) {
	var result []objects.UnpinnedObject
	err := s.transaction(func(tx *txn) error {
		result = result[:0]
		rows, err := tx.Query(`
			SELECT u.sia_object_id,
			       (SELECT sia_object FROM objects WHERE sia_object_id = u.sia_object_id LIMIT 1),
			       u.pin_before
			FROM unpinned_objects u
			WHERE u.next_attempt_at <= $1
			ORDER BY u.next_attempt_at
			LIMIT $2
		`, sqlTime(now), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var uo objects.UnpinnedObject
			var id sqlHash256
			var sealed *sqlSiaObject
			if err := rows.Scan(&id, &sealed, (*sqlTime)(&uo.PinBefore)); err != nil {
				return err
			}
			if sealed == nil {
				// no objects row references this sia_object_id anymore;
				// insertOrphan drops the unpinned_objects row atomically
				// with the last reference, so this shouldn't happen. Skip
				// the row defensively rather than pinning it.
				continue
			}
			uo.SiaObject = objects.SiaObject{
				ID:     types.Hash256(id),
				Sealed: sdk.SealedObject(*sealed),
			}
			result = append(result, uo)
		}
		return rows.Err()
	})
	return result, err
}

// NextPinningAttempt returns the earliest next_attempt_at across all
// unpinned_objects rows. The boolean is false when the table is empty.
func (s *Store) NextPinningAttempt() (next time.Time, ok bool, err error) {
	err = s.transaction(func(tx *txn) error {
		var ts sql.Null[sqlTime]
		if err := tx.QueryRow(`SELECT MIN(next_attempt_at) FROM unpinned_objects`).Scan(&ts); err != nil {
			return err
		}
		next = time.Time(ts.V)
		ok = ts.Valid
		return nil
	})
	return
}

// RescheduleUnpinnedObject updates next_attempt_at for the unpinned object
// identified by sia_object_id. Returns ErrObjectNotFound if no row exists.
func (s *Store) RescheduleUnpinnedObject(siaObjectID types.Hash256, nextAttemptAt time.Time) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`
			UPDATE unpinned_objects SET next_attempt_at = $1
			WHERE sia_object_id = $2
		`, sqlTime(nextAttemptAt), sqlHash256(siaObjectID))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return objects.ErrObjectNotFound
		}
		return nil
	})
}

// UpdateSiaObjects batch updates object metadata in the database within a
// single transaction. It returns the number of rows that were updated.
func (s *Store) UpdateSiaObjects(siaObjects []objects.SiaObject) (updated int64, err error) {
	err = s.transaction(func(tx *txn) error {
		updated = 0 // reset per transaction attempt

		stmt, err := tx.Prepare(`
			UPDATE objects SET sia_object = $1
			WHERE sia_object_id = $2
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, obj := range siaObjects {
			res, err := stmt.Exec(sqlSiaObject(obj.Sealed), sqlHash256(obj.ID))
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			updated += n
		}
		return nil
	})
	return
}

// CopyObject atomically reads the source object and writes it to the
// destination within a single transaction, applying metadata according to the
// replace flag. Returns the copied object metadata, and if the copy overwrote
// a previously pending object whose file is no longer referenced, its filename
// so the caller can remove it from disk.
func (s *Store) CopyObject(accessKeyID, srcBucket, srcName, dstBucket, dstName string, meta map[string]string, replace bool) (result *objects.Object, orphanFile string, orphanSize int64, err error) {
	var obj objects.Object
	err = s.transaction(func(tx *txn) error {
		obj = objects.Object{} // reset per transaction attempt

		srcBid, err := bucketID(tx, accessKeyID, srcBucket)
		if err != nil {
			return err
		}

		if err := getObject(tx, &obj, srcBid, srcName, nil); err != nil {
			return err
		}

		if replace {
			obj.Meta = meta
		} else {
			maps.Copy(obj.Meta, meta)
		}

		dstBid := srcBid
		if dstBucket != srcBucket {
			dstBid, err = bucketID(tx, accessKeyID, dstBucket)
			if err != nil {
				return err
			}
		}

		// self-copy only changes metadata: update the row in place so
		// object_parts is preserved and no orphaning is needed.
		if srcBid == dstBid && srcName == dstName {
			if obj.Meta == nil {
				obj.Meta = make(map[string]string)
			}
			_, err := tx.Exec(`
				UPDATE objects SET metadata = $1, updated_at = $2
				WHERE bucket_id = $3 AND name = $4
			`, sqlMetaJSON(obj.Meta), sqlTime(time.Now()), dstBid, dstName)
			return err
		}

		orphanFile, orphanSize, err = putObject(tx, dstBid, dstName, obj.ContentMD5, obj.Meta, obj.Length, obj.PartsCount, obj.FileName, obj.SiaObject)
		if err != nil {
			return err
		}

		if obj.PartsCount > 0 {
			_, err = tx.Exec(`
				INSERT INTO object_parts (bucket_id, name, part_number, filename, content_md5, content_length, offset)
				SELECT $1, $2, part_number, filename, content_md5, content_length, offset
				FROM object_parts
				WHERE bucket_id = $3 AND name = $4
			`, dstBid, dstName, srcBid, srcName)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", 0, s3errs.ErrNoSuchKey
	}
	if err != nil {
		return nil, "", 0, err
	}
	return &obj, orphanFile, orphanSize, nil
}

// ObjectPartsByName returns the parts for a completed multipart object. It is
// intended for internal callers (the upload loop and downstream of an
// ownership-scoped GetObject) and does not perform an access check.
func (s *Store) ObjectPartsByName(bucket, name string) ([]objects.Part, error) {
	var parts []objects.Part
	err := s.transaction(func(tx *txn) error {
		parts = parts[:0] // reuse same slice if transaction retries
		bid, err := bucketIDByName(tx, bucket)
		if err != nil {
			return err
		}
		rows, err := tx.Query(`
			SELECT part_number, filename, content_length, content_md5
			FROM object_parts
			WHERE bucket_id = $1 AND name = $2
			ORDER BY part_number
		`, bid, name)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var p objects.Part
			if err := rows.Scan(&p.PartNumber, &p.Filename, &p.Size, (*sqlMD5)(&p.ContentMD5)); err != nil {
				return err
			}
			parts = append(parts, p)
		}
		return rows.Err()
	})
	return parts, err
}

// ObjectsCursor returns the cursor for resuming object event syncing.
func (s *Store) ObjectsCursor() (cursor slabs.Cursor, err error) {
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT last_sync_at, last_sync_key FROM global_settings LIMIT 1`).
			Scan((*sqlTime)(&cursor.After), (*sqlHash256)(&cursor.Key))
	})
	return
}

// SetObjectsCursor updates the cursor for resuming object event syncing.
func (s *Store) SetObjectsCursor(cursor slabs.Cursor) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("UPDATE global_settings SET last_sync_at = $1, last_sync_key = $2", sqlTime(cursor.After), sqlHash256(cursor.Key))
		return err
	})
}

// AllFilenames returns all filenames from the objects table and in-progress
// multipart uploads.
func (s *Store) AllFilenames() (filenames []string, err error) {
	err = s.transaction(func(tx *txn) error {
		filenames = filenames[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT filename FROM objects WHERE filename IS NOT NULL
			UNION ALL
			SELECT LOWER(HEX(upload_id)) FROM multipart_uploads`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			filenames = append(filenames, name)
		}
		return rows.Err()
	})
	return
}

// OrphanedObjects returns up to limit object IDs from the orphaned_objects table.
func (s *Store) OrphanedObjects(limit int) (ids []types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		ids = ids[:0] // reuse same slice if transaction retries
		rows, err := tx.Query("SELECT sia_object_id FROM orphaned_objects LIMIT $1", limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id types.Hash256
			if err := rows.Scan((*sqlHash256)(&id)); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	})
	return
}

// RemoveOrphanedObject removes an object ID from the orphaned_objects table.
func (s *Store) RemoveOrphanedObject(objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM orphaned_objects WHERE sia_object_id = $1", sqlHash256(objectID))
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n > 0 {
			return incrementStat(tx, statOrphanedObjects, -1)
		}
		return nil
	})
}

// ObjectsForUpload returns all objects stored on disk, ordered by size
// descending for greedy best-fit slab filling.
func (s *Store) ObjectsForUpload() ([]objects.ObjectForUpload, error) {
	var objs []objects.ObjectForUpload
	if err := s.transaction(func(tx *txn) error {
		objs = objs[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT b.name, o.name, o.filename, o.content_md5, o.size, o.parts_count > 0 AS has_parts
			FROM objects o
			JOIN buckets b ON b.id = o.bucket_id
			WHERE o.filename IS NOT NULL AND o.sia_object_id IS NULL
			ORDER BY o.size DESC
		`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var obj objects.ObjectForUpload
			if err := rows.Scan(&obj.Bucket, &obj.Name, &obj.Filename, (*sqlMD5)(&obj.ContentMD5), &obj.Length, &obj.Multipart); err != nil {
				return err
			}
			objs = append(objs, obj)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return objs, nil
}

// putObject replaces the object row at (bid, name) with the given values. Any
// prior row is deleted first. If the prior row's sia_object_id is no longer
// referenced, it is added to orphaned_objects. If the prior row's filename is
// no longer referenced, its filename is returned for cleanup.
func putObject(tx *txn, bid int64, name string, contentMD5 [16]byte, meta map[string]string, length int64, partsCount int32, fileName *string, siaObject *objects.SiaObject) (string, int64, error) {
	if meta == nil {
		meta = make(map[string]string) // force '{}' instead of 'null' in JSON
	}

	oldID, oldFilename, oldSize, err := deleteObject(tx, bid, name)
	if err != nil {
		return "", 0, err
	}

	var id *sqlHash256
	var sealed *sqlSiaObject
	if siaObject != nil {
		id = (*sqlHash256)(&siaObject.ID)
		sealed = (*sqlSiaObject)(&siaObject.Sealed)
	}

	_, err = tx.Exec(`
		INSERT INTO objects (bucket_id, name, sia_object_id, content_md5, metadata, size, parts_count, updated_at, filename, sia_object)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, bid, name, id, sqlMD5(contentMD5),
		sqlMetaJSON(meta), length, partsCount, sqlTime(time.Now()),
		fileName, sealed)
	if err != nil {
		return "", 0, err
	}

	// an object with a sia_object_id counts as uploaded even when it still
	// keeps a filename as a backup (e.g. a copy of a not-yet-pinned object);
	// only a filename without a sia_object_id is pending.
	if fileName != nil && siaObject == nil {
		if err := addPendingObject(tx, length); err != nil {
			return "", 0, err
		}
	}
	if siaObject != nil {
		if err := addUploadedObject(tx, length); err != nil {
			return "", 0, err
		}
	}

	if oldID != nil && (siaObject == nil || *oldID != siaObject.ID) {
		if err := insertOrphan(tx, *oldID); err != nil {
			return "", 0, err
		}
	}

	return newOrphanedFile(tx, oldFilename, oldSize)
}

// newOrphanedFile returns the filename and size if the given filename is
// non-nil and no longer referenced by any row in the objects table.
func newOrphanedFile(tx *txn, filename *string, size int64) (string, int64, error) {
	if filename == nil {
		return "", 0, nil
	}
	var shared bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM objects WHERE filename = $1)`, *filename).Scan(&shared); err != nil {
		return "", 0, err
	}
	if shared {
		return "", 0, nil
	}
	return *filename, size, nil
}

// deleteObject deletes the row at (bid, name) and returns its sia_object_id,
// filename, and size. All zero when no row exists.
func deleteObject(tx *txn, bid int64, name string) (*types.Hash256, *string, int64, error) {
	var id sql.Null[sqlHash256]
	var filename *string
	var size int64
	err := tx.QueryRow(`
		DELETE FROM objects WHERE bucket_id = $1 AND name = $2
		RETURNING sia_object_id, filename, size
	`, bid, name).Scan(&id, &filename, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, 0, nil
	} else if err != nil {
		return nil, nil, 0, err
	}
	// an object is pending (filename, no sia_object_id), uploaded
	// (sia_object_id, possibly still keeping its filename as a backup until
	// pinned) or empty.
	if filename != nil && !id.Valid {
		if err := removePendingObject(tx, size); err != nil {
			return nil, nil, 0, err
		}
	}
	if id.Valid {
		if err := removeUploadedObject(tx, size); err != nil {
			return nil, nil, 0, err
		}
	}
	if !id.Valid {
		return nil, filename, size, nil
	}
	return (*types.Hash256)(&id.V), filename, size, nil
}

// insertOrphan finalizes deletion of objectID when no rows in the objects
// table still reference it: the pin queue entry is dropped and the id is
// recorded in orphaned_objects so the orphan loop unpins it. The orphan row
// is inserted even when the object was still queued for pinning — a pin may
// be in flight concurrently (or may already have succeeded without
// MarkObjectPinned having committed), and skipping the insert would leak
// that pin forever since the orphan loop is the only unpin mechanism. For
// data that was truly never pinned the unpin is a no-op: the orphan loop
// treats the indexer's "object not found" as success.
func insertOrphan(tx *txn, objectID types.Hash256) error {
	if objectID == (types.Hash256{}) {
		return nil // skip zero-value (empty objects)
	}
	var referenced bool
	if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM objects WHERE sia_object_id = $1)", sqlHash256(objectID)).Scan(&referenced); err != nil {
		return err
	}
	if referenced {
		return nil
	}
	// drop the pin queue entry; if it existed the object was still unpinned.
	res, err := tx.Exec(`DELETE FROM unpinned_objects WHERE sia_object_id = $1`, sqlHash256(objectID))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n > 0 {
		if err := incrementStat(tx, statUnpinnedObjects, -1); err != nil {
			return err
		}
	}
	res, err = tx.Exec("INSERT OR IGNORE INTO orphaned_objects (sia_object_id) VALUES ($1)", sqlHash256(objectID))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n > 0 {
		return incrementStat(tx, statOrphanedObjects, 1)
	}
	return nil
}

// ListObjects lists objects in the specified bucket, filtered by prefix and
// pagination settings.
func (s *Store) ListObjects(accessKeyID, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (result *s3.ObjectsListResult, err error) {
	result = s3.NewObjectsListResult(page.MaxKeys)
	if page.MaxKeys == 0 {
		return result, nil
	}

	// adjust marker if it falls inside a common prefix
	marker := page.Marker
	if marker != nil && *marker != "" {
		if adjustedKey, adjusted := adjustMarkerForCommonPrefix(prefix, *marker); adjusted {
			marker = &adjustedKey
		}
	}

	const maxObjsPerQuery = 100
	err = s.transaction(func(tx *txn) error {
		*result = *s3.NewObjectsListResult(page.MaxKeys) // reset per transaction attempt

		var bid int64
		bid, err = bucketID(tx, accessKeyID, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		innerMarker := marker

		list := func(marker *string) (string, string, error) {
			query := `SELECT o.name, o.content_md5, o.size, o.parts_count, o.updated_at
FROM objects o
WHERE o.bucket_id = ?`
			args := []any{bid}

			if marker != nil && *marker != "" {
				query += ` AND o.name > ?`
				args = append(args, *marker)
			}

			if prefix.HasPrefix {
				query += ` AND o.name >= ? AND o.name < ?`
				args = append(args, prefix.Prefix, prefix.Prefix+"\xFF")
			}

			query += ` ORDER BY o.name`
			query += `  LIMIT ?`
			args = append(args, maxObjsPerQuery)

			rows, err := tx.Query(query, args...)
			if err != nil {
				return "", "", fmt.Errorf("failed to query objects: %w", err)
			}
			defer rows.Close()

			var lastMatchedPart, lastObj string
			for rows.Next() && !result.IsTruncated && lastMatchedPart == "" {
				var obj objects.Object
				err = rows.Scan(
					&obj.Name,
					(*sqlMD5)(&obj.ContentMD5),
					&obj.Length,
					&obj.PartsCount,
					(*sqlTime)(&obj.LastModified),
				)
				if err != nil {
					return "", "", fmt.Errorf("failed to scan object: %w", err)
				}

				cp := prefix.CommonPrefix(obj.Name)
				if cp != "" {
					result.AddPrefix(cp)
					lastMatchedPart = cp
				} else {
					result.Add(&s3.Content{
						Key:          obj.Name,
						LastModified: s3.NewContentTime(obj.LastModified),
						ETag:         s3.FormatETag(obj.ContentMD5[:], int(obj.PartsCount)),
						Size:         int64(obj.Length),
						Owner:        nil,
					})
					lastObj = obj.Name
				}
			}
			if err := rows.Err(); err != nil {
				return "", "", fmt.Errorf("failed to get rows: %w", err)
			}
			return lastMatchedPart, lastObj, nil
		}

		for !result.IsTruncated {
			lastMatchedPart, lastObj, err := list(innerMarker)
			if err != nil {
				return err
			}
			if lastMatchedPart != "" {
				// if we get a common prefix, skip over the remainder of it
				lastMatchedPart += "\xFF"
				innerMarker = &lastMatchedPart
			} else if lastObj != "" {
				// otherwise continue getting the matching objects
				innerMarker = &lastObj
			} else {
				break
			}
		}

		if !result.IsTruncated {
			result.NextMarker = ""
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
