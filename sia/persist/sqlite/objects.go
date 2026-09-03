package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
)

// claimCurrentSeq returns the next sequence for (bid, name) and clears the
// existing current version. The caller must use the returned sequence for the
// row it marks current.
func claimCurrentSeq(tx *txn, bid int64, name string) (seq int64, err error) {
	err = tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) + 1 FROM objects WHERE bucket_id = $1 AND name = $2`, bid, name).Scan(&seq)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(`UPDATE objects SET is_latest = FALSE WHERE bucket_id = $1 AND name = $2 AND is_latest = TRUE`, bid, name)
	return seq, err
}

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

// DeleteObject deletes an object according to the bucket's versioning status,
// returning the wire-encoded version ID affected and whether a delete marker
// was involved. A non-nil objectID.VersionID permanently deletes that version
// ("" is the null version); otherwise an enabled bucket inserts a delete
// marker, a suspended bucket replaces the null version with a null delete
// marker, and an unversioned bucket deletes outright. A removed object's
// filename is returned for cleanup if no longer referenced.
func (s *Store) DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (versionID string, isDeleteMarker bool, orphan objects.OrphanedFile, _ error) {
	err := s.transaction(func(tx *txn) error {
		versionID, isDeleteMarker, orphan = "", false, objects.OrphanedFile{} // reset per attempt

		bid, status, err := bucketIDAndVersioning(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}

		// a specific version ID permanently deletes exactly that version,
		// independent of the bucket's versioning status.
		if objectID.VersionID != nil {
			version := *objectID.VersionID
			res, err := deleteSpecificVersion(tx, bid, objectID.Key, version, objectID)
			if errors.Is(err, sql.ErrNoRows) {
				versionID = s3.FormatVersion(version) // idempotent: report as deleted
				return nil
			} else if err != nil {
				return err
			}
			versionID, isDeleteMarker, orphan = res.reportVersionID, res.deleteMarker, res.orphanFile
			return nil
		}

		// otherwise apply a versioning-aware delete to the current object.
		res, err := deleteCurrentObject(tx, bid, objectID.Key, status, objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // nothing to delete; report success without a version
		} else if err != nil {
			return err
		}
		versionID, isDeleteMarker, orphan = res.reportVersionID, res.deleteMarker, res.orphanFile
		return nil
	})
	return versionID, isDeleteMarker, orphan, err
}

// scanObjectAttrs scans the attributes preconditions are matched against,
// reporting whether the row exists.
func scanObjectAttrs(r *row) (attrs s3.ObjectAttrs, found bool, _ error) {
	var contentMD5 [16]byte
	var partsCount int32
	err := r.Scan((*sqlMD5)(&contentMD5), &partsCount, &attrs.Size, (*sqlTime)(&attrs.LastModified), &attrs.IsDeleteMarker)
	if errors.Is(err, sql.ErrNoRows) {
		return s3.ObjectAttrs{}, false, nil
	} else if err != nil {
		return s3.ObjectAttrs{}, false, err
	}
	// a multipart object's ETag carries its part count
	attrs.ETag = s3.FormatETag(contentMD5[:], int(partsCount))
	return attrs, true, nil
}

// currentObjectAttrs reads the attributes of the current version of (bid, name),
// reporting whether the key has one.
func currentObjectAttrs(tx *txn, bid int64, name string) (s3.ObjectAttrs, bool, error) {
	return scanObjectAttrs(tx.QueryRow(`
		SELECT content_md5, parts_count, size, updated_at, is_delete_marker
		FROM objects
		WHERE bucket_id = $1 AND name = $2 AND is_latest = TRUE
	`, bid, name))
}

// versionObjectAttrs reads the attributes of the (bid, name, version) row,
// reporting whether it exists.
func versionObjectAttrs(tx *txn, bid int64, name, version string) (s3.ObjectAttrs, bool, error) {
	return scanObjectAttrs(tx.QueryRow(`
		SELECT content_md5, parts_count, size, updated_at, is_delete_marker
		FROM objects
		WHERE bucket_id = $1 AND name = $2 AND version_id = $3
	`, bid, name, version))
}

// checkObjectPreconditions validates objectID's preconditions against the
// current version of (bid, name), for deletes that create a delete marker
// without removing a row. sql.ErrNoRows means there is no object the
// preconditions apply to, so the caller leaves the key alone; a key with no
// versions and one whose current version is a delete marker are both no object.
func checkObjectPreconditions(tx *txn, bid int64, name string, objectID s3.ObjectID) error {
	if !objectID.HasPreconditions() {
		return nil
	}
	attrs, found, err := currentObjectAttrs(tx, bid, name)
	if err != nil {
		return err
	} else if !found {
		return sql.ErrNoRows
	} else if attrs.IsDeleteMarker && !objectID.HasSpecificPreconditions() {
		// the wildcard only asserts that an object is there, and none is; a
		// precondition naming a particular object is failed by CheckDelete
		// below.
		return sql.ErrNoRows
	}
	return objectID.CheckDelete(attrs)
}

// checkWritePreconditions evaluates the preconditions against the current
// version of name. It runs in the same transaction as the write so concurrent
// writers cannot both satisfy the same precondition.
func checkWritePreconditions(tx *txn, bid int64, name string, p s3.ObjectPreconditions) error {
	if !p.HasWritePreconditions() {
		return nil
	}
	attrs, found, err := currentObjectAttrs(tx, bid, name)
	if err != nil {
		return err
	} else if !found {
		return p.CheckWrite(nil)
	}
	return p.CheckWrite(&attrs)
}

// GetObject retrieves an object. An unspecified version returns the current
// version (ErrNoSuchKey if the key has no versions); a specified version returns
// that version (ErrNoSuchVersion if absent). The result may be a delete marker.
//
// A nil accessKeyID is an anonymous read, which only succeeds when the bucket's
// policy grants action. The caller picks action so that a read resolving a
// version internally stays authorized by what the original request asked for.
func (s *Store) GetObject(accessKeyID *string, bucket, name string, version s3.VersionRequest, partNumber *int32, action s3.PolicyActions) (*objects.Object, error) {
	var obj objects.Object
	if err := s.transaction(func(tx *txn) error {
		b, err := bucketForRead(tx, accessKeyID, bucket, action)
		if err != nil {
			return err
		}
		if err := getObject(tx, &obj, b.id, name, version, partNumber); err != nil {
			return err
		}
		obj.Versioned = b.versioning != ""
		return nil
	}); errors.Is(err, sql.ErrNoRows) {
		if version.Specified {
			return nil, s3errs.ErrNoSuchVersion
		}
		return nil, s3errs.ErrNoSuchKey
	} else if err != nil {
		return nil, err
	}

	return &obj, nil
}

// getObject resolves the requested version of (bid, name) and populates obj. An
// unspecified version uses the current version (greatest seq); a specified
// version uses the exact version, returning sql.ErrNoRows if absent. The version
// may be a delete marker.
func getObject(tx *txn, obj *objects.Object, bid int64, name string, version s3.VersionRequest, partNumber *int32) error {
	// resolve the target version and read its parts count and delete-marker flag
	var versionID string
	if !version.Specified {
		err := tx.QueryRow(`
			SELECT version_id, parts_count, is_delete_marker
			FROM objects
			WHERE bucket_id = $1 AND name = $2 AND is_latest = TRUE
		`, bid, name).Scan(&versionID, &obj.PartsCount, &obj.IsDeleteMarker)
		if err != nil {
			return err
		}
	} else {
		versionID = version.ID
		err := tx.QueryRow(`
			SELECT parts_count, is_delete_marker
			FROM objects
			WHERE bucket_id = $1 AND name = $2 AND version_id = $3
		`, bid, name, versionID).Scan(&obj.PartsCount, &obj.IsDeleteMarker)
		if err != nil {
			return err
		}
	}
	obj.VersionID = versionID

	// return full object if no part specified, or this is a delete marker
	if partNumber == nil || obj.PartsCount == 0 || obj.IsDeleteMarker {
		if !obj.IsDeleteMarker && obj.PartsCount == 0 && partNumber != nil && *partNumber != 1 {
			return s3errs.ErrInvalidPart
		}
		var objectID sql.Null[sqlHash256]
		err := tx.QueryRow(`
			SELECT filename, sia_object_id, metadata, updated_at, size, content_md5
			FROM objects
			WHERE bucket_id = $1 AND name = $2 AND version_id = $3
		`, bid, name, versionID).Scan(&obj.FileName, &objectID, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Length, (*sqlMD5)(&obj.ContentMD5))
		if err != nil {
			return err
		}
		obj.Size = obj.Length
		return attachSiaObject(tx, obj, objectID)
	}

	// return error if part number is invalid
	if *partNumber > int32(obj.PartsCount) {
		return s3errs.ErrInvalidPart
	}

	// part specified, return part info. the MD5 read is the object's, not the
	// part's, because ContentMD5 and PartsCount are the object's ETag inputs
	var partObjID sql.Null[sqlHash256]
	err := tx.QueryRow(`
		SELECT o.filename, o.sia_object_id, o.metadata, o.updated_at, o.size, p.offset, p.content_length, o.content_md5
		FROM object_parts p
		JOIN objects o ON o.bucket_id = p.bucket_id AND o.name = p.name AND o.version_id = p.version_id
		WHERE o.bucket_id = $1 AND o.name = $2 AND o.version_id = $3 AND p.part_number = $4
	`, bid, name, versionID, *partNumber).Scan(&obj.FileName, &partObjID, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Size, &obj.Offset, &obj.Length, (*sqlMD5)(&obj.ContentMD5))
	if err != nil {
		return err
	}
	return attachSiaObject(tx, obj, partObjID)
}

// attachSiaObject loads the sealed object referenced by objectID from the
// normalized sia object tables and attaches it to obj. A null objectID leaves
// obj untouched.
func attachSiaObject(tx *txn, obj *objects.Object, objectID sql.Null[sqlHash256]) error {
	if !objectID.Valid {
		return nil
	}
	sealed, err := siaObject(tx, types.Hash256(objectID.V))
	if err != nil {
		return fmt.Errorf("failed to load sia object %v: %w", types.Hash256(objectID.V), err)
	}
	obj.SiaObject = &objects.SiaObject{
		ID:     types.Hash256(objectID.V),
		Sealed: sealed,
	}
	return nil
}

// PutObject stores the object and returns the wire-encoded version ID to
// report ("" on a suspended or unversioned bucket, since neither reports a
// version). An enabled bucket creates a new version; otherwise the null version
// is overwritten, orphaning any prior object ID or pending file that is no
// longer referenced (the latter returned so the caller can remove it from disk).
func (s *Store) PutObject(accessKeyID, bucket, name string, opts objects.PutOptions) (versionID string, orphan objects.OrphanedFile, _ error) {
	err := s.transaction(func(tx *txn) error {
		versionID, orphan = "", objects.OrphanedFile{} // reset per attempt
		bid, status, err := bucketIDAndVersioning(tx, accessKeyID, bucket)
		if err != nil {
			return err
		}
		if err := checkWritePreconditions(tx, bid, name, opts.Preconditions); err != nil {
			return err
		}
		res, err := putObject(tx, bid, name, status, opts.ContentMD5, opts.Meta, opts.Length, 0, opts.FileName, nil)
		versionID, orphan = res.reportVersionID, res.orphanFile
		return err
	})
	return versionID, orphan, err
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
func (s *Store) MarkObjectUploaded(bucket, name, versionID string, contentMD5 [16]byte, sealed sdk.SealedObject, pinBefore time.Time) error {
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
			WHERE bucket_id = $1 AND name = $2 AND version_id = $3 AND sia_object_id IS NULL AND filename IS NOT NULL
		`, bid, name, versionID).Scan((*sqlMD5)(&storedMD5), &filename, &size)
		if errors.Is(err, sql.ErrNoRows) {
			return objects.ErrObjectNotFound
		} else if err != nil {
			return err
		} else if storedMD5 != contentMD5 {
			return objects.ErrObjectModified
		}

		// store the sealed object before referencing it from the objects row
		if err := upsertSiaObject(tx, sealed); err != nil {
			return err
		}

		// keep the filename set so the file on disk remains available as a
		// backup until the pin completes.
		if _, err := tx.Exec(`
			UPDATE objects
			SET sia_object_id = $1
			WHERE bucket_id = $2 AND name = $3 AND version_id = $4 AND sia_object_id IS NULL AND filename IS NOT NULL
		`, sqlHash256(sealed.ID()), bid, name, versionID); err != nil {
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
			orphan, err := newOrphanedFile(tx, &name, size)
			if err != nil {
				return err
			}
			if orphan.Filename != "" {
				orphans = append(orphans, orphan)
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
			UPDATE objects SET sia_object_id = NULL
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
			SELECT sia_object_id, pin_before
			FROM unpinned_objects
			WHERE next_attempt_at <= $1
			ORDER BY next_attempt_at
			LIMIT $2
		`, sqlTime(now), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		var candidates []objects.UnpinnedObject
		for rows.Next() {
			var uo objects.UnpinnedObject
			if err := rows.Scan((*sqlHash256)(&uo.SiaObject.ID), (*sqlTime)(&uo.PinBefore)); err != nil {
				return err
			}
			candidates = append(candidates, uo)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		for _, uo := range candidates {
			sealed, err := siaObject(tx, uo.SiaObject.ID)
			if errors.Is(err, sql.ErrNoRows) {
				// the sealed object is gone; insertOrphan drops the
				// unpinned_objects row atomically with the last reference,
				// so this shouldn't happen. Skip the row defensively rather
				// than pinning it.
				continue
			} else if err != nil {
				return err
			}
			uo.SiaObject.Sealed = sealed
			result = append(result, uo)
		}
		return nil
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

// UpdateSiaObjects batch updates sealed object metadata in the database within
// a single transaction. It returns the number of sealed objects that were
// updated; objects that are no longer tracked are skipped.
func (s *Store) UpdateSiaObjects(siaObjects []objects.SiaObject) (updated int64, err error) {
	err = s.transaction(func(tx *txn) error {
		updated = 0 // reset per transaction attempt

		for _, obj := range siaObjects {
			var exists bool
			if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sia_objects WHERE id = $1)`, sqlHash256(obj.ID)).Scan(&exists); err != nil {
				return err
			} else if !exists {
				continue
			}
			if err := upsertSiaObject(tx, obj.Sealed); err != nil {
				return err
			}
			updated++
		}
		return nil
	})
	return
}

// CopyObject atomically reads the source object and writes it to the
// destination within a single transaction, applying opts.Meta per opts.Replace.
// The result carries the wire-encoded version IDs of the new copy and the
// source copied; an orphaned pending file is returned for the caller to remove.
func (s *Store) CopyObject(accessKeyID, srcBucket, srcName string, srcVersion s3.VersionRequest, dstBucket, dstName string, opts s3.CopyObjectOptions) (_ *s3.CopyObjectResult, orphan objects.OrphanedFile, err error) {
	var obj objects.Object
	var versionID, srcVersionWire string
	err = s.transaction(func(tx *txn) error {
		obj = objects.Object{} // reset per transaction attempt
		versionID, srcVersionWire, orphan = "", "", objects.OrphanedFile{}

		// the destination is written, so it must be owned by the caller. It is
		// resolved first so the same-bucket shortcut below can never inherit a
		// bucket the caller only has read access to.
		dstBid, dstStatus, err := bucketIDAndVersioning(tx, accessKeyID, dstBucket)
		if err != nil {
			return err
		}

		// the source is only read, so a policy granting that read is enough,
		// matching UploadPartCopy
		srcBid, srcStatus := dstBid, dstStatus
		if srcBucket != dstBucket {
			src, err := bucketForRead(tx, &accessKeyID, srcBucket, s3.ReadAction(srcVersion))
			if err != nil {
				return err
			}
			srcBid, srcStatus = src.id, src.versioning
		}

		// copy the requested source version, or the current version when
		// unspecified
		if err := getObject(tx, &obj, srcBid, srcName, srcVersion, nil); err != nil {
			return err
		}
		if err := opts.SourcePreconditions.CheckCopySource(obj.Attrs(), srcVersion); err != nil {
			return err
		}
		srcInternalVersion := obj.VersionID
		if srcStatus != "" {
			srcVersionWire = s3.FormatVersion(srcInternalVersion)
		}

		if opts.Replace {
			obj.Meta = opts.Meta
		} else {
			maps.Copy(obj.Meta, opts.Meta)
		}

		if err := checkWritePreconditions(tx, dstBid, dstName, opts.DestinationPreconditions); err != nil {
			return err
		}

		// a self-copy onto the same null version rewrites the row in place to
		// preserve its object_parts and avoid orphaning. Refresh seq too so a
		// suspended-bucket restore of versionId=null makes it current again.
		if srcBid == dstBid && srcName == dstName && dstStatus != s3.VersioningStatusEnabled && srcInternalVersion == nullVersion {
			if obj.Meta == nil {
				obj.Meta = make(map[string]string)
			}
			seq, err := claimCurrentSeq(tx, dstBid, dstName)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`
				UPDATE objects SET seq = $1, metadata = $2, updated_at = $3, is_latest = TRUE
				WHERE bucket_id = $4 AND name = $5 AND version_id = $6
			`, seq, sqlMetaJSON(obj.Meta), sqlTime(time.Now()), dstBid, dstName, srcInternalVersion)
			obj.VersionID = srcInternalVersion
			versionID = reportedWriteVersion(dstStatus, srcInternalVersion)
			return err
		}

		res, err := putObject(tx, dstBid, dstName, dstStatus, obj.ContentMD5, obj.Meta, obj.Length, obj.PartsCount, obj.FileName, obj.SiaObject)
		if err != nil {
			return err
		}
		orphan = res.orphanFile
		dstVersion := res.dbVersionID

		if obj.PartsCount > 0 {
			_, err = tx.Exec(`
				INSERT INTO object_parts (bucket_id, name, version_id, part_number, filename, content_md5, content_length, offset)
				SELECT $1, $2, $3, part_number, filename, content_md5, content_length, offset
				FROM object_parts
				WHERE bucket_id = $4 AND name = $5 AND version_id = $6
			`, dstBid, dstName, dstVersion, srcBid, srcName, srcInternalVersion)
			if err != nil {
				return err
			}
		}
		obj.VersionID = dstVersion
		versionID = res.reportVersionID
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		// a missing source maps to ErrNoSuchVersion when a specific version was
		// requested, otherwise ErrNoSuchKey.
		if srcVersion.Specified {
			return nil, objects.OrphanedFile{}, s3errs.ErrNoSuchVersion
		}
		return nil, objects.OrphanedFile{}, s3errs.ErrNoSuchKey
	}
	if err != nil {
		return nil, objects.OrphanedFile{}, err
	}
	return &s3.CopyObjectResult{
		ContentMD5:      obj.ContentMD5,
		LastModified:    obj.LastModified,
		VersionID:       versionID,
		SourceVersionID: srcVersionWire,
		PartsCount:      obj.PartsCount,
	}, orphan, nil
}

// ObjectPartsByName returns the parts for a completed multipart object. It is
// intended for internal callers (the upload loop and downstream of an
// ownership-scoped GetObject) and does not perform an access check.
func (s *Store) ObjectPartsByName(bucket, name, versionID string) ([]objects.Part, error) {
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
			WHERE bucket_id = $1 AND name = $2 AND version_id = $3
			ORDER BY part_number
		`, bid, name, versionID)
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
			SELECT b.name, o.name, o.version_id, o.filename, o.content_md5, o.size, o.parts_count > 0 AS has_parts
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
			if err := rows.Scan(&obj.Bucket, &obj.Name, &obj.VersionID, &obj.Filename, (*sqlMD5)(&obj.ContentMD5), &obj.Length, &obj.Multipart); err != nil {
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

// newOrphanedFile returns the orphaned file if the given filename is non-nil
// and no longer referenced by any row in the objects table. The zero value
// reports that nothing was orphaned.
func newOrphanedFile(tx *txn, filename *string, size int64) (objects.OrphanedFile, error) {
	if filename == nil {
		return objects.OrphanedFile{}, nil
	}
	var shared bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM objects WHERE filename = $1)`, *filename).Scan(&shared); err != nil {
		return objects.OrphanedFile{}, err
	}
	if shared {
		return objects.OrphanedFile{}, nil
	}
	return objects.OrphanedFile{Filename: *filename, Size: size}, nil
}

// deletedRow holds the columns of a deleted object row that the caller needs to
// orphan the row's backing data and to report the delete.
type deletedRow struct {
	siaObjectID    *types.Hash256
	filename       *string
	size           int64
	isDeleteMarker bool
}

// deleteObject deletes the row at (bid, name, versionID), decrementing the
// pending/uploaded disk-usage stats for its backing data, and returns the
// deleted row. found is false when no such row exists (the returned row is
// then the zero value). It does not orphan the row's data; callers pass the
// returned row to orphanDeleted for that.
func deleteObject(tx *txn, bid int64, name string, version string) (row deletedRow, found bool, _ error) {
	var id sql.Null[sqlHash256]
	var wasLatest bool
	err := tx.QueryRow(`
		DELETE FROM objects WHERE bucket_id = $1 AND name = $2 AND version_id = $3
		RETURNING sia_object_id, filename, size, is_delete_marker, is_latest
	`, bid, name, version).Scan(&id, &row.filename, &row.size, &row.isDeleteMarker, &wasLatest)
	if errors.Is(err, sql.ErrNoRows) {
		return deletedRow{}, false, nil
	} else if err != nil {
		return deletedRow{}, false, err
	}
	// removing the current version promotes the next-highest seq to current
	// (a no-op when no versions remain).
	if wasLatest {
		if _, err := tx.Exec(`
			UPDATE objects SET is_latest = TRUE
			WHERE bucket_id = $1 AND name = $2
			  AND seq = (SELECT MAX(seq) FROM objects WHERE bucket_id = $3 AND name = $4)`,
			bid, name, bid, name); err != nil {
			return deletedRow{}, false, err
		}
	}
	// an object is pending (filename, no sia_object_id), uploaded
	// (sia_object_id, possibly still keeping its filename as a backup until
	// pinned) or empty. Delete markers carry no data.
	if row.filename != nil && !id.Valid {
		if err := removePendingObject(tx, row.size); err != nil {
			return deletedRow{}, false, err
		}
	}
	if id.Valid {
		if err := removeUploadedObject(tx, row.size); err != nil {
			return deletedRow{}, false, err
		}
		row.siaObjectID = (*types.Hash256)(&id.V)
	}
	return row, true, nil
}

// orphanDeleted records the backing data of a deleted row as orphaned: its Sia
// object (when no other row still references it) and its on-disk upload file
// (when no longer referenced). The returned OrphanedFile is the zero value when
// nothing was orphaned.
func orphanDeleted(tx *txn, row deletedRow) (objects.OrphanedFile, error) {
	if row.siaObjectID != nil {
		if err := insertOrphan(tx, *row.siaObjectID); err != nil {
			return objects.OrphanedFile{}, err
		}
	}
	return newOrphanedFile(tx, row.filename, row.size)
}

// insertOrphan finalizes deletion of objectID when no rows in the objects
// table still reference it: the normalized sia object rows are removed, the
// pin queue entry is dropped and the id is recorded in orphaned_objects so
// the orphan loop unpins it. The orphan row
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
	// the last objects-row reference is gone; drop the sealed object along
	// with its slices and any slabs referenced only by it.
	if err := deleteSiaObject(tx, objectID); err != nil {
		return err
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

// listObjectsPage runs one bounded page query over the current, non-delete-marker
// version of each key, folding rows into result until the page fills or a common
// prefix is rolled up. It returns the marker to resume from and whether more
// pages remain.
func listObjectsPage(tx *txn, bid int64, prefix s3.Prefix, marker string, limit int, result *s3.ObjectsListResult) (next string, more bool, _ error) {
	// only the current version of each key is listed, and keys whose current
	// version is a delete marker are hidden.
	query := `SELECT o.name, o.content_md5, o.size, o.parts_count, o.updated_at
FROM objects o
WHERE o.bucket_id = ?
  AND o.is_latest = TRUE
  AND o.is_delete_marker = FALSE`
	args := []any{bid}

	if marker != "" {
		query += ` AND o.name > ?`
		args = append(args, marker)
	}

	if prefix.HasPrefix {
		query += ` AND o.name >= ? AND o.name < ?`
		args = append(args, prefix.Prefix, prefix.Prefix+"\xFF")
	}

	query += ` ORDER BY o.name`
	query += `  LIMIT ?`
	args = append(args, limit)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return "", false, fmt.Errorf("failed to query objects: %w", err)
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
			return "", false, fmt.Errorf("failed to scan object: %w", err)
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
		return "", false, fmt.Errorf("failed to get rows: %w", err)
	}

	switch {
	case result.IsTruncated:
		// page filled; stop
		return "", false, nil
	case lastMatchedPart != "":
		// resume after the rolled-up common prefix
		return lastMatchedPart + "\xFF", true, nil
	case lastObj != "":
		// otherwise continue getting the matching objects
		return lastObj, true, nil
	default:
		// nothing more to list
		return "", false, nil
	}
}

// ListObjects lists objects in the specified bucket, filtered by prefix and
// pagination settings.
func (s *Store) ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (result *s3.ObjectsListResult, err error) {
	result = s3.NewObjectsListResult(page.MaxKeys)

	// adjust marker if it falls inside a common prefix
	var marker string
	if page.Marker != nil {
		marker = *page.Marker
	}
	if adjustedKey, adjusted := adjustMarkerForCommonPrefix(prefix, marker); adjusted {
		marker = adjustedKey
	}

	const maxObjsPerQuery = 100
	err = s.transaction(func(tx *txn) error {
		*result = *s3.NewObjectsListResult(page.MaxKeys) // reset per transaction attempt

		b, err := bucketForRead(tx, accessKeyID, bucket, s3.ActionListBucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}
		bid := b.id

		// a caller that asked for no keys still has to be allowed to list
		if page.MaxKeys == 0 {
			return nil
		}

		innerMarker := marker
		for {
			next, more, err := listObjectsPage(tx, bid, prefix, innerMarker, maxObjsPerQuery, result)
			if err != nil {
				return err
			}
			if !more {
				break
			}
			innerMarker = next
		}

		if !result.IsTruncated {
			result.NextMarker = ""
		}
		if page.FetchOwner != nil && *page.FetchOwner {
			owner := b.userInfo()
			for i := range result.Contents {
				result.Contents[i].Owner = owner
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// versionMarker is the pagination cursor for one page query in
// ListObjectVersions. seq is the exclusive lower bound on the marker key's own
// versions: rows of key with a smaller seq are listed. 0 (the zero value) skips
// the key entirely since seqs start at 1, a real seq resumes mid-key, and
// math.MaxInt64 lists the whole key from its newest version.
type versionMarker struct {
	key string
	seq int64
}

// resolveVersionCursor translates the request's (key-marker, version-id-marker)
// pair into the internal page cursor. The version-id-marker is resolved to its
// row's seq so paging can resume mid-key; a key-marker that lands inside a
// common prefix is advanced past the whole group.
func resolveVersionCursor(tx *txn, bid int64, prefix s3.Prefix, page s3.ListObjectVersionsPage) (versionMarker, error) {
	var m versionMarker
	if page.KeyMarker != nil {
		m.key = *page.KeyMarker
	}
	if m.key != "" && page.VersionIDMarker != nil {
		versionID := *page.VersionIDMarker
		if versionID == s3.Null {
			versionID = nullVersion
		}
		err := tx.QueryRow(`SELECT seq FROM objects WHERE bucket_id = $1 AND name = $2 AND version_id = $3`,
			bid, m.key, versionID).Scan(&m.seq)
		if errors.Is(err, sql.ErrNoRows) {
			// marker version deleted between pages; list the whole key from its
			// newest version so its older, not-yet-listed versions aren't dropped
			m.seq = math.MaxInt64
		} else if err != nil {
			return versionMarker{}, err
		}
	}
	if adjusted, reset := adjustMarkerForCommonPrefix(prefix, m.key); reset {
		m.key, m.seq = adjusted, 0
	}
	return m, nil
}

// queryObjectVersions queries up to limit version rows of the bucket at or after
// the cursor m, ordered by key ascending then seq descending (newest first),
// each tagged with whether it is the current (latest) version of its key.
func queryObjectVersions(tx *txn, bid int64, prefix s3.Prefix, m versionMarker, limit int) (*rows, error) {
	query := `SELECT o.name, o.version_id, o.seq, o.is_delete_marker, o.parts_count, o.size, o.content_md5, o.updated_at, o.is_latest
FROM objects o
WHERE o.bucket_id = ?`
	args := []any{bid}
	if m.key != "" {
		query += ` AND (o.name > ? OR (o.name = ? AND o.seq < ?))`
		args = append(args, m.key, m.key, m.seq)
	}
	if prefix.HasPrefix {
		query += ` AND o.name >= ? AND o.name < ?`
		args = append(args, prefix.Prefix, prefix.Prefix+"\xFF")
	}
	query += ` ORDER BY o.name ASC, o.seq DESC LIMIT ?`
	args = append(args, limit)
	return tx.Query(query, args...)
}

// listVersionPage runs one bounded page query, folding rows into result until
// the page fills or a common prefix is rolled up. It returns the cursor to
// resume from and whether more pages remain.
func listVersionPage(tx *txn, bid int64, prefix s3.Prefix, m versionMarker, limit int, result *s3.ObjectVersionsListResult) (next versionMarker, more bool, _ error) {
	rows, err := queryObjectVersions(tx, bid, prefix, m, limit)
	if err != nil {
		return versionMarker{}, false, err
	}
	defer rows.Close()

	var lastKey, skipPrefix string
	var lastSeq int64
	var sawObject bool
	var n int
	for rows.Next() && !result.IsTruncated && skipPrefix == "" {
		n++
		var v s3.ObjectVersion
		var seq int64
		var partsCount int32
		var md5 [16]byte
		var lastModified time.Time
		if err := rows.Scan(&v.Key, &v.VersionID, &seq, &v.IsDeleteMarker, &partsCount, &v.Size, (*sqlMD5)(&md5), (*sqlTime)(&lastModified), &v.IsLatest); err != nil {
			return versionMarker{}, false, err
		}

		if cp := prefix.CommonPrefix(v.Key); cp != "" {
			result.AddPrefix(cp)
			skipPrefix = cp
			continue
		}

		v.LastModified = lastModified
		if !v.IsDeleteMarker {
			v.ETag = s3.FormatETag(md5[:], int(partsCount))
		} else {
			v.Size = 0
		}
		result.AddVersion(v)
		lastKey, lastSeq, sawObject = v.Key, seq, true
	}
	if err := rows.Err(); err != nil {
		return versionMarker{}, false, err
	}

	switch {
	case result.IsTruncated:
		// page filled; stop
		return versionMarker{}, false, nil
	case skipPrefix != "":
		// resume after the rolled-up common prefix
		return versionMarker{key: skipPrefix + "\xFF"}, true, nil
	case sawObject && n == limit:
		// a full page of object rows; resume mid-key after the last one
		return versionMarker{key: lastKey, seq: lastSeq}, true, nil
	default:
		// the query returned fewer than a full page; nothing more to list
		return versionMarker{}, false, nil
	}
}

// ListObjectVersions lists every version (including delete markers) of the
// objects in the bucket, ordered by key ascending then by version creation
// order descending (newest first), applying prefix, delimiter and the
// (key-marker, version-id-marker) cursor.
func (s *Store) ListObjectVersions(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectVersionsPage) (*s3.ObjectVersionsListResult, error) {
	result := s3.NewObjectVersionsListResult(page.MaxKeys)

	const maxRowsPerQuery = 100
	err := s.transaction(func(tx *txn) error {
		*result = *s3.NewObjectVersionsListResult(page.MaxKeys) // reset per attempt

		b, err := bucketForRead(tx, accessKeyID, bucket, s3.ActionListBucketVersions)
		if err != nil {
			return err
		}
		bid := b.id

		// a caller that asked for no keys still has to be allowed to list
		if page.MaxKeys == 0 {
			return nil
		}

		m, err := resolveVersionCursor(tx, bid, prefix, page)
		if err != nil {
			return err
		}

		for {
			next, more, err := listVersionPage(tx, bid, prefix, m, maxRowsPerQuery, result)
			if err != nil {
				return err
			}
			if !more {
				break
			}
			m = next
		}

		if !result.IsTruncated {
			result.NextKeyMarker, result.NextVersionIDMarker = "", ""
		}
		if page.FetchOwner != nil && *page.FetchOwner {
			owner := b.userInfo()
			for k := range result.Versions {
				result.Versions[k].Owner = owner
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
