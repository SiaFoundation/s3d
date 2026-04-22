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
)

// DeleteObject deletes the object with the given bucket and name if it exists
// and all provided preconditions match. If the deleted object's ID has no
// remaining references, it is inserted into the orphaned_objects table. If the
// object hasn't been uploaded yet, its filename is returned for cleanup.
func (s *Store) DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) (*string, error) {
	var fileName *string
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		// delete the row and return its values for precondition checks and
		// orphan detection; the transaction rolls back if preconditions fail
		var deletedID sql.Null[sqlHash256]
		var contentMD5 [16]byte
		var size int64
		var updatedAt time.Time
		err = tx.QueryRow(`
			DELETE FROM objects WHERE bucket_id = $1 AND name = $2
			RETURNING filename, object_id, content_md5, size, updated_at
		`, bid, objectID.Key).Scan(&fileName, &deletedID, (*sqlMD5)(&contentMD5), &size, (*sqlTime)(&updatedAt))
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

		// only return the filename for cleanup if no other object
		// references the same file
		if fileName != nil {
			var shared bool
			if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM objects WHERE filename = $1)`, *fileName).Scan(&shared); err != nil {
				return err
			}
			if shared {
				fileName = nil
			}
		}

		if deletedID.Valid {
			return insertOrphan(tx, types.Hash256(deletedID.V))
		}
		return nil
	})
	return fileName, err
}

// GetObject retrieves the object with the given bucket and name.
func (s *Store) GetObject(accessKeyID *string, bucket, name string, partNumber *int32) (*objects.Object, error) {
	var obj objects.Object
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
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
	// get parts count
	err := tx.QueryRow(`
		SELECT COUNT(*)
		FROM object_parts
		WHERE bucket_id = $1 AND name = $2
	`, bid, name).Scan(&obj.PartsCount)
	if err != nil {
		return err
	}

	// return full object if no part specified
	if partNumber == nil || obj.PartsCount == 0 {
		if obj.PartsCount == 0 && partNumber != nil {
			if *partNumber != 1 {
				return s3errs.ErrInvalidPart
			}
			obj.PartsCount = *partNumber
		}
		var objectID sql.Null[sqlHash256]
		var siaObj sql.Null[sqlSiaObject]
		err := tx.QueryRow(`
			SELECT filename, object_id, metadata, updated_at, size, content_md5, sia_object, cached_at
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).Scan(&obj.FileName, &objectID, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Length, (*sqlMD5)(&obj.ContentMD5), &siaObj, (*sqlTime)(&obj.CachedAt))
		if objectID.Valid {
			obj.ID = (*types.Hash256)(&objectID.V)
		}
		if siaObj.Valid {
			obj.SiaObject = (*slabs.SealedObject)(&siaObj.V)
		}
		return err
	}

	// return error if part number is invalid
	if partNumber != nil && obj.PartsCount > 0 && *partNumber > int32(obj.PartsCount) {
		return s3errs.ErrInvalidPart
	}

	// part specified, return part info
	var partObjID sql.Null[sqlHash256]
	err = tx.QueryRow(`
		SELECT o.object_id, o.filename, o.metadata, o.updated_at, p.offset, p.content_length, p.content_md5
		FROM object_parts p
		JOIN objects o ON o.bucket_id = p.bucket_id AND o.name = p.name
		WHERE o.bucket_id = $1 AND o.name = $2 AND p.part_number = $3
	`, bid, name, *partNumber).Scan(&partObjID, &obj.FileName, (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Offset, &obj.Length, (*sqlMD5)(&obj.ContentMD5))
	if partObjID.Valid {
		obj.ID = (*types.Hash256)(&partObjID.V)
	}
	return err
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists. If updatedModTime is true, the
// `updated_at` time that represents the S3 last modified time will be updated.
// If the overwritten object's ID has no remaining references, it is inserted
// into the orphaned_objects table.
func (s *Store) PutObject(accessKeyID, bucket, name string, contentMD5 [16]byte, meta map[string]string, length int64, fileName *string, updateModTime bool) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		return putObject(tx, bid, name, nil, contentMD5, meta, length, fileName, nil, time.Time{}, updateModTime)
	})
}

// MarkObjectUploaded transitions a pending upload to a Sia-backed object by
// setting the object_id, sia_object and cached_at fields while clearing
// filename. Fails if the object already has an object_id.
func (s *Store) MarkObjectUploaded(bucket, name string, siaObject slabs.SealedObject) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		objID := siaObject.ID()
		res, err := tx.Exec(`
			UPDATE objects
			SET object_id = $1, sia_object = $2, filename = NULL, cached_at = $3
			WHERE bucket_id = $4 AND name = $5 AND object_id IS NULL
		`, sqlHash256(objID), sqlSiaObject(siaObject), sqlTime(time.Now()), bid, name)
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return fmt.Errorf("object already has an object ID or does not exist")
		}
		return err
	})
}

// UpdateSiaObject refreshes the cached sia_object and cached_at fields for for
// all uploaded objects with the corresponding object id.
func (s *Store) UpdateSiaObject(siaObject slabs.SealedObject, cachedAt time.Time) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`
			UPDATE objects SET sia_object = $1, cached_at = $2
			WHERE object_id = $3
		`, sqlSiaObject(siaObject), sqlTime(cachedAt), sqlHash256(siaObject.ID()))
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return fmt.Errorf("object not found or object ID mismatch")
		}
		return err
	})
}

// CopyObject atomically reads the source object and writes it to the
// destination within a single transaction, applying metadata according to the
// replace flag. Returns the copied object metadata.
func (s *Store) CopyObject(srcBucket, srcName, dstBucket, dstName string, meta map[string]string, replace bool) (result *objects.Object, err error) {
	var obj objects.Object
	err = s.transaction(func(tx *txn) error {
		srcBid, err := bucketID(tx, srcBucket)
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
			dstBid, err = bucketID(tx, dstBucket)
			if err != nil {
				return err
			}
		}

		return putObject(tx, dstBid, dstName, obj.ID, obj.ContentMD5, obj.Meta, obj.Length, obj.FileName, obj.SiaObject, obj.CachedAt, true)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, s3errs.ErrNoSuchKey
	}
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// ObjectParts returns the parts for a completed multipart object.
func (s *Store) ObjectParts(bucket, name string) ([]objects.Part, error) {
	var parts []objects.Part
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
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

// OrphanedObjects returns up to limit object IDs from the orphaned_objects table.
func (s *Store) OrphanedObjects(limit int) (ids []types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query("SELECT object_id FROM orphaned_objects LIMIT $1", limit)
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
		_, err := tx.Exec("DELETE FROM orphaned_objects WHERE object_id = $1", sqlHash256(objectID))
		return err
	})
}

func putObject(tx *txn, bid int64, name string, id *types.Hash256, contentMD5 [16]byte, meta map[string]string, length int64, fileName *string, siaObject *slabs.SealedObject, cachedAt time.Time, updateModTime bool) error {
	if meta == nil {
		meta = make(map[string]string) // force '{}' instead of 'null' in JSON
	}

	oldID, err := previousObjectID(tx, bid, name)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at, filename, sia_object, cached_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT(bucket_id, name) DO UPDATE SET
			object_id = excluded.object_id,
			content_md5 = excluded.content_md5,
			metadata = excluded.metadata,
			size = excluded.size,
			updated_at = CASE WHEN $11 THEN excluded.updated_at ELSE objects.updated_at END,
			filename = excluded.filename,
			sia_object = excluded.sia_object,
			cached_at = excluded.cached_at
	`, bid, name, (*sqlHash256)(id), sqlMD5(contentMD5),
		sqlMetaJSON(meta), length, sqlTime(time.Now()),
		fileName, (*sqlSiaObject)(siaObject), sqlTime(cachedAt), updateModTime)
	if err != nil {
		return err
	}

	// clear any stale orphan entry for the new object ID, in case it was
	// previously orphaned and is now referenced again
	if id != nil {
		if _, err := tx.Exec("DELETE FROM orphaned_objects WHERE object_id = $1", sqlHash256(*id)); err != nil {
			return err
		}
	}

	if oldID != nil && (id == nil || *oldID != *id) {
		return insertOrphan(tx, *oldID)
	}
	return nil
}

// previousObjectID returns the object_id currently stored for the given bucket
// and name, or nil if no row exists.
func previousObjectID(tx *txn, bid int64, name string) (*types.Hash256, error) {
	var id sql.Null[sqlHash256]
	err := tx.QueryRow("SELECT object_id FROM objects WHERE bucket_id = $1 AND name = $2", bid, name).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if !id.Valid {
		return nil, nil
	}
	return (*types.Hash256)(&id.V), nil
}

// insertOrphan adds objectID to the orphaned_objects table if no rows in the
// objects table reference it.
func insertOrphan(tx *txn, objectID types.Hash256) error {
	if objectID == (types.Hash256{}) {
		return nil // skip zero-value (empty objects)
	}
	var referenced bool
	if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM objects WHERE object_id = $1)", sqlHash256(objectID)).Scan(&referenced); err != nil {
		return err
	}
	if referenced {
		return nil
	}
	_, err := tx.Exec("INSERT OR IGNORE INTO orphaned_objects (object_id) VALUES ($1)", sqlHash256(objectID))
	return err
}

// ListObjects lists objects in the specified bucket for the user identified
// by the given access key. The backend should use the prefix to limit the
// contents of the bucket and sort the results into the Contents and
// CommonPrefixes fields of the returned ObjectsListResult.
func (s *Store) ListObjects(_ *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (result *s3.ObjectsListResult, err error) {
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

	// prepare owner info if requested
	var owner *s3.UserInfo
	if page.FetchOwner != nil && *page.FetchOwner {
		owner = s3.GlobalUserInfo
	}

	const maxObjsPerQuery = 100
	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		list := func(marker *string) (string, string, error) {
			query := `SELECT o.name, o.content_md5, o.size, o.updated_at
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
						ETag:         s3.FormatETag(obj.ContentMD5[:], 0),
						Size:         int64(obj.Length),
						Owner:        owner,
					})
					lastObj = obj.Name
				}
			}
			if err := rows.Err(); err != nil {
				return "", "", fmt.Errorf("failed to get rows: %w", err)
			}
			return lastMatchedPart, lastObj, nil
		}

		innerMarker := marker
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
