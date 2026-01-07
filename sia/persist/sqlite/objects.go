package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
)

// DeleteObject deletes the object with the given bucket and name if it exists
// and all provided preconditions match.
func (s *Store) DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		if objectID.ETag != nil || objectID.LastModifiedTime != nil || objectID.Size != nil {
			var contentMD5 [16]byte
			var size int64
			var updatedAt time.Time
			err = tx.QueryRow("SELECT content_md5, size, updated_at FROM objects WHERE bucket_id = $1 AND name = $2", bid, objectID.Key).
				Scan((*sqlMD5)(&contentMD5), &size, (*sqlTime)(&updatedAt))
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
		}

		_, err = tx.Exec("DELETE FROM objects WHERE bucket_id = $1 AND name = $2", bid, objectID.Key)
		return err
	})
}

// GetObject retrieves the object with the given bucket and name.
func (s *Store) GetObject(accessKeyID *string, bucket, name string, partNumber *int32) (*objects.Object, error) {
	var obj objects.Object
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		// get parts count
		err = tx.QueryRow(`
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
			return tx.QueryRow(`
				SELECT object_id, metadata, updated_at, size, content_md5
				FROM objects
				WHERE bucket_id = $1 AND name = $2
			`, bid, name).Scan((*sqlHash256)(&obj.ID), (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Length, (*sqlMD5)(&obj.ContentMD5))
		}

		// part specified, return part info
		return tx.QueryRow(`
			SELECT o.object_id, o.metadata, o.updated_at, p.offset, p.content_length, p.content_md5
			FROM object_parts p
			JOIN objects o ON o.bucket_id = p.bucket_id AND o.name = p.name
			WHERE o.bucket_id = $1 AND o.name = $2 AND p.part_number = $3
		`, bid, name, *partNumber).Scan((*sqlHash256)(&obj.ID), (*sqlMetaJSON)(&obj.Meta), (*sqlTime)(&obj.LastModified), &obj.Offset, &obj.Length, (*sqlMD5)(&obj.ContentMD5))
	}); errors.Is(err, sql.ErrNoRows) {
		return nil, s3errs.ErrNoSuchKey
	} else if err != nil {
		return nil, err
	}

	return &obj, nil
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists.
func (s *Store) PutObject(accessKeyID, bucket, name string, objectID types.Hash256, metadata map[string]string, contentMD5 [16]byte, contentLength int64) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		if metadata == nil {
			metadata = make(map[string]string) // force '{}' instead of 'null' in JSON
		}

		_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				object_id = excluded.object_id,
				content_md5 = excluded.content_md5,
				metadata = excluded.metadata,
				size = excluded.size,
				updated_at = excluded.updated_at
		`, bid, name, sqlHash256(objectID), sqlMD5(contentMD5),
			sqlMetaJSON(metadata), contentLength, sqlTime(time.Now()))
		return err
	})
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
		} else if prefix.HasDelimiter && strings.HasSuffix(result.NextMarker, prefix.Delimiter) {
			// NextMarker is a common prefix. Append \xFF to skip past all objects
			// with that prefix on the next call.
			result.NextMarker += "\xFF"
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
