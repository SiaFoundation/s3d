package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
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

			if objectID.ETag != nil && *objectID.ETag != s3.FormatETag(contentMD5[:]) {
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
func (s *Store) GetObject(accessKeyID *string, bucket, name string) (*objects.Object, error) {
	var obj objects.Object
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		var meta string
		err = tx.QueryRow(`
			SELECT name, object_id, content_md5, metadata, size, updated_at, cached_metadata, cached_at
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).
			Scan(&obj.Name, (*sqlHash256)(&obj.ID), (*sqlMD5)(&obj.ContentMD5), &meta,
				&obj.Size, (*sqlTime)(&obj.UpdatedAt), (*sqlCachedMetadata)(&obj.CachedMetadata), (*sqlTime)(&obj.CachedAt))
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchKey
		} else if err != nil {
			return err
		}

		if err = json.Unmarshal([]byte(meta), &obj.Meta); err != nil {
			return errors.New("failed to unmarshal object metadata: " + err.Error())
		}
		return nil
	})
	return &obj, err
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists.
func (s *Store) PutObject(accessKeyID, bucket, name string, obj *objects.Object) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		if obj.Meta == nil {
			obj.Meta = make(map[string]string) // force '{}' instead of 'null' in JSON
		}
		metaJson, err := json.Marshal(obj.Meta)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at, cached_metadata, cached_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				object_id = excluded.object_id,
				content_md5 = excluded.content_md5,
				metadata = excluded.metadata,
				size = excluded.size,
				updated_at = excluded.updated_at,
				cached_metadata = excluded.cached_metadata,
				cached_at = excluded.cached_at
		`, bid, name, sqlHash256(obj.ID), sqlMD5(obj.ContentMD5), string(metaJson), obj.Size, sqlTime(obj.UpdatedAt),
			sqlCachedMetadata(obj.CachedMetadata), sqlTime(obj.CachedAt))
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
					&obj.Size,
					(*sqlTime)(&obj.UpdatedAt),
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
						LastModified: s3.NewContentTime(obj.UpdatedAt),
						ETag:         s3.FormatETag(obj.ContentMD5[:]),
						Size:         int64(obj.Size),
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
