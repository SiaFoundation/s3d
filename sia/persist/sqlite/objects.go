package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
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
			SELECT name, object_id, content_md5, metadata, size, updated_at
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).
			Scan(&obj.Name, (*sqlHash256)(&obj.ID), (*sqlMD5)(&obj.ContentMD5), &meta,
				&obj.Size, (*sqlTime)(&obj.UpdatedAt))
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchKey
		} else if err != nil {
			return err
		}

		err = json.Unmarshal([]byte(meta), &obj.Meta)
		if err != nil {
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
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				object_id = excluded.object_id,
				content_md5 = excluded.content_md5,
				metadata = excluded.metadata,
				size = excluded.size,
				updated_at = excluded.updated_at
		`, bid, name, sqlHash256(obj.ID), sqlMD5(obj.ContentMD5),
			string(metaJson), obj.Size, sqlTime(obj.UpdatedAt))
		return err
	})
}

// pathClean has the same behavior as path.Clean except it preserves
// trailing slashes for any input besides "/"
func pathClean(p string) string {
	if p == "" {
		return ""
	} else if p == "/" {
		return p
	}

	hasSlash := len(p) > 1 && p[len(p)-1] == '/'
	cleaned := path.Clean(p)
	if hasSlash && cleaned != "/" {
		cleaned += "/"
	}
	return cleaned
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

	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		// if a string is empty, path.Clean will replace it with "." but the
		// rest of its [rules](https://pkg.go.dev/path#Clean) are OK to enforce
		// here
		if prefix.HasPrefix && prefix.Prefix != "" {
			prefix.Prefix = pathClean(prefix.Prefix)
		}
		if prefix.HasDelimiter && prefix.Delimiter != "" {
			prefix.Delimiter = pathClean(prefix.Delimiter)
		}

		list := func(marker *string) (string, error) {
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
			args = append(args, page.MaxKeys)

			rows, err := tx.Query(query, args...)
			if err != nil {
				return "", fmt.Errorf("failed to query objects: %w", err)
			}
			defer rows.Close()

			var lastMatchedPart string
			for rows.Next() {
				var obj objects.Object
				err = rows.Scan(
					&obj.Name,
					(*sqlMD5)(&obj.ContentMD5),
					&obj.Size,
					(*sqlTime)(&obj.UpdatedAt),
				)
				if err != nil {
					return "", fmt.Errorf("failed to scan object: %w", err)
				}

				match := s3.Match(prefix, obj.Name)
				switch {
				case match == nil:
					continue
				case match.CommonPrefix:
					if marker != nil && strings.Compare(*marker, match.MatchedPart) >= 0 {
						continue
					}
					if match.MatchedPart == lastMatchedPart {
						continue // should not count towards keys
					}
					result.AddPrefix(match.MatchedPart)
					lastMatchedPart = match.MatchedPart
					break
				default:
					if marker != nil && strings.Compare(*marker, obj.Name) >= 0 {
						continue
					}
					result.Add(&s3.Content{
						Key:          obj.Name,
						LastModified: s3.NewContentTime(obj.UpdatedAt),
						ETag:         s3.FormatETag(obj.ContentMD5[:]),
						Size:         int64(obj.Size),
					})
				}

				if result.IsTruncated {
					break
				}
			}
			if err := rows.Err(); err != nil {
				return "", fmt.Errorf("failed to get rows: %w", err)
			}
			return lastMatchedPart, nil
		}

		marker := page.Marker
		for !result.IsTruncated {
			lastMatchedPart, err := list(marker)
			if err != nil {
				return err
			}
			if lastMatchedPart != "" {
				// if we get a common prefix, skip over remainder of common prefix
				lastMatchedPart += "\xFF"
				marker = &lastMatchedPart
			} else if result.NextMarker != "" {
				// otherwise continue getting the matching keys
				marker = &result.NextMarker
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
