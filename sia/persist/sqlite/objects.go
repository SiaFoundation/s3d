package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

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

func commonPrefix(key, prefix, delimiter string) string {
	after, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return ""
	}

	idx := strings.IndexRune(after, rune(delimiter[0]))
	if idx == -1 {
		return ""
	}

	return prefix + after[:idx+utf8.RuneCountInString(delimiter)]
}

// ListObjects lists objects in the specified bucket for the user identified
// by the given access key. The backend should use the prefix to limit the
// contents of the bucket and sort the results into the Contents and
// CommonPrefixes fields of the returned ObjectsListResult.
func (s *Store) ListObjects(_ *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (result *s3.ObjectsListResult, err error) {
	result = s3.NewObjectsListResult(page.MaxKeys)

	// if the marker falls inside a common prefix (e.g. prefix "ac", delimiter
	// "/", marker "acb/x"), advance past that prefix so it isn't returned
	// twice
	if prefix.Delimiter != "" && page.Marker != nil && *page.Marker != "" {
		markerRemainder := *page.Marker
		var prefixLen int
		if after, ok := strings.CutPrefix(*page.Marker, prefix.Prefix); ok {
			prefixLen = len(prefix.Prefix)
			markerRemainder = after
		}
		if idx := strings.Index(markerRemainder, prefix.Delimiter); idx != -1 {
			commonPrefix := (*page.Marker)[:prefixLen+idx+len(prefix.Delimiter)]
			*page.Marker = commonPrefix + string([]byte{0xFF})
		}
	}

	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		// build uploads query
		query, args := buildContentsQuery(bid, prefix.Prefix, prefix.Delimiter, page.Marker)

		// build common prefixes query if needed
		if prefix.HasDelimiter {
			query2, args2 := buildCommonPrefixesQuery(bid, prefix.Prefix, prefix.Delimiter, page.Marker)
			query = fmt.Sprintf(`
				WITH uploads AS (%s), prefixes AS (%s)
				SELECT name, content_md5, size, updated_at, is_prefix FROM uploads
				UNION ALL
				SELECT name, content_md5, size, updated_at, is_prefix FROM prefixes`, query, query2)
			args = append(args, args2...)
		}

		// order and limit
		query += " ORDER BY name"
		query += " LIMIT ?"
		args = append(args, page.MaxKeys+1)

		// collect results
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() && !result.IsTruncated {
			var obj objects.Object
			var isPrefix bool
			err = rows.Scan(
				&obj.Name,
				(*sqlMD5)(&obj.ContentMD5),
				&obj.Size,
				(*sqlTime)(&obj.UpdatedAt),
				&isPrefix,
			)
			if err != nil {
				return fmt.Errorf("failed to scan object: %w", err)
			}

			if isPrefix {
				result.AddPrefix(commonPrefix(obj.Name, prefix.Prefix, prefix.Delimiter))
			} else {
				result.Add(&s3.Content{
					Key:          obj.Name,
					LastModified: s3.NewContentTime(obj.UpdatedAt),
					ETag:         s3.FormatETag(obj.ContentMD5[:]),
					Size:         int64(obj.Size),
				})
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	if !result.IsTruncated {
		result.NextMarker = ""
	}
	return result, nil
}

func buildContentsQuery(bucketID int64, prefix, delimiter string, keyMarker *string) (string, []any) {
	var (
		hasPrefix    = prefix != ""
		hasDelim     = delimiter != ""
		prefixLen    = utf8.RuneCountInString(prefix)
		searchOffset = prefixLen + 1
	)

	// check bucket
	where := []string{"bucket_id = ?"}
	args := []any{bucketID}

	// handle prefix
	if hasPrefix {
		where = append(where, "SUBSTR(name, 1, ?) = ?")
		args = append(args, prefixLen, prefix)
	}

	// handle delimiter
	if hasDelim {
		if hasPrefix {
			// when we know there's a prefix, start searching after it
			where = append(where, "INSTR(SUBSTR(name, ?), ?) = 0")
			args = append(args, searchOffset, delimiter)
		} else {
			// no prefix, just ensure delimiter not in the whole name
			where = append(where, "INSTR(name, ?) = 0")
			args = append(args, delimiter)
		}
	}

	if keyMarker != nil {
		where = append(where, "name > ?")
		args = append(args, *keyMarker)
	}

	return fmt.Sprintf(`SELECT name, content_md5, size, updated_at, FALSE as is_prefix FROM objects WHERE %s`, strings.Join(where, " AND ")), args
}

func buildCommonPrefixesQuery(bucketID int64, prefix, delimiter string, keyMarker *string) (_ string, args []any) {
	var (
		prefixLen    = utf8.RuneCountInString(prefix)
		searchOffset = prefixLen + 1
	)

	// search delimiter after prefix
	args = append(args, searchOffset, delimiter, prefixLen)

	// check bucket
	where := []string{"bucket_id = ?"}
	args = append(args, bucketID)

	// check prefix
	where = append(where, "SUBSTR(name, 1, ?) = ? AND INSTR(SUBSTR(name, ?), ?) > 0")
	args = append(args, prefixLen, prefix, searchOffset, delimiter)

	if keyMarker != nil {
		where = append(where, "name > ?")
		args = append(args, *keyMarker)
	}

	return fmt.Sprintf(`
		SELECT name, content_md5, size, updated_at, TRUE as is_prefix FROM (
			SELECT
				name,
				content_md5,
				size,
				updated_at,
				ROW_NUMBER() OVER (
					PARTITION BY SUBSTR(name, 1, INSTR(SUBSTR(name, ?), ?) + ?)
					ORDER BY name
				) as row
			FROM objects
			WHERE %s
		) WHERE row = 1`, strings.Join(where, " AND ")), args
}
