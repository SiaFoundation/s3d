package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/SiaFoundation/s3d/s3"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
)

// DeleteObject deletes the object with the given bucket and name.
func (s *Store) DeleteObject(accessKeyID, bucket, name string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec("DELETE FROM objects WHERE bucket_id = $1 AND name = $2", bid, name)
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
	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		// if a string is empty, path.Clean will replace it with "." but the
		// rest of its [rules](https://pkg.go.dev/path#Clean) are OK to enforce
		// here
		if prefix.HasPrefix && prefix.Prefix != "" {
			prefix.Prefix = strings.ToLower(pathClean(prefix.Prefix))
		}
		if prefix.HasDelimiter && prefix.Delimiter != "" {
			prefix.Delimiter = strings.ToLower(pathClean(prefix.Delimiter))
		}

		// fetch up to maxKeys actual objects
		objects, err := s.fetchObjects(tx, bid, prefix, page)
		if err != nil {
			return err
		}

		// fetch up to maxKeys common prefixes
		commonPrefixes, err := s.fetchCommonPrefixes(tx, bid, prefix, page)
		if err != nil {
			return err
		}

		// merge results in sorted order
		result = s.mergeResults(objects, commonPrefixes, page.MaxKeys)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) fetchObjects(tx *txn, bid int64, prefix s3.Prefix, page s3.ListObjectsPage) ([]*s3.Content, error) {
	query := `SELECT o.name, o.content_md5, o.size, o.updated_at 
FROM objects o
WHERE o.bucket_id = ?`
	args := []any{bid}

	if page.Marker != nil && *page.Marker != "" {
		query += ` AND o.name > ?`
		args = append(args, *page.Marker)
	}

	if prefix.HasPrefix {
		query += ` AND o.name_lower >= ? AND o.name_lower < ?`
		args = append(args, prefix.Prefix, prefix.Prefix+"\xFF")
	}

	if prefix.HasDelimiter {
		query += ` AND instr(substr(o.name_lower, ?), ?) = 0`
		args = append(args, len(prefix.Prefix)+1, prefix.Delimiter)
	}

	query += ` ORDER BY o.name LIMIT ?`
	args = append(args, page.MaxKeys+1)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}
	defer rows.Close()

	var objs []*s3.Content
	for rows.Next() {
		var obj objects.Object
		err = rows.Scan(
			&obj.Name,
			(*sqlMD5)(&obj.ContentMD5),
			&obj.Size,
			(*sqlTime)(&obj.UpdatedAt),
		)
		if err != nil {
			return nil, err
		}

		objs = append(objs, &s3.Content{
			Key:          obj.Name,
			LastModified: s3.NewContentTime(obj.UpdatedAt),
			ETag:         s3.FormatETag(obj.ContentMD5[:]),
			Size:         int64(obj.Size),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	return objs, nil
}

func (s *Store) fetchCommonPrefixes(tx *txn, bid int64, prefix s3.Prefix, page s3.ListObjectsPage) ([]string, error) {
	if !prefix.HasDelimiter {
		return nil, nil
	}

	// find distinct common prefixes by selecting the minimum name for each prefix group
	query := `
SELECT DISTINCT substr(o.name, 1, instr(substr(o.name, ?), ?) + ?) as common_prefix FROM objects o
WHERE bucket_id = ?`

	prefixLen := len(prefix.Prefix) + 1
	args := []any{prefixLen, strings.ToLower(prefix.Delimiter), len(prefix.Prefix), bid}

	if page.Marker != nil && *page.Marker != "" {
		query += ` AND o.name > ?`
		args = append(args, *page.Marker)
	}

	if prefix.HasPrefix {
		query += ` AND o.name_lower >= ? AND o.name_lower < ?`
		args = append(args, prefix.Prefix, prefix.Prefix+"\xFF")
	}

	// Only objects with delimiter after prefix
	query += ` AND instr(substr(o.name_lower, ?), ?) > 0`
	args = append(args, prefixLen, prefix.Delimiter)

	query += ` ORDER BY common_prefix LIMIT ?`
	args = append(args, page.MaxKeys+1)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query common prefixes: %w", err)
	}
	defer rows.Close()

	var prefixes []string
	for rows.Next() {
		var commonPrefix string
		if err := rows.Scan(&commonPrefix); err != nil {
			return nil, fmt.Errorf("failed to scan common prefix: %w", err)
		}
		prefixes = append(prefixes, commonPrefix)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	return prefixes, nil
}

func (s *Store) mergeResults(objects []*s3.Content, prefixes []string, maxKeys int64) *s3.ObjectsListResult {
	result := s3.NewObjectsListResult(maxKeys)

	i, j := 0, 0
	for int64(len(result.CommonPrefixes)+len(result.Contents)) < maxKeys && (i < len(objects) || j < len(prefixes)) {
		if i >= len(objects) {
			// only prefixes left
			result.AddPrefix(prefixes[j])
			j++
		} else if j >= len(prefixes) {
			// only objects left
			result.Add(objects[i])
			i++
		} else {
			// compare and add the smaller one
			if objects[i].Key < prefixes[j] {
				result.Add(objects[i])
				i++
			} else {
				result.AddPrefix(prefixes[j])
				j++
			}
		}
	}

	// check if there are more results
	if i < len(objects) || j < len(prefixes) {
		result.IsTruncated = true
		// set NextMarker to the last added key
		if (len(result.CommonPrefixes) + len(result.Contents)) > 0 {
			// get the last item added (either from Contents or CommonPrefixes)
			var lastContent string
			if len(result.Contents) > 0 {
				lastContent = result.Contents[len(result.Contents)-1].Key
			}
			var lastPrefix string
			if len(result.CommonPrefixes) > 0 {
				lastPrefix = result.CommonPrefixes[len(result.CommonPrefixes)-1].Prefix
			}
			if lastPrefix != "" && (lastContent == "" || lastPrefix > lastContent) {
				result.NextMarker = lastPrefix
			} else if lastContent != "" {
				result.NextMarker = lastContent
			}
		}
	}
	return result
}
