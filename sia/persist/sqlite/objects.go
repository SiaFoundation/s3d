package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/SiaFoundation/s3d/s3"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.sia.tech/indexd/slabs"
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
func (s *Store) GetObject(accessKeyID *string, bucket, name string) (slabs.SealedObject, error) {
	var encoded []byte
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		err = tx.QueryRow(`
			SELECT sia_meta
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).Scan(&encoded)
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchKey
		}
		return err
	})
	if err != nil {
		return slabs.SealedObject{}, err
	}
	var obj slabs.SealedObject
	err = obj.UnmarshalSia(encoded)
	return obj, err
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists.
func (s *Store) PutObject(accessKeyID, bucket, name string, obj slabs.SealedObject) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		encoded, err := obj.MarshalSia()
		if err != nil {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, sia_meta)
			VALUES ($1, $2, $3)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				sia_meta = excluded.sia_meta
		`, bid, name, encoded)
		return err
	})
}

// ListObjects lists objects in the specified bucket for the user identified
// by the given access key. The backend should use the prefix to limit the
// contents of the bucket and sort the results into the Contents and
// CommonPrefixes fields of the returned ObjectsListResult.
func (s *Store) ListObjects(accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error) {
	result := s3.NewObjectsListResult(page.MaxKeys)
	if page.MaxKeys == 0 {
		return result, nil
	}

	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return fmt.Errorf("failed to get bucket ID: %w", err)
		}

		// Query 1: Fetch up to maxKeys actual objects
		objects, err := s.fetchObjects(tx, bid, prefix, page)
		if err != nil {
			return err
		}

		// Query 2: Fetch up to maxKeys common prefixes
		commonPrefixes, err := s.fetchCommonPrefixes(tx, bid, prefix, page)
		if err != nil {
			return err
		}

		// Merge results in sorted order
		s.mergeResults(result, objects, commonPrefixes, page.MaxKeys)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) fetchObjects(tx *txn, bid int64, prefix s3.Prefix, page s3.ListObjectsPage) ([]*s3.Content, error) {
	query := `SELECT name, sia_meta FROM objects WHERE bucket_id = ?`
	args := []any{bid}

	if page.Marker != nil && *page.Marker != "" {
		query += ` AND name > ?`
		args = append(args, *page.Marker)
	}
	if prefix.HasPrefix {
		query += ` AND name LIKE ?`
		args = append(args, prefix.Prefix+"%")
	}
	// Exclude objects that would be common prefixes
	if prefix.HasDelimiter {
		query += ` AND name NOT LIKE ?`
		args = append(args, prefix.Prefix+"%"+prefix.Delimiter+"%")
	}
	query += ` ORDER BY name LIMIT ?`
	args = append(args, page.MaxKeys+1)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}
	defer rows.Close()

	var objects []*s3.Content
	for rows.Next() {
		var name string
		var siaMeta []byte
		if err := rows.Scan(&name, &siaMeta); err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}

		var obj slabs.SealedObject
		if err := obj.UnmarshalSia(siaMeta); err != nil {
			return nil, fmt.Errorf("failed to parse object: %w", err)
		}
		objID := obj.ID()

		var size uint32
		for _, slab := range obj.Slabs {
			size += slab.Length
		}

		objects = append(objects, &s3.Content{
			Key:          name,
			LastModified: s3.NewContentTime(obj.UpdatedAt),
			ETag:         s3.FormatETag(objID[:]),
			Size:         int64(size),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	return objects, nil
}

func (s *Store) fetchCommonPrefixes(tx *txn, bid int64, prefix s3.Prefix, page s3.ListObjectsPage) ([]string, error) {
	if !prefix.HasDelimiter {
		return nil, nil
	}

	// Find distinct common prefixes by selecting the minimum name for each prefix group
	query := `
SELECT DISTINCT substr(name, 1, instr(substr(name, ?), ?) + ? - 1) as common_prefix
FROM objects 
WHERE bucket_id = ?`

	prefixLen := len(prefix.Prefix) + 1
	args := []any{prefixLen, prefix.Delimiter, len(prefix.Prefix), bid}

	if page.Marker != nil && *page.Marker != "" {
		query += ` AND name > ?`
		args = append(args, *page.Marker)
	}
	if prefix.HasPrefix {
		query += ` AND name LIKE ?`
		args = append(args, prefix.Prefix+"%")
	}
	// Only include objects that have the delimiter after the prefix
	if prefix.HasDelimiter {
		query += ` AND name LIKE ?`
		args = append(args, prefix.Prefix+"%"+prefix.Delimiter+"%")
	}

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
		prefixes = append(prefixes, commonPrefix+"/")
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	return prefixes, nil
}

func (s *Store) mergeResults(result *s3.ObjectsListResult, objects []*s3.Content, prefixes []string, maxKeys int64) {
	i, j := 0, 0

	for int64(len(result.CommonPrefixes)+len(result.Contents)) < maxKeys && (i < len(objects) || j < len(prefixes)) {
		if i >= len(objects) {
			// Only prefixes left
			result.AddPrefix(prefixes[j])
			j++
		} else if j >= len(prefixes) {
			// Only objects left
			result.Add(objects[i])
			i++
		} else {
			// Compare and add the smaller one
			if objects[i].Key < prefixes[j] {
				result.Add(objects[i])
				i++
			} else {
				result.AddPrefix(prefixes[j])
				j++
			}
		}
	}

	// Check if there are more results
	if i < len(objects) || j < len(prefixes) {
		result.IsTruncated = true
		// Set NextMarker to the last added key
		if (len(result.CommonPrefixes) + len(result.Contents)) > 0 {
			// Get the last item added (either from Contents or CommonPrefixes)
			lastContent := result.Contents[len(result.Contents)-1]
			lastPrefix := ""
			if len(result.CommonPrefixes) > 0 {
				lastPrefix = result.CommonPrefixes[len(result.CommonPrefixes)-1].Prefix
			}
			if lastPrefix != "" && (lastContent == nil || lastPrefix > lastContent.Key) {
				result.NextMarker = lastPrefix
			} else if lastContent != nil {
				result.NextMarker = lastContent.Key
			}
		}
	}
}
