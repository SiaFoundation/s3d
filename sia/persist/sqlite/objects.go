package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
		query += `
ORDER BY name
LIMIT ?;`
		args = append(args, page.MaxKeys+1)

		rows, err := tx.Query(query, args...)
		if err != nil {
			return fmt.Errorf("failed to query objects: %w", err)
		}
		defer rows.Close()

		var lastMatchedPart string
		for rows.Next() {
			var name string
			var siaMeta []byte
			if err := rows.Scan(&name, &siaMeta); err != nil {
				return fmt.Errorf("failed to scan: %w", err)
			}

			var obj slabs.SealedObject
			if err := obj.UnmarshalSia(siaMeta); err != nil {
				return fmt.Errorf("failed to parse object: %w", err)
			}
			objID := obj.ID()

			match := s3.Match(prefix, name)
			switch {
			case match == nil:
				continue
			case match.CommonPrefix:
				if page.Marker != nil && strings.Compare(*page.Marker, match.MatchedPart) >= 0 {
					continue
				}
				if match.MatchedPart == lastMatchedPart {
					continue // should not count towards keys
				}
				result.AddPrefix(match.MatchedPart)
				lastMatchedPart = match.MatchedPart
			default:
				if page.Marker != nil && strings.Compare(*page.Marker, name) >= 0 {
					continue
				}

				var size uint32
				for _, obj := range obj.Slabs {
					size += obj.Length
				}

				result.Add(&s3.Content{
					Key:          name,
					LastModified: s3.NewContentTime(obj.UpdatedAt),
					ETag:         s3.FormatETag(objID[:]),
					Size:         int64(size),
				})
			}

			if result.IsTruncated {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to get rows: %w", err)
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
