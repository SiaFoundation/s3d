package sqlite

import (
	"database/sql"
	"errors"
	"strings"
	"time"

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
	// 1. Get the bucket ID (and check if the bucket exists).
	var bid int64
	err := s.transaction(func(tx *txn) error {
		var err error
		bid, err = bucketID(tx, bucket)
		if err != nil {
			// This will correctly return s3errs.ErrNoSuchBucket if not found.
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := s3.NewObjectsListResult(page.MaxKeys)
	if page.MaxKeys == 0 {
		return result, nil
	}

	// 2. Determine the SQL query parameters for range-based pagination.
	// We use the existing unique index: UNIQUE(bucket_id, name)

	// The SQL query should start at the greatest of the S3 marker or the S3 prefix.
	startKey := prefix.Prefix
	if page.Marker != nil && strings.Compare(*page.Marker, startKey) > 0 {
		startKey = *page.Marker
	}

	// We fetch MaxKeys + 1 to efficiently check for truncation.
	limit := page.MaxKeys + 1

	// 3. Execute the scalable range query.
	// The query uses the bucket_id and name >= startKey to leverage the index.
	// Note: We avoid an index-unfriendly LIKE query here.
	rows, err := s.db.Query(`
		SELECT name, sia_meta
		FROM objects
		WHERE bucket_id = $1
		  AND name >= $2
		ORDER BY name ASC
		LIMIT $3
	`, bid, startKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		lastMatchedPart string
		objNameBytes    []byte
		encoded         []byte
		count           int64
	)

	// 4. Process results, apply prefix/delimiter filtering and internal pagination.
	for rows.Next() {
		// Stop processing rows if we've fetched enough for the current page.
		if count >= page.MaxKeys {
			result.IsTruncated = true
			break
		}
		count++

		if err := rows.Scan(&objNameBytes, &encoded); err != nil {
			return nil, err
		}
		objName := string(objNameBytes)

		// Filter out objects that don't match the prefix (if startKey was the marker)
		if !strings.HasPrefix(objName, prefix.Prefix) {
			// This case should ideally not happen if startKey = prefix.Prefix,
			// but serves as a final safety check if the database optimization is imperfect.
			continue
		}

		match := s3.Match(prefix, objName)
		if match == nil {
			continue // Should have been caught by strings.HasPrefix check, but remains for safety.
		}

		if match.CommonPrefix {
			// Handle common prefix (directory-like behavior)
			if match.MatchedPart == lastMatchedPart {
				continue // Already counted this prefix
			}
			result.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart
		} else {
			// Handle object (Content)
			var obj slabs.SealedObject
			if err := obj.UnmarshalSia(encoded); err != nil {
				return nil, err
			}

			// Add the actual object content to the result.
			result.Add(&s3.Content{
				Key:          objName,
				LastModified: s3.NewContentTime(time.Unix(obj.LastModified, 0)),
				ETag:         s3.FormatETag(obj.SiaMetadata.ETag),
				Owner: &s3.UserInfo{
					ID: "", // Owner ID is not stored in the object table
				},
				Size: obj.SiaMetadata.Size,
			})
			result.NextMarker = objName
		}
	}

	// Handle potential error from rows iteration.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 5. Finalize the result.
	if !result.IsTruncated {
		result.NextMarker = ""
	}

	return result, nil
}
