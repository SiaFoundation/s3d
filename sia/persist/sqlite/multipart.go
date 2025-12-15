package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
)

// CreateMultipartUpload persists metadata for a new multipart upload.
func (s *Store) CreateMultipartUpload(bucket, name string, uploadID s3.UploadID, meta map[string]string) error {
	if meta == nil {
		meta = make(map[string]string) // force '{}' instead of 'null' in JSON
	}

	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		metaJson, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(`
				INSERT INTO multipart_uploads (upload_id, bucket_id, name, metadata, created_at)
				VALUES ($1, $2, $3, $4, $5)
			`, sqlUploadID(uploadID), bid, name, string(metaJson), sqlTime(time.Now())); err != nil {
			return fmt.Errorf("failed to insert multipart upload: %w", err)
		}
		return nil
	})
}

// CompleteMultipartUpload finalizes a multipart upload by creating the object
// and transferring parts from the upload to the object.
func (s *Store) CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, objectID types.Hash256, contentMD5 [16]byte, contentLength int64) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		// validate parts exist and are continuous
		var partCount, minPart, maxPart int
		var totalSize int64
		err = tx.QueryRow(`
			SELECT
				COUNT(*),
				MIN(part_number),
				MAX(part_number),
				SUM(content_length)
			FROM parts
			WHERE multipart_upload_id = $1
		`, uid).Scan(&partCount, &minPart, &maxPart, &totalSize)
		if err != nil {
			return err
		} else if partCount == 0 {
			return errors.New("cannot complete multipart upload with no parts")
		} else if minPart != 1 || maxPart != partCount {
			return fmt.Errorf("part numbers must be continuous from 1 to %d, got range %d to %d", partCount, minPart, maxPart)
		} else if totalSize != contentLength {
			return fmt.Errorf("total part size (%d) does not match content length (%d)", totalSize, contentLength)
		}

		// verify all parts except last meet minimum size
		var smallParts int
		err = tx.QueryRow(`
			SELECT COUNT(*)
			FROM parts
			WHERE multipart_upload_id = $1
			  AND part_number < $2
			  AND content_length < $3
		`, uid, partCount, s3.MinUploadPartSize).Scan(&smallParts)
		if err != nil {
			return err
		}
		if smallParts > 0 {
			return fmt.Errorf("found %d parts smaller than minimum size (%d bytes)", smallParts, s3.MinUploadPartSize)
		}

		// create object with metadata from multipart upload
		var objID int64
		err = tx.QueryRow(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at)
			SELECT bucket_id, name, $1, $2, metadata, $3, $4
			FROM multipart_uploads
			WHERE id = $5
			RETURNING id
		`, sqlHash256(objectID), sqlMD5(contentMD5), contentLength, sqlTime(time.Now()), uid).Scan(&objID)
		if err != nil {
			return err
		}

		// compute and set offsets
		_, err = tx.Exec(`
			UPDATE parts
			SET offset = (
				SELECT COALESCE(SUM(p2.content_length), 0)
				FROM parts p2
				WHERE p2.multipart_upload_id = $1 AND p2.part_number < parts.part_number
			)
			WHERE multipart_upload_id = $1
		`, uid)
		if err != nil {
			return err
		}

		// transfer parts to object
		_, err = tx.Exec(`
			UPDATE parts
			SET object_id = $1, multipart_upload_id = NULL, filename = NULL, created_at = NULL
			WHERE multipart_upload_id = $2
		`, objID, uid)

		// delete the multipart upload
		_, err = tx.Exec(`DELETE FROM multipart_uploads WHERE id = $1`, uid)
		return err
	})
}

// AbortMultipartUpload removes a multipart upload from the store.
func (s *Store) AbortMultipartUpload(bucket, name string, uploadID s3.UploadID) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`DELETE FROM multipart_uploads WHERE id = $1`, uid)
		return err
	})
}

// AddMultipartPart adds metadata for a multipart part to the store.
func (s *Store) AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error) {
	var prevFilename string
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		err = tx.QueryRow(`SELECT filename FROM parts WHERE multipart_upload_id = $1 AND part_number = $2`, uid, partNumber).Scan(&prevFilename)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO parts (multipart_upload_id, part_number, filename, content_md5, content_length, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET
				filename       = EXCLUDED.filename,
				content_md5    = EXCLUDED.content_md5,
				content_length = EXCLUDED.content_length,
				created_at     = EXCLUDED.created_at
		`, uid, partNumber, filename, sqlMD5(contentMD5), contentLength, sqlTime(time.Now()))
		return err
	}); err != nil {
		return "", err
	}
	return prevFilename, nil
}

// HasMultipartUpload checks if a multipart upload exists.
func (s *Store) HasMultipartUpload(bucket, name string, uploadID s3.UploadID) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		_, err = multipartID(tx, uploadID, bid, name)
		return err
	})
}

// MultipartParts returns the parts belonging to the specified multipart upload.
func (s *Store) MultipartParts(bucket, name string, uploadID s3.UploadID) ([]objects.Part, error) {
	var parts []objects.Part
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		rows, err := tx.Query(`
			SELECT part_number, filename, content_md5, content_length
			FROM parts
			WHERE multipart_upload_id = $1
			ORDER BY part_number ASC`, uid)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var part objects.Part
			if err := rows.Scan(&part.PartNumber, &part.Filename, (*sqlMD5)(&part.ContentMD5), &part.Size); err != nil {
				return err
			}
			parts = append(parts, part)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return parts, nil
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Store) ListParts(bucket, name string, uploadID s3.UploadID, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error) {
	res := &s3.ListPartsResult{
		OwnerID:              "", // TODO: sia backend does not yet support owners
		InitiatorID:          "", // TODO: sia backend does not yet support initiators
		OwnerDisplayName:     "",
		InitiatorDisplayName: "",
		Parts:                make([]s3.UploadPart, 0, maxParts),
	}

	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		rows, err := tx.Query(`
			SELECT part_number, content_length, content_md5, created_at
			FROM parts
			WHERE multipart_upload_id = $1 AND part_number > $2
			ORDER BY part_number ASC
			LIMIT $3
		`, uid, partNumberMarker, maxParts+1)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			if len(res.Parts) >= int(maxParts) {
				res.IsTruncated = true
				res.NextPartNumberMarker = strconv.Itoa(res.Parts[len(res.Parts)-1].PartNumber)
				break
			}

			var p s3.UploadPart
			if err := rows.Scan(&p.PartNumber, &p.Size, (*sqlMD5)(&p.ContentMD5), (*sqlTime)(&p.LastModified)); err != nil {
				return err
			}
			res.Parts = append(res.Parts, p)
		}

		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return res, nil
}
