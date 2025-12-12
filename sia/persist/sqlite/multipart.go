package sqlite

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/multipart"
	"lukechampine.com/frand"
)

// CreateMultipartUpload persists metadata for a new multipart upload and
// returns a random upload ID.
func (s *Store) CreateMultipartUpload(bucket, name string, meta map[string]string) (string, error) {
	uid := frand.Entropy128()
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		if meta == nil {
			meta = make(map[string]string) // force '{}' instead of 'null' in JSON
		}
		metaJson, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(`
				INSERT INTO multipart_uploads (upload_id, bucket_id, name, metadata, created_at)
				VALUES ($1, $2, $3, $4, $5)
			`, sqlUploadID(uid), bid, name, string(metaJson), sqlTime(time.Now())); err != nil {
			return fmt.Errorf("failed to insert multipart upload: %w", err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return hex.EncodeToString(uid[:]), nil
}

// CompleteMultipartUpload finalizes a multipart upload by creating the final
// object and removing the multipart upload from the store.
func (s *Store) CompleteMultipartUpload(bucket, name, uploadID string, parts []multipart.Part) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		var objID int64
		err = tx.QueryRow(`SELECT id FROM objects WHERE bucket_id = $1 AND name = $2`, bid, name).Scan(&objID)
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchKey
		} else if err != nil {
			return err
		}

		var offset int64
		for _, part := range parts {
			_, err = tx.Exec(`INSERT INTO object_parts (object_id, part_number, content_md5, offset, length) VALUES ($1, $2, $3, $4, $5)`,
				objID, part.PartNumber, sqlMD5(part.MD5), offset, part.Size)
			if err != nil {
				return err
			}
			offset += part.Size
		}

		_, err = tx.Exec(`DELETE FROM multipart_uploads WHERE id = $1`, uid)
		return err
	})
}

// AbortMultipartUpload removes a multipart upload from the store.
func (s *Store) AbortMultipartUpload(bucket, name, uploadID string) error {
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
func (s *Store) AddMultipartPart(bucket, name, uploadID, filename string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) (string, error) {
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

		var sha256Value any
		if contentSHA256 != nil {
			sha256Value = sqlHash256(*contentSHA256)
		}

		var prev sql.NullString
		err = tx.QueryRow(`SELECT filename FROM multipart_parts WHERE multipart_upload_id = $1 AND part_number = $2`, uid, partNumber).Scan(&prev)
		if err != nil && err != sql.ErrNoRows {
			return err
		} else if prev.Valid {
			prevFilename = prev.String
		}

		_, err = tx.Exec(`
			INSERT INTO multipart_parts (multipart_upload_id, part_number, filename, content_md5, content_sha256, content_length, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET
				filename       = EXCLUDED.filename,
				content_md5    = EXCLUDED.content_md5,
				content_sha256 = EXCLUDED.content_sha256,
				content_length = EXCLUDED.content_length,
				created_at     = EXCLUDED.created_at
		`, uid, partNumber, filename, sqlMD5(contentMD5), sha256Value, contentLength, sqlTime(time.Now()))
		return err
	}); err != nil {
		return "", err
	}
	return prevFilename, nil
}

// HasMultipartUpload checks if a multipart upload exists.
func (s *Store) HasMultipartUpload(bucket, name, uploadID string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		_, err = multipartID(tx, uploadID, bid, name)
		return err
	})
}

// MultipartUpload returns the parts belonging to the specified multipart
// upload.
func (s *Store) MultipartUpload(bucket, name, uploadID string) (multipart.Upload, error) {
	upload := multipart.Upload{Parts: make([]multipart.Part, 0, s3.MaxUploadListParts)}
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		var meta string
		if err := tx.QueryRow(`SELECT metadata FROM multipart_uploads WHERE id = $1`, uid).Scan(&meta); err != nil {
			return err
		} else if err := json.Unmarshal([]byte(meta), &upload.Meta); err != nil {
			return err
		}

		rows, err := tx.Query(`
			SELECT part_number, filename, content_md5, content_length
			FROM multipart_parts 
			WHERE multipart_upload_id = $1 
			ORDER BY part_number ASC`, uid)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var part multipart.Part
			if err := rows.Scan(&part.PartNumber, &part.Filename, (*sqlMD5)(&part.MD5), &part.Size); err != nil {
				return err
			}
			upload.Parts = append(upload.Parts, part)
		}
		return rows.Err()
	}); err != nil {
		return multipart.Upload{}, err
	}
	return upload, nil
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Store) ListParts(accessKeyID, bucket, name, uploadID string, partNumberMarker int, maxParts int64) (*s3.ListPartsResult, error) {
	res := &s3.ListPartsResult{
		OwnerID:              "", // TODO: sia backend does not yet support owners
		InitiatorID:          accessKeyID,
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
			FROM multipart_parts
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
