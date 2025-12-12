package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
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
func (s *Store) AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) (string, error) {
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

		err = tx.QueryRow(`SELECT filename FROM multipart_parts WHERE multipart_upload_id = $1 AND part_number = $2`, uid, partNumber).Scan(&prevFilename)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
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
