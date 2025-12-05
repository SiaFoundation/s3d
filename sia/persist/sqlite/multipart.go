package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3"
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
func (s *Store) AddMultipartPart(bucket, name, uploadID string, partNumber int) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO multipart_parts (multipart_upload_id, part_number, created_at, updated_at)
			VALUES ($1, $2, $3, $3)
			ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET updated_at = EXCLUDED.updated_at
		`, uid, partNumber, sqlTime(time.Now()))
		return err
	})
}

// FinishMultipartPart updates metadata for a multipart part in the store.
func (s *Store) FinishMultipartPart(bucket, name, uploadID string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		uid, err := multipartID(tx, uploadID, bid, name)
		if err != nil {
			return err
		}

		var sha256Bytes [32]byte
		if contentSHA256 != nil {
			sha256Bytes = *contentSHA256
		}

		_, err = tx.Exec(`
			UPDATE multipart_parts
			SET content_md5 = $1, content_sha256 = $2, content_length = $3, updated_at = $4
			WHERE multipart_upload_id = $5 AND part_number = $6
		`, sqlMD5(contentMD5), sqlSHA256(sha256Bytes), contentLength, sqlTime(time.Now()), uid, partNumber)
		return err
	})
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
			SELECT part_number, content_length, content_md5, updated_at
			FROM multipart_parts
			WHERE content_md5 IS NOT NULL AND multipart_upload_id = $1 AND part_number > $2
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

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}
