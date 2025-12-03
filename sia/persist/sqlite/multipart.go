package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
)

func parseUploadID(s string) (uid [16]byte, _ error) {
	if len(s) != 32 {
		return uid, fmt.Errorf("invalid length: got %d, want 32", len(s))
	} else if _, err := hex.Decode(uid[:], []byte(s)); err != nil {
		return uid, fmt.Errorf("failed to parse upload ID %q: %w", s, err)
	}
	return
}

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
	uid, err := parseUploadID(uploadID)
	if err != nil {
		return err
	}
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		res, err := tx.Exec(`
			DELETE FROM multipart_uploads
			WHERE bucket_id = $1 AND name = $2 AND upload_id = $3
		`, bid, name, sqlUploadID(uid))
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return s3errs.ErrNoSuchUpload
		}

		return nil
	})
}

// HasMultipartUpload returns an error if the multipart upload does not exist.
func (s *Store) HasMultipartUpload(bucket, name, uploadID string) error {
	uid, err := parseUploadID(uploadID)
	if err != nil {
		return err
	}
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM multipart_uploads
				WHERE bucket_id = $1 AND name = $2 AND upload_id = $3
			)
		`, bid, name, sqlUploadID(uid)).Scan(&exists); err != nil {
			return err
		} else if !exists {
			return s3errs.ErrNoSuchUpload
		}
		return nil
	})
}

// AddMultipartPart adds metadata for a multipart part to the store.
func (s *Store) AddMultipartPart(uploadID string, partNumber int) error {
	uid, err := parseUploadID(uploadID)
	if err != nil {
		return err
	}
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`
			INSERT INTO multipart_parts (upload_id, part_number, created_at)
			VALUES ($1, $2, $3)
			ON CONFLICT(upload_id, part_number) DO UPDATE SET created_at = EXCLUDED.created_at
		`, sqlUploadID(uid), partNumber, sqlTime(time.Now()))
		if err != nil {
			return fmt.Errorf("failed to insert multipart part: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return fmt.Errorf("no rows affected when inserting multipart part")
		}
		return nil
	})
}

// FinishMultipartPart updates metadata for a multipart part in the store.
func (s *Store) FinishMultipartPart(uploadID string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) error {
	uid, err := parseUploadID(uploadID)
	if err != nil {
		return err
	}
	return s.transaction(func(tx *txn) error {
		var sha256Bytes []byte
		if contentSHA256 != nil {
			sha256Bytes = contentSHA256[:]
		}
		res, err := tx.Exec(`
			UPDATE multipart_parts
			SET content_md5 = $1, content_sha256 = $2, content_length = $3
			WHERE upload_id = $4 AND part_number = $5
		`, sqlMD5(contentMD5), sqlSHA256(sha256Bytes), contentLength, sqlUploadID(uid), partNumber)
		if err != nil {
			return fmt.Errorf("failed to update multipart part: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return fmt.Errorf("no rows affected when updating multipart part")
		}
		return nil
	})
}
