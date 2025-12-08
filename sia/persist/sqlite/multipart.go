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
