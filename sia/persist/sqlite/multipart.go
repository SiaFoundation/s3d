package sqlite

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// UploadID is a unique identifier for a multipart upload.
type UploadID [16]byte

// NewUploadID generates a new random UploadID.
func NewUploadID() UploadID {
	return frand.Entropy128()
}

// ParseUploadID parses a hex string into an UploadID.
func ParseUploadID(s string) (UploadID, error) {
	if len(s) != 32 {
		return UploadID{}, fmt.Errorf("invalid upload id length: got %d, want 32", len(s))
	}

	var uid UploadID
	if _, err := hex.Decode(uid[:], []byte(s)); err != nil {
		return uid, fmt.Errorf("failed to parse UploadID %q: %w", s, err)
	}

	return uid, nil
}

// String returns the hex string representation of the UploadID.
func (uid UploadID) String() string {
	return hex.EncodeToString(uid[:])
}

// CreateMultipartUpload persists metadata for a new multipart upload and
// returns a random upload ID.
func (s *Store) CreateMultipartUpload(bucket, name string, meta map[string]string) (string, error) {
	// encode metadata
	encodedMeta, err := encodeMeta(meta)
	if err != nil {
		return "", fmt.Errorf("failed to encode metadata: %w", err)
	}

	// insert upload
	uid := NewUploadID()
	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(`
				INSERT INTO multipart_uploads (upload_id, bucket_id, name, sia_meta, created_at)
				VALUES ($1, $2, $3, $4, $5)
			`, uid, bid, name, encodedMeta, time.Now().Unix()); err != nil {
			return fmt.Errorf("failed to insert multipart upload: %w", err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return uid.String(), nil
}

// AbortMultipartUpload removes a multipart upload from the store.
func (s *Store) AbortMultipartUpload(bucket, name, uploadID string) error {
	uid, err := ParseUploadID(uploadID)
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
		`, bid, name, uid)
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

func encodeMeta(meta map[string]string) ([]byte, error) {
	if meta == nil {
		meta = map[string]string{}
	}

	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	enc.WriteUint64(uint64(len(meta)))
	keys := slices.Collect(maps.Keys(meta))
	sort.Strings(keys)
	for _, k := range keys {
		enc.WriteString(k)
		enc.WriteString(meta[k])
	}
	if err := enc.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
