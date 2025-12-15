package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
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

		res, err := tx.Exec(`DELETE FROM multipart_uploads WHERE upload_id = $1 AND bucket_id = $2 AND name = $3`,
			sqlUploadID(uploadID), bid, name)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return s3errs.ErrNoSuchUpload
		}
		return nil
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

		// Verify the multipart upload exists
		var exists bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE upload_id = $1 AND bucket_id = $2 AND name = $3)`,
			sqlUploadID(uploadID), bid, name).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return s3errs.ErrNoSuchUpload
		}

		var sha256Value any
		if contentSHA256 != nil {
			sha256Value = sqlHash256(*contentSHA256)
		}

		err = tx.QueryRow(`SELECT filename FROM multipart_parts WHERE multipart_upload_id = $1 AND part_number = $2`,
			sqlUploadID(uploadID), partNumber).Scan(&prevFilename)
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
		`, sqlUploadID(uploadID), partNumber, filename, sqlMD5(contentMD5), sha256Value, contentLength, sqlTime(time.Now()))
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

		var exists bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE upload_id = $1 AND bucket_id = $2 AND name = $3)`,
			sqlUploadID(uploadID), bid, name).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return s3errs.ErrNoSuchUpload
		}
		return nil
	})
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

		var exists bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE upload_id = $1 AND bucket_id = $2 AND name = $3)`,
			sqlUploadID(uploadID), bid, name).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return s3errs.ErrNoSuchUpload
		}

		rows, err := tx.Query(`
			SELECT part_number, content_length, content_md5, created_at
			FROM multipart_parts
			WHERE multipart_upload_id = $1 AND part_number > $2
			ORDER BY part_number ASC
			LIMIT $3
		`, sqlUploadID(uploadID), partNumberMarker, maxParts+1)
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

// ListMultipartUploads lists all multipart uploads for the given bucket and
// filters.
func (s *Store) ListMultipartUploads(bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error) {
	uploadIDMarker := page.UploadIDMarker

	// adjust marker if it falls inside a common prefix
	keyMarker := page.KeyMarker
	if adjustedKey, resetUploadID := adjustMarkerForCommonPrefix(prefix, keyMarker); resetUploadID {
		keyMarker = adjustedKey
		uploadIDMarker = [16]byte{}
	}

	// ignore upload ID marker if no key marker is set
	if keyMarker == "" {
		uploadIDMarker = [16]byte{}
	}

	// set default max uploads
	maxUploads := page.MaxUploads
	if maxUploads == 0 {
		maxUploads = 1000
	}

	if prefix.HasDelimiter {
		return s.listMultipartUploadsWithDelim(bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	}
	return s.listMultipartUploadsNoDelim(bucket, prefix.Prefix, keyMarker, uploadIDMarker, maxUploads)
}

func (s *Store) listMultipartUploadsWithDelim(bucket string, prefix s3.Prefix, keyMarker string, uploadIDMarker [16]byte, maxUploads int64) (*s3.ListMultipartUploadsResult, error) {
	res := &s3.ListMultipartUploadsResult{
		Uploads:        make([]s3.MultipartUploadInfo, 0, maxUploads),
		CommonPrefixes: make([]string, 0, maxUploads),
	}

	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		currentKeyMarker := keyMarker
		currentUploadIDMarker := uploadIDMarker

		for !res.IsTruncated {
			query, args := buildUploadsQuery(bid, prefix.Prefix, currentKeyMarker, currentUploadIDMarker, 100)
			rows, err := tx.Query(query, args...)
			if err != nil {
				return err
			}

			var lastMatchedPrefix string
			var foundRow bool
			for rows.Next() {
				foundRow = true
				var upload s3.MultipartUploadInfo
				if err := rows.Scan(&upload.Key, (*sqlUploadID)(&upload.UploadID), (*sqlTime)(&upload.Initiated)); err != nil {
					rows.Close()
					return err
				}

				commonPrefix := prefix.CommonPrefix(upload.Key)
				if commonPrefix != "" && commonPrefix != lastMatchedPrefix {
					res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix)
					lastMatchedPrefix = commonPrefix
					currentKeyMarker = commonPrefix + "\xFF"
					currentUploadIDMarker = [16]byte{}

					// set marker for next iteration
					if len(res.Uploads)+len(res.CommonPrefixes) >= int(maxUploads) {
						res.IsTruncated = true
						res.NextKeyMarker = currentKeyMarker
						res.NextUploadIDMarker = currentUploadIDMarker
						break
					}
					continue
				} else if commonPrefix == "" {
					res.Uploads = append(res.Uploads, upload)
					currentKeyMarker = upload.Key
					currentUploadIDMarker = upload.UploadID

					// set marker for next iteration
					if len(res.Uploads)+len(res.CommonPrefixes) >= int(maxUploads) {
						res.IsTruncated = true
						res.NextKeyMarker = currentKeyMarker
						res.NextUploadIDMarker = currentUploadIDMarker
						break
					}
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}

			if !foundRow {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Store) listMultipartUploadsNoDelim(bucket, prefix, keyMarker string, uploadIDMarker [16]byte, maxUploads int64) (*s3.ListMultipartUploadsResult, error) {
	res := &s3.ListMultipartUploadsResult{
		Uploads: make([]s3.MultipartUploadInfo, 0, maxUploads),
	}

	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		query, args := buildUploadsQuery(bid, prefix, keyMarker, uploadIDMarker, maxUploads+1)
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			if len(res.Uploads) == int(maxUploads) {
				res.IsTruncated = true
				last := res.Uploads[len(res.Uploads)-1]
				res.NextKeyMarker = last.Key
				res.NextUploadIDMarker = last.UploadID
				break
			}

			var upload s3.MultipartUploadInfo
			if err := rows.Scan(&upload.Key, (*sqlUploadID)(&upload.UploadID), (*sqlTime)(&upload.Initiated)); err != nil {
				return err
			}
			res.Uploads = append(res.Uploads, upload)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func buildUploadsQuery(bucketID int64, prefix, keyMarker string, uploadIDMarker [16]byte, limit int64) (string, []any) {
	where := []string{"bucket_id = ?"}
	args := []any{bucketID}

	// handle prefix
	if prefix != "" {
		where = append(where, "name >= ? AND name < ?")
		args = append(args, prefix, prefix+"\xFF")
	}

	// handle markers
	if keyMarker != "" && uploadIDMarker != [16]byte{} {
		where = append(where, "(name > ? OR (name = ? AND upload_id > ?))")
		args = append(args, keyMarker, keyMarker, sqlUploadID(uploadIDMarker))
	} else if keyMarker != "" {
		where = append(where, "name > ?")
		args = append(args, keyMarker)
	}

	query := fmt.Sprintf("SELECT name, upload_id, created_at FROM multipart_uploads WHERE %s ORDER BY name, upload_id LIMIT ?", strings.Join(where, " AND "))
	args = append(args, limit)
	return query, args
}

// adjustMarkerForCommonPrefix adjusts the key marker if it falls inside a common
// prefix. For example, if prefix="ac", delimiter="/", and marker="acb/x", this
// advances the marker past the "acb/" prefix so it isn't returned twice in
// paginated results.
func adjustMarkerForCommonPrefix(prefix s3.Prefix, keyMarker string) (adjustedKey string, resetUploadID bool) {
	if !prefix.HasDelimiter || keyMarker == "" {
		return keyMarker, false
	}

	markerRemainder := keyMarker
	var prefixLen int
	if after, ok := strings.CutPrefix(keyMarker, prefix.Prefix); ok {
		prefixLen = len(prefix.Prefix)
		markerRemainder = after
	}

	if idx := strings.Index(markerRemainder, prefix.Delimiter); idx != -1 {
		commonPrefix := keyMarker[:prefixLen+idx+len(prefix.Delimiter)]
		return commonPrefix + string([]byte{0xFF}), true
	}

	return keyMarker, false
}
