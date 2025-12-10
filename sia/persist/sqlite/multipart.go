package sqlite

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

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
func (s *Store) AddMultipartPart(bucket, name, uploadID, location string, partNumber int, contentMD5 [16]byte, contentSHA256 *[32]byte, contentLength int64) (string, error) {
	var previousLocation string
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
		err = tx.QueryRow(`SELECT location FROM multipart_parts WHERE multipart_upload_id = $1 AND part_number = $2`, uid, partNumber).Scan(&prev)
		if err != nil && err != sql.ErrNoRows {
			return err
		} else if prev.Valid {
			previousLocation = prev.String
		}

		_, err = tx.Exec(`
			INSERT INTO multipart_parts (multipart_upload_id, part_number, location, content_md5, content_sha256, content_length, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET
				location       = EXCLUDED.location,
				content_md5    = EXCLUDED.content_md5,
				content_sha256 = EXCLUDED.content_sha256,
				content_length = EXCLUDED.content_length,
				created_at     = EXCLUDED.created_at
		`, uid, partNumber, location, sqlMD5(contentMD5), sha256Value, contentLength, sqlTime(time.Now()))
		return err
	}); err != nil {
		return "", err
	}
	return previousLocation, nil
}

// ListMultipartUploads lists all multipart uploads for the given bucket and
// filters.
func (s *Store) ListMultipartUploads(bucket string, prefix, delimiter, keyMarker, uploadIDMarker string, maxUploads int64) (*s3.ListMultipartUploadsResult, error) {
	if keyMarker == "" {
		uploadIDMarker = "" // ignored if key marker is not set
	}

	// if the marker falls inside a common prefix (e.g. prefix "ac", delimiter
	// "/", marker "acb/x"), advance past that prefix so it isn't returned
	// twice
	if delimiter != "" && keyMarker != "" {
		markerRemainder := keyMarker
		var prefixLen int
		if after, ok := strings.CutPrefix(keyMarker, prefix); ok {
			prefixLen = len(prefix)
			markerRemainder = after
		}
		if idx := strings.Index(markerRemainder, delimiter); idx != -1 {
			commonPrefix := keyMarker[:prefixLen+idx+len(delimiter)]
			keyMarker = commonPrefix + string([]byte{0xFF})
			uploadIDMarker = ""
		}
	}

	uidMarker, err := parseUploadID(uploadIDMarker)
	if err != nil {
		return nil, fmt.Errorf("invalid upload ID marker: %w", err)
	}

	res := &s3.ListMultipartUploadsResult{
		Uploads:        make([]s3.MultipartUploadInfo, 0, maxUploads),
		CommonPrefixes: make([]string, 0, maxUploads),
	}

	if err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		// build uploads query
		query, args := buildUploadsQuery(bid, prefix, delimiter, keyMarker, uidMarker)

		// build common prefixes query if needed
		if delimiter != "" {
			query2, args2 := buildCommonPrefixesQuery(bid, prefix, delimiter, keyMarker, uidMarker)
			query = fmt.Sprintf(`
				WITH uploads AS (%s), prefixes AS (%s)
				SELECT name, upload_id, created_at, is_prefix FROM uploads
				UNION ALL
				SELECT name, upload_id, created_at, is_prefix FROM prefixes`, query, query2)
			args = append(args, args2...)
		}

		// order and limit
		query += " ORDER BY name, upload_id"
		query += " LIMIT ?"
		args = append(args, maxUploads+1)

		// collect results
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		var prev *multipartUploadInfo
		for rows.Next() {
			if len(res.Uploads)+len(res.CommonPrefixes) == int(maxUploads) {
				if prev != nil {
					res.IsTruncated = true
					res.NextKeyMarker = prev.Key
					res.NextUploadIDMarker = prev.UploadID
				}
				break
			}

			upload, err := scanMultipartUpload(rows)
			if err != nil {
				return err
			}
			prev = &upload

			if upload.IsPrefix {
				res.CommonPrefixes = append(res.CommonPrefixes, upload.CommonPrefix(prefix, delimiter))
			} else {
				res.Uploads = append(res.Uploads, upload.MultipartUploadInfo)
			}
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return res, nil
}

func buildUploadsQuery(bucketID int64, prefix, delimiter, keyMarker string, uploadIDMarker [16]byte) (string, []any) {
	var (
		hasPrefix    = prefix != ""
		hasDelim     = delimiter != ""
		prefixLen    = utf8.RuneCountInString(prefix)
		searchOffset = prefixLen + 1
	)

	// check bucket
	where := []string{"bucket_id = ?"}
	args := []any{bucketID}

	// handle prefix
	if hasPrefix {
		where = append(where, "SUBSTR(name, 1, ?) = ?")
		args = append(args, prefixLen, prefix)
	}

	// handle delimiter
	if hasDelim {
		if hasPrefix {
			// when we know there's a prefix, start searching after it
			where = append(where, "INSTR(SUBSTR(name, ?), ?) = 0")
			args = append(args, searchOffset, delimiter)
		} else {
			// no prefix, just ensure delimiter not in the whole name
			where = append(where, "INSTR(name, ?) = 0")
			args = append(args, delimiter)
		}
	}

	// check markers
	if keyMarker != "" && uploadIDMarker != [16]byte{} {
		where = append(where, "(name > ? OR (name = ? AND upload_id > ?))")
		args = append(args, keyMarker, keyMarker, sqlUploadID(uploadIDMarker))
	} else if keyMarker != "" {
		where = append(where, "name > ?")
		args = append(args, keyMarker)
	}

	return fmt.Sprintf(`SELECT name, upload_id, created_at, FALSE as is_prefix FROM multipart_uploads WHERE %s`, strings.Join(where, " AND ")), args
}

func buildCommonPrefixesQuery(bucketID int64, prefix, delimiter, keyMarker string, uploadIDMarker [16]byte) (_ string, args []any) {
	var (
		prefixLen    = utf8.RuneCountInString(prefix)
		searchOffset = prefixLen + 1
	)

	// search delimiter after prefix
	args = append(args, searchOffset, delimiter, prefixLen)

	// check bucket
	where := []string{"bucket_id = ?"}
	args = append(args, bucketID)

	// check prefix
	where = append(where, "SUBSTR(name, 1, ?) = ? AND INSTR(SUBSTR(name, ?), ?) > 0")
	args = append(args, prefixLen, prefix, searchOffset, delimiter)

	// check markers
	if keyMarker != "" && uploadIDMarker != [16]byte{} {
		where = append(where, "(name > ? OR (name = ? AND upload_id > ?))")
		args = append(args, keyMarker, keyMarker, sqlUploadID(uploadIDMarker))
	} else if keyMarker != "" {
		where = append(where, "name > ?")
		args = append(args, keyMarker)
	}

	return fmt.Sprintf(`
		SELECT name, upload_id, created_at, TRUE as is_prefix FROM (
			SELECT
				name,
				upload_id,
				created_at,
				ROW_NUMBER() OVER (
					PARTITION BY SUBSTR(name, 1, INSTR(SUBSTR(name, ?), ?) + ?)
					ORDER BY name, upload_id
				) as row
			FROM multipart_uploads
			WHERE %s
		) WHERE row = 1`, strings.Join(where, " AND ")), args
}

type multipartUploadInfo struct {
	s3.MultipartUploadInfo
	IsPrefix bool
}

func (m multipartUploadInfo) CommonPrefix(prefix, delimiter string) string {
	after, ok := strings.CutPrefix(m.Key, prefix)
	if !ok {
		return ""
	}

	idx := strings.IndexRune(after, rune(delimiter[0]))
	if idx == -1 {
		return ""
	}

	return prefix + after[:idx+utf8.RuneCountInString(delimiter)]
}

func scanMultipartUpload(s scanner) (multipartUploadInfo, error) {
	var uid sqlUploadID
	var name string
	var initiated time.Time
	var isPrefix bool
	if err := s.Scan(&name, &uid, (*sqlTime)(&initiated), &isPrefix); err != nil {
		return multipartUploadInfo{}, err
	}
	return multipartUploadInfo{
		MultipartUploadInfo: s3.MultipartUploadInfo{
			Key:       name,
			UploadID:  hex.EncodeToString(uid[:]),
			Initiated: initiated,
		},
		IsPrefix: isPrefix,
	}, nil
}
