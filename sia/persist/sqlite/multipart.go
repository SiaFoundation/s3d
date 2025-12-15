package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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

// ListMultipartUploads lists all multipart uploads for the given bucket and
// filters.
func (s *Store) ListMultipartUploads(bucket, prefix, delimiter, keyMarker string, uploadIDMarker s3.UploadID, maxUploads int64) (*s3.ListMultipartUploadsResult, error) {
	if keyMarker == "" {
		uploadIDMarker = [16]byte{} // ignored if key marker is not set
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
			uploadIDMarker = [16]byte{}
		}
	}

	// helper to compute common prefix from upload key
	commonPrefix := func(key string) string {
		after, ok := strings.CutPrefix(key, prefix)
		if !ok {
			return ""
		}

		idx := strings.IndexRune(after, rune(delimiter[0]))
		if idx == -1 {
			return ""
		}

		return prefix + after[:idx+utf8.RuneCountInString(delimiter)]
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
		query, args := buildUploadsQuery(bid, prefix, delimiter, keyMarker, uploadIDMarker)

		// build common prefixes query if needed
		if delimiter != "" {
			query2, args2 := buildCommonPrefixesQuery(bid, prefix, delimiter, keyMarker, uploadIDMarker)
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

		var prev *s3.MultipartUploadInfo
		for rows.Next() {
			if len(res.Uploads)+len(res.CommonPrefixes) == int(maxUploads) {
				if prev != nil {
					res.IsTruncated = true
					res.NextKeyMarker = prev.Key
					res.NextUploadIDMarker = prev.UploadID
				}
				break
			}

			upload, isPrefix, err := scanMultipartUpload(rows)
			if err != nil {
				return err
			}
			prev = &upload

			if isPrefix {
				res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix(upload.Key))
			} else {
				res.Uploads = append(res.Uploads, upload)
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
		where = append(where, "name >= ? AND name < ?")
		args = append(args, prefix, prefix+"\xFF")
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
	where = append(where, "name >= ? AND name < ? AND INSTR(SUBSTR(name, ?), ?) > 0")
	args = append(args, prefix, prefix+"\xFF", searchOffset, delimiter)

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

func scanMultipartUpload(s scanner) (s3.MultipartUploadInfo, bool, error) {
	var info s3.MultipartUploadInfo
	var isPrefix bool
	if err := s.Scan(&info.Key, (*sqlUploadID)(&info.UploadID), (*sqlTime)(&info.Initiated), &isPrefix); err != nil {
		return s3.MultipartUploadInfo{}, false, err
	}
	return info, isPrefix, nil
}
