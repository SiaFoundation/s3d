package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
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

		if _, err := tx.Exec(`
				INSERT INTO multipart_uploads (upload_id, bucket_id, name, metadata, created_at)
				VALUES ($1, $2, $3, $4, $5)
			`, sqlUploadID(uploadID), bid, name, sqlMetaJSON(meta), sqlTime(time.Now())); err != nil {
			return fmt.Errorf("failed to insert multipart upload: %w", err)
		}
		return nil
	})
}

// CompleteMultipartUpload finalizes a multipart upload by creating the object
// and transferring parts from the upload to the object. If the overwritten
// object's ID has no remaining references, it is inserted into the
// orphaned_objects table.
func (s *Store) CompleteMultipartUpload(bucket, name string, uploadID s3.UploadID, objectID *types.Hash256, filename *string, contentMD5 [16]byte, contentLength int64) (*string, error) {
	var prevFilename *string
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

		// validate parts exist
		var partCount, maxPartNumber int
		var totalSize int64
		err = tx.QueryRow(`
			SELECT COUNT(*), MAX(part_number), SUM(content_length)
			FROM multipart_parts
			WHERE upload_id = $1
		`, sqlUploadID(uploadID)).Scan(&partCount, &maxPartNumber, &totalSize)
		if err != nil {
			return err
		} else if partCount == 0 {
			return errors.New("cannot complete multipart upload with no parts")
		} else if totalSize != contentLength {
			return fmt.Errorf("total part size (%d) does not match content length (%d)", totalSize, contentLength)
		}

		// verify all parts except last meet minimum size
		var smallParts int
		err = tx.QueryRow(`
			SELECT COUNT(*)
			FROM multipart_parts
			WHERE upload_id = $1
			  AND part_number < $2
			  AND content_length < $3
		`, sqlUploadID(uploadID), maxPartNumber, s3.MinUploadPartSize).Scan(&smallParts)
		if err != nil {
			return err
		}
		if smallParts > 0 {
			return fmt.Errorf("found %d parts smaller than minimum size (%d bytes)", smallParts, s3.MinUploadPartSize)
		}

		oldID, oldFilename, err := objectInfo(tx, bucket, name)
		prevFilename = oldFilename
		if err != nil {
			return err
		}

		// delete any existing parts for the object
		_, err = tx.Exec(`DELETE FROM object_parts WHERE bucket_id = $1 AND name = $2`, bid, name)
		if err != nil {
			return err
		}

		// upsert object with metadata from multipart upload
		_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at, sia_object, cached_at, filename)
			SELECT bucket_id, name, $1, $2, metadata, $3, $4, x'', $5, $6
			FROM multipart_uploads
			WHERE upload_id = $7
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				object_id = excluded.object_id,
				content_md5 = excluded.content_md5,
				metadata = excluded.metadata,
				size = excluded.size,
				updated_at = excluded.updated_at,
				sia_object = excluded.sia_object,
				cached_at = excluded.cached_at,
				filename = excluded.filename
		`, (*sqlHash256)(objectID), sqlMD5(contentMD5), contentLength, sqlTime(time.Now()), sqlTime(time.Time{}), filename, sqlUploadID(uploadID))
		if err != nil {
			return err
		}

		// clear any stale orphan entry for the new object ID, in case it was
		// previously orphaned and is now referenced again
		if objectID != nil {
			if _, err := tx.Exec("DELETE FROM orphaned_objects WHERE object_id = $1", sqlHash256(*objectID)); err != nil {
				return err
			}
		}

		// orphan the old object if it differs from the new one
		if oldID != nil && (objectID == nil || *oldID != *objectID) {
			if err := insertOrphan(tx, *oldID); err != nil {
				return err
			}
		}

		// move parts to object_parts
		_, err = tx.Exec(`
			INSERT INTO object_parts (bucket_id, name, part_number, content_md5, content_length, offset)
			SELECT $1, $2, part_number, content_md5, content_length,
				(SELECT COALESCE(SUM(content_length), 0)
				FROM multipart_parts mp
				WHERE mp.upload_id = $3 AND mp.part_number < multipart_parts.part_number)
			FROM multipart_parts
			WHERE upload_id = $3
		`, bid, name, sqlUploadID(uploadID))
		if err != nil {
			return err
		}

		// delete the multipart upload
		_, err = tx.Exec(`DELETE FROM multipart_uploads WHERE upload_id = $1`, sqlUploadID(uploadID))
		return err
	}); err != nil {
		return nil, err
	}
	return prevFilename, nil
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
func (s *Store) AddMultipartPart(bucket, name string, uploadID s3.UploadID, filename string, partNumber int, contentMD5 [16]byte, contentLength int64) (string, error) {
	var prevFilename string
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

		err = tx.QueryRow(`SELECT filename FROM multipart_parts WHERE upload_id = $1 AND part_number = $2`,
			sqlUploadID(uploadID), partNumber).Scan(&prevFilename)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO multipart_parts (upload_id, part_number, filename, content_md5, content_length, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(upload_id, part_number) DO UPDATE SET
				filename       = EXCLUDED.filename,
				content_md5    = EXCLUDED.content_md5,
				content_length = EXCLUDED.content_length,
				created_at     = EXCLUDED.created_at
		`, sqlUploadID(uploadID), partNumber, filename, sqlMD5(contentMD5), contentLength, sqlTime(time.Now()))
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

// MultipartParts returns the parts belonging to the specified multipart upload.
func (s *Store) MultipartParts(bucket, name string, uploadID s3.UploadID) ([]objects.Part, error) {
	var parts []objects.Part
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
			SELECT part_number, filename, content_md5, content_length
			FROM multipart_parts
			WHERE upload_id = $1
			ORDER BY part_number ASC`, sqlUploadID(uploadID))
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
			WHERE upload_id = $1 AND part_number > $2
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
func (s *Store) ListMultipartUploads(bucket string, prefix s3.Prefix, page s3.ListMultipartUploadsPage) (_ *s3.ListMultipartUploadsResult, err error) {
	// parse upload ID marker
	var uploadIDMarker s3.UploadID
	if page.UploadIDMarker != "" {
		uploadIDMarker, err = s3.ParseUploadID(page.UploadIDMarker)
		if err != nil {
			return nil, s3errs.ErrInvalidArgument
		}
	}

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

	// without a delimiter we can fetch all results in a single query since we
	// won't need to skip over common prefixes
	batchLimit := int64(100)
	if !prefix.HasDelimiter {
		batchLimit = page.MaxUploads + 1
	}

	res := &s3.ListMultipartUploadsResult{
		Uploads:        make([]s3.MultipartUploadInfo, 0, page.MaxUploads),
		CommonPrefixes: make([]string, 0, page.MaxUploads),
	}

	err = s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		currentKeyMarker := keyMarker
		currentUploadIDMarker := uploadIDMarker

		for !res.IsTruncated {
			query, args := buildUploadsQuery(bid, prefix.Prefix, currentKeyMarker, currentUploadIDMarker, batchLimit)
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

					if len(res.Uploads)+len(res.CommonPrefixes) >= int(page.MaxUploads) {
						res.IsTruncated = true
						res.NextKeyMarker = currentKeyMarker
						res.NextUploadIDMarker = currentUploadIDMarker.String()
						break
					}
					continue
				} else if commonPrefix == "" {
					res.Uploads = append(res.Uploads, upload)
					currentKeyMarker = upload.Key
					currentUploadIDMarker = upload.UploadID

					if len(res.Uploads)+len(res.CommonPrefixes) >= int(page.MaxUploads) {
						res.IsTruncated = true
						res.NextKeyMarker = currentKeyMarker
						res.NextUploadIDMarker = currentUploadIDMarker.String()
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
