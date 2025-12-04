package sqlite

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
)

// DeleteObject deletes the object with the given bucket and name if it exists
// and all provided preconditions match.
func (s *Store) DeleteObject(accessKeyID, bucket string, objectID s3.ObjectID) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		if objectID.ETag != nil || objectID.LastModifiedTime != nil || objectID.Size != nil {
			var contentMD5 [16]byte
			var size int64
			var updatedAt time.Time
			err = tx.QueryRow("SELECT content_md5, size, updated_at FROM objects WHERE bucket_id = $1 AND name = $2", bid, objectID.Key).
				Scan((*sqlMD5)(&contentMD5), &size, (*sqlTime)(&updatedAt))
			if errors.Is(err, sql.ErrNoRows) {
				return nil // object doesn't exist, nothing to delete
			} else if err != nil {
				return err
			}

			if objectID.ETag != nil && *objectID.ETag != hex.EncodeToString(contentMD5[:]) {
				return s3errs.ErrPreconditionFailed
			}
			if objectID.Size != nil && *objectID.Size != size {
				return s3errs.ErrPreconditionFailed
			}
			if objectID.LastModifiedTime != nil && !updatedAt.Truncate(time.Second).Equal(objectID.LastModifiedTime.StdTime()) {
				return s3errs.ErrPreconditionFailed
			}
		}

		_, err = tx.Exec("DELETE FROM objects WHERE bucket_id = $1 AND name = $2", bid, objectID.Key)
		return err
	})
}

// GetObject retrieves the object with the given bucket and name.
func (s *Store) GetObject(accessKeyID *string, bucket, name string) (*objects.Object, error) {
	var obj objects.Object
	err := s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		var meta string
		err = tx.QueryRow(`
			SELECT object_id, content_md5, metadata, size, updated_at
			FROM objects
			WHERE bucket_id = $1 AND name = $2
		`, bid, name).
			Scan((*sqlHash256)(&obj.ID), (*sqlMD5)(&obj.ContentMD5), &meta,
				&obj.Size, (*sqlTime)(&obj.UpdatedAt))
		if errors.Is(err, sql.ErrNoRows) {
			return s3errs.ErrNoSuchKey
		} else if err != nil {
			return err
		}

		err = json.Unmarshal([]byte(meta), &obj.Meta)
		if err != nil {
			return errors.New("failed to unmarshal object metadata: " + err.Error())
		}
		return nil
	})
	return &obj, err
}

// PutObject stores the given object in the given bucket with the given name or
// overwrites it if it already exists.
func (s *Store) PutObject(accessKeyID, bucket, name string, obj *objects.Object) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		if obj.Meta == nil {
			obj.Meta = make(map[string]string) // force '{}' instead of 'null' in JSON
		}
		metaJson, err := json.Marshal(obj.Meta)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				object_id = excluded.object_id,
				content_md5 = excluded.content_md5,
				metadata = excluded.metadata,
				size = excluded.size,
				updated_at = excluded.updated_at
		`, bid, name, sqlHash256(obj.ID), sqlMD5(obj.ContentMD5),
			string(metaJson), obj.Size, sqlTime(obj.UpdatedAt))
		return err
	})
}
