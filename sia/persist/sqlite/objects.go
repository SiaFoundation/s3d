package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
)

// DeleteObject deletes the object with the given bucket and name.
func (s *Store) DeleteObject(accessKeyID, bucket, name string) error {
	return s.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}
		_, err = tx.Exec("DELETE FROM objects WHERE bucket_id = $1 AND name = $2", bid, name)
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
		meta_json, err := json.Marshal(obj.Meta)
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
			string(meta_json), obj.Size, sqlTime(obj.UpdatedAt))
		return err
	})
}
