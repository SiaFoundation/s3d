package sqlite

import (
	"go.sia.tech/indexd/slabs"
)

func (s *Store) PutObject(bucket, name string, obj slabs.SealedObject) error {
	return s.transaction(func(t *txn) error {
		bid, err := bucketID(t, bucket)
		if err != nil {
			return err
		}
		encoded, err := obj.MarshalSia()
		if err != nil {
			return err
		}

		_, err = t.Exec(`
			INSERT INTO objects (bucket_id, name, sia_meta)
			VALUES ($1, $2, $3)
			ON CONFLICT(bucket_id, name) DO UPDATE SET
				checksum_md5 = excluded.checksum_md5,
				size = excluded.size
		`, bid, obj, encoded)
		return err
	})
}
