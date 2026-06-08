package sqlite

import (
	"go.uber.org/zap"
)

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx *txn, log *zap.Logger) error{
	func(tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(`CREATE INDEX IF NOT EXISTS objects_filename_idx ON objects(filename) WHERE filename IS NOT NULL`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`ALTER TABLE global_settings ADD COLUMN indexer_url TEXT;`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`ALTER TABLE objects ADD COLUMN parts_count INTEGER NOT NULL DEFAULT 0`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE objects SET parts_count = (SELECT COUNT(*) FROM object_parts WHERE object_parts.bucket_id = objects.bucket_id AND object_parts.name = objects.name)`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		// Rebuild objects to relax the CHECK constraint: filename and
		// sia_object_id may both be set when an object has been uploaded
		// to Sia but not yet pinned (file is kept on disk as a backup
		// until the pin completes).
		_, err := tx.Exec(`
CREATE TABLE objects_new (
    bucket_id INTEGER REFERENCES buckets(id) NOT NULL,
    name TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    parts_count INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    filename TEXT,
    sia_object_id BLOB,
    sia_object BLOB,
    CHECK ((sia_object_id IS NULL AND sia_object IS NULL) OR (sia_object_id IS NOT NULL AND sia_object IS NOT NULL)),
    CHECK ((size = 0 AND filename IS NULL AND sia_object_id IS NULL) OR (size > 0 AND (filename IS NOT NULL OR sia_object_id IS NOT NULL))),
    PRIMARY KEY (bucket_id, name)
) WITHOUT ROWID;
INSERT INTO objects_new (bucket_id, name, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id, sia_object)
    SELECT bucket_id, name, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id, sia_object FROM objects;
DROP TABLE objects;
ALTER TABLE objects_new RENAME TO objects;
CREATE INDEX objects_sia_object_id_idx ON objects(sia_object_id);
CREATE INDEX objects_filename_idx ON objects(filename) WHERE filename IS NOT NULL;

CREATE TABLE unpinned_objects (
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    pin_before INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (bucket_id, name) REFERENCES objects(bucket_id, name) ON DELETE CASCADE,
    PRIMARY KEY (bucket_id, name)
);
CREATE INDEX unpinned_objects_next_attempt_at_idx ON unpinned_objects(next_attempt_at);
`)
		return err
	},
}
