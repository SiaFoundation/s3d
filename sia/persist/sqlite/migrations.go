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
		_, err := tx.Exec(`
CREATE TABLE object_parts_backup AS SELECT bucket_id, name, part_number, filename, content_md5, content_length, offset FROM object_parts;
DROP TABLE object_parts;

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

CREATE TABLE object_parts (
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    filename TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    FOREIGN KEY (bucket_id, name) REFERENCES objects(bucket_id, name) ON DELETE CASCADE,
    PRIMARY KEY (bucket_id, name, part_number)
);
INSERT INTO object_parts (bucket_id, name, part_number, filename, content_md5, content_length, offset)
    SELECT bucket_id, name, part_number, filename, content_md5, content_length, offset FROM object_parts_backup;
DROP TABLE object_parts_backup;

CREATE TABLE unpinned_objects (
    sia_object_id BLOB PRIMARY KEY,
    pin_before INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX unpinned_objects_next_attempt_at_idx ON unpinned_objects(next_attempt_at);
`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(`CREATE TABLE stats (
    stat TEXT PRIMARY KEY NOT NULL,
    stat_value INTEGER NOT NULL CHECK (stat_value >= 0)
)`); err != nil {
			return err
		}
		// backfill the stat counters from existing data. An object is pending
		// only while it has a filename and no sia_object_id; once uploaded it
		// keeps its filename as a backup until the pin completes, so uploaded
		// objects must be excluded from the pending counts.
		_, err := tx.Exec(`
			INSERT INTO stats (stat, stat_value)
			SELECT 'pending_objects', COUNT(CASE WHEN filename IS NOT NULL AND sia_object_id IS NULL THEN 1 END) FROM objects
			UNION ALL SELECT 'pending_size', COALESCE(SUM(CASE WHEN filename IS NOT NULL AND sia_object_id IS NULL THEN size END), 0) FROM objects
			UNION ALL SELECT 'uploaded_objects', COUNT(sia_object_id) FROM objects
			UNION ALL SELECT 'uploaded_size', COALESCE(SUM(CASE WHEN sia_object_id IS NOT NULL THEN size END), 0) FROM objects
			UNION ALL SELECT 'unpinned_objects', (SELECT COUNT(*) FROM unpinned_objects)
			UNION ALL SELECT 'orphaned_objects', (SELECT COUNT(*) FROM orphaned_objects)
			UNION ALL SELECT 'multipart_uploads', (SELECT COUNT(*) FROM multipart_uploads)`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`
			CREATE TABLE bucket_lifecycle_configurations (
				bucket_id INTEGER PRIMARY KEY,
				configuration TEXT NOT NULL,
				FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
			);
			CREATE INDEX IF NOT EXISTS objects_bucket_id_updated_at_idx ON objects(bucket_id, updated_at);
			CREATE INDEX IF NOT EXISTS multipart_uploads_bucket_id_created_at_idx ON multipart_uploads(bucket_id, created_at);`)
		return err
	},
	// add object versioning. objects keys on (bucket_id, name, version_id)
	// with a monotonic seq (current version = MAX(seq) per key), an
	// is_delete_marker flag, and an is_latest flag marking the current version
	// (maintained on write so current-version listing is a plain index scan);
	// object_parts is rebuilt to carry version_id. Pre-existing rows become the
	// null version (''), each the sole and current version of its key.
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`
CREATE TABLE object_parts_backup AS SELECT bucket_id, name, part_number, filename, content_md5, content_length, offset FROM object_parts;
DROP TABLE object_parts;

CREATE TABLE objects_new (
    bucket_id INTEGER REFERENCES buckets(id) NOT NULL,
    name TEXT NOT NULL,
    version_id TEXT NOT NULL DEFAULT '',
    seq INTEGER NOT NULL,
    is_delete_marker INTEGER NOT NULL DEFAULT FALSE,
    is_latest INTEGER NOT NULL DEFAULT TRUE,
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
    CHECK (is_delete_marker IN (FALSE, TRUE)),
    CHECK (is_latest IN (FALSE, TRUE)),
    PRIMARY KEY (bucket_id, name, version_id)
) WITHOUT ROWID;
INSERT INTO objects_new (bucket_id, name, version_id, seq, is_delete_marker, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id, sia_object)
    SELECT bucket_id, name, '', ROW_NUMBER() OVER (ORDER BY bucket_id, name), FALSE, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id, sia_object FROM objects;
DROP TABLE objects;
ALTER TABLE objects_new RENAME TO objects;
CREATE INDEX objects_sia_object_id_idx ON objects(sia_object_id);
CREATE INDEX objects_filename_idx ON objects(filename) WHERE filename IS NOT NULL;
CREATE INDEX objects_bucket_id_updated_at_idx ON objects(bucket_id, updated_at);
CREATE INDEX objects_bucket_name_seq_idx ON objects(bucket_id, name, seq DESC);
CREATE UNIQUE INDEX objects_is_latest_idx ON objects(bucket_id, name) WHERE is_latest = TRUE;

CREATE TABLE object_parts (
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    version_id TEXT NOT NULL DEFAULT '',
    part_number INTEGER NOT NULL,
    filename TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    FOREIGN KEY (bucket_id, name, version_id) REFERENCES objects(bucket_id, name, version_id) ON DELETE CASCADE,
    PRIMARY KEY (bucket_id, name, version_id, part_number)
);
INSERT INTO object_parts (bucket_id, name, version_id, part_number, filename, content_md5, content_length, offset)
    SELECT bucket_id, name, '', part_number, filename, content_md5, content_length, offset FROM object_parts_backup;
DROP TABLE object_parts_backup;

ALTER TABLE buckets ADD COLUMN versioning_status TEXT NOT NULL DEFAULT '' CHECK (versioning_status IN ('', 'Enabled', 'Suspended'));`)
		return err
	},
}
