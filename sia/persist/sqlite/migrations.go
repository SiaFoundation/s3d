package sqlite

import (
	"fmt"

	"go.sia.tech/core/types"
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
	// normalize the sia_object blob on the objects table: sealed objects move
	// into sia_objects, their slab slices into sia_slab_slices, deduplicated
	// slabs into sia_slabs (with an initial version of 0) and the slabs'
	// sectors into sia_slab_sectors. The blob column is dropped from objects
	// and sia_object_id becomes a reference into sia_objects.
	func(tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(`
CREATE TABLE sia_objects (
    id BLOB PRIMARY KEY,
    encrypted_data_key BLOB NOT NULL,
    data_signature BLOB NOT NULL,
    encrypted_metadata_key BLOB,
    encrypted_metadata BLOB,
    metadata_signature BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE sia_slabs (
    id BLOB PRIMARY KEY,
    encryption_key BLOB NOT NULL,
    min_shards INTEGER NOT NULL,
    version INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE sia_slab_slices (
    sia_object_id BLOB NOT NULL,
    slice_index INTEGER NOT NULL,
    slab_id BLOB NOT NULL,
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    FOREIGN KEY (sia_object_id) REFERENCES sia_objects(id) ON DELETE CASCADE,
    FOREIGN KEY (slab_id) REFERENCES sia_slabs(id) ON DELETE CASCADE,
    PRIMARY KEY (sia_object_id, slice_index)
) WITHOUT ROWID;
CREATE INDEX sia_slab_slices_slab_id_idx ON sia_slab_slices(slab_id);

CREATE TABLE sia_slab_sectors (
    slab_id BLOB NOT NULL,
    sector_index INTEGER NOT NULL,
    root BLOB NOT NULL,
    host_key BLOB NOT NULL,
    FOREIGN KEY (slab_id) REFERENCES sia_slabs(id) ON DELETE CASCADE,
    PRIMARY KEY (slab_id, sector_index)
) WITHOUT ROWID;`); err != nil {
			return err
		}

		objStmt, err := tx.Prepare(`
			INSERT INTO sia_objects (id, encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
		if err != nil {
			return err
		}
		defer objStmt.Close()
		slabStmt, err := tx.Prepare(`
			INSERT INTO sia_slabs (id, encryption_key, min_shards, version)
			VALUES ($1, $2, $3, 0)
			ON CONFLICT (id) DO NOTHING`)
		if err != nil {
			return err
		}
		defer slabStmt.Close()
		sliceStmt, err := tx.Prepare(`
			INSERT INTO sia_slab_slices (sia_object_id, slice_index, slab_id, offset, length)
			VALUES ($1, $2, $3, $4, $5)`)
		if err != nil {
			return err
		}
		defer sliceStmt.Close()
		sectorStmt, err := tx.Prepare(`
			INSERT INTO sia_slab_sectors (slab_id, sector_index, root, host_key)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (slab_id, sector_index) DO UPDATE SET host_key = excluded.host_key`)
		if err != nil {
			return err
		}
		defer sectorStmt.Close()

		rows, err := tx.Query(`SELECT sia_object_id, sia_object FROM objects WHERE sia_object_id IS NOT NULL GROUP BY sia_object_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		normalized := 0
		for rows.Next() {
			var id sqlHash256
			var blob []byte
			if err := rows.Scan(&id, &blob); err != nil {
				return err
			}
			var legacy legacySealedObject
			d := types.NewBufDecoder(blob)
			legacy.DecodeFrom(d)
			if err := d.Err(); err != nil {
				return fmt.Errorf("failed to decode sia object %v: %w", types.Hash256(id), err)
			}
			sealed := legacy.convert()
			if sealed.ID() != types.Hash256(id) {
				return fmt.Errorf("decoded sia object id %v does not match stored id %v", sealed.ID(), types.Hash256(id))
			}

			if _, err := objStmt.Exec(id, sealed.EncryptedDataKey, sqlSignature(sealed.DataSignature),
				sealed.EncryptedMetadataKey, sealed.EncryptedMetadata, sqlSignature(sealed.MetadataSignature),
				sqlTime(sealed.CreatedAt), sqlTime(sealed.UpdatedAt)); err != nil {
				return fmt.Errorf("failed to insert sia object %v: %w", types.Hash256(id), err)
			}
			for i, ss := range sealed.Slabs {
				slabID := ss.Digest()
				if _, err := slabStmt.Exec(sqlHash256(slabID), sqlHash256(ss.EncryptionKey), int64(ss.MinShards)); err != nil {
					return fmt.Errorf("failed to insert slab %v: %w", slabID, err)
				}
				if _, err := sliceStmt.Exec(id, i, sqlHash256(slabID), int64(ss.Offset), int64(ss.Length)); err != nil {
					return fmt.Errorf("failed to insert slab slice %d of sia object %v: %w", i, types.Hash256(id), err)
				}
				for j, sec := range ss.Sectors {
					if _, err := sectorStmt.Exec(sqlHash256(slabID), j, sqlHash256(sec.Root), sqlHash256(sec.HostKey)); err != nil {
						return fmt.Errorf("failed to insert sector %d of slab %v: %w", j, slabID, err)
					}
				}
			}
			normalized++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		log.Info("normalized sia objects", zap.Int("objects", normalized))

		// rebuild objects without the sia_object blob column
		_, err = tx.Exec(`
CREATE TABLE object_parts_backup AS SELECT bucket_id, name, version_id, part_number, filename, content_md5, content_length, offset FROM object_parts;
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
    sia_object_id BLOB REFERENCES sia_objects(id),
    CHECK ((size = 0 AND filename IS NULL AND sia_object_id IS NULL) OR (size > 0 AND (filename IS NOT NULL OR sia_object_id IS NOT NULL))),
    CHECK (is_delete_marker IN (FALSE, TRUE)),
    CHECK (is_latest IN (FALSE, TRUE)),
    PRIMARY KEY (bucket_id, name, version_id)
) WITHOUT ROWID;
INSERT INTO objects_new (bucket_id, name, version_id, seq, is_delete_marker, is_latest, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id)
    SELECT bucket_id, name, version_id, seq, is_delete_marker, is_latest, content_md5, metadata, size, parts_count, updated_at, filename, sia_object_id FROM objects;
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
    SELECT bucket_id, name, version_id, part_number, filename, content_md5, content_length, offset FROM object_parts_backup;
DROP TABLE object_parts_backup;`)
		return err
	},
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`
CREATE TABLE snapshots (
	id INTEGER PRIMARY KEY,
	created_at INTEGER NOT NULL,
	sia_object_id BLOB,
	nonce BLOB, -- identifies the in-flight upload, unset on adopted snapshots
	gen INTEGER NOT NULL,
	gen_completed INTEGER,
	object_count INTEGER NOT NULL
);
CREATE UNIQUE INDEX snapshots_sia_object_id_idx ON snapshots(sia_object_id) WHERE sia_object_id IS NOT NULL;
CREATE INDEX snapshots_gen_idx ON snapshots(gen, gen_completed);
ALTER TABLE global_settings ADD COLUMN snapshot_gen INTEGER NOT NULL DEFAULT 0;

CREATE TABLE orphaned_objects_new (
    sia_object_id BLOB PRIMARY KEY,
    orphaned_at_gen INTEGER NOT NULL,
    created_at_gen INTEGER NOT NULL
);
INSERT INTO orphaned_objects_new (sia_object_id, orphaned_at_gen, created_at_gen)
    SELECT sia_object_id, 0, 0 FROM orphaned_objects;
DROP TABLE orphaned_objects;
ALTER TABLE orphaned_objects_new RENAME TO orphaned_objects;
CREATE INDEX orphaned_objects_gen_idx ON orphaned_objects(orphaned_at_gen);

CREATE TABLE sia_slab_slices_backup AS SELECT sia_object_id, slice_index, slab_id, offset, length FROM sia_slab_slices;
DROP TABLE sia_slab_slices;

CREATE TABLE sia_objects_backup AS
    SELECT id, encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at FROM sia_objects;
CREATE TABLE sia_objects_new (
    id BLOB PRIMARY KEY,
    encrypted_data_key BLOB NOT NULL,
    data_signature BLOB NOT NULL,
    encrypted_metadata_key BLOB,
    encrypted_metadata BLOB,
    metadata_signature BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    created_at_gen INTEGER NOT NULL
);
DROP TABLE sia_objects;
ALTER TABLE sia_objects_new RENAME TO sia_objects;
INSERT INTO sia_objects (id, encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at, created_at_gen)
    SELECT id, encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at, 0 FROM sia_objects_backup;
DROP TABLE sia_objects_backup;

CREATE TABLE sia_slab_slices (
    sia_object_id BLOB NOT NULL,
    slice_index INTEGER NOT NULL,
    slab_id BLOB NOT NULL,
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    FOREIGN KEY (sia_object_id) REFERENCES sia_objects(id) ON DELETE CASCADE,
    FOREIGN KEY (slab_id) REFERENCES sia_slabs(id) ON DELETE CASCADE,
    PRIMARY KEY (sia_object_id, slice_index)
) WITHOUT ROWID;
INSERT INTO sia_slab_slices (sia_object_id, slice_index, slab_id, offset, length)
    SELECT sia_object_id, slice_index, slab_id, offset, length FROM sia_slab_slices_backup;
DROP TABLE sia_slab_slices_backup;
CREATE INDEX sia_slab_slices_slab_id_idx ON sia_slab_slices(slab_id);`)
		return err
	},
}
