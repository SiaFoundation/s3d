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
		if _, err := tx.Exec(`CREATE TABLE stats (
    stat TEXT PRIMARY KEY NOT NULL,
    stat_value INTEGER NOT NULL CHECK (stat_value >= 0)
)`); err != nil {
			return err
		}
		// backfill the stat counters from existing data
		_, err := tx.Exec(`
			INSERT INTO stats (stat, stat_value)
			SELECT 'pending_objects', COUNT(filename) FROM objects
			UNION ALL SELECT 'pending_size', COALESCE(SUM(CASE WHEN filename IS NOT NULL THEN size END), 0) FROM objects
			UNION ALL SELECT 'uploaded_objects', COUNT(sia_object_id) FROM objects
			UNION ALL SELECT 'uploaded_size', COALESCE(SUM(CASE WHEN sia_object_id IS NOT NULL THEN size END), 0) FROM objects
			UNION ALL SELECT 'orphaned_objects', (SELECT COUNT(*) FROM orphaned_objects)
			UNION ALL SELECT 'multipart_uploads', (SELECT COUNT(*) FROM multipart_uploads)`)
		return err
	},
}
