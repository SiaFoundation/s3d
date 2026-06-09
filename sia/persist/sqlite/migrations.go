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
			CREATE TABLE bucket_lifecycle_configurations (
				bucket_id INTEGER PRIMARY KEY,
				configuration TEXT NOT NULL,
				FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
			);
			CREATE INDEX IF NOT EXISTS objects_bucket_id_updated_at_idx ON objects(bucket_id, updated_at);
			CREATE INDEX IF NOT EXISTS multipart_uploads_bucket_id_created_at_idx ON multipart_uploads(bucket_id, created_at);`)
		return err
	},
}
