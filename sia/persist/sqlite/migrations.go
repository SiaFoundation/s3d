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
}
