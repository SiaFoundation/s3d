package sqlite

import (
	"go.uber.org/zap"
)

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx *txn, log *zap.Logger) error{
	func(tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(`CREATE INDEX IF NOT EXISTS objects_object_id_idx ON objects(object_id);
CREATE TABLE IF NOT EXISTS orphaned_objects (object_id BLOB PRIMARY KEY);`)
		return err
	},
}
