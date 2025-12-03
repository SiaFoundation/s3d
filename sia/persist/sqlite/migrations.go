package sqlite

import "go.uber.org/zap"

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx *txn, log *zap.Logger) error{
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`
ALTER TABLE objects ADD COLUMN name_lower TEXT GENERATED ALWAYS AS (LOWER(name)) VIRTUAL;
CREATE INDEX objects_name ON objects(name);
CREATE INDEX objects_name_lower ON objects(name_lower);
`)
		return err
	},
}
