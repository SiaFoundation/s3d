package sqlite

import "go.uber.org/zap"

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx *txn, log *zap.Logger) error{
	func(tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(`
CREATE INDEX objects_name ON objects(name);
ALTER TABLE objects ADD COLUMN content_md5 BLOB NOT NULL;
ALTER TABLE objects ADD COLUMN size INTEGER NOT NULL;
ALTER TABLE objects ADD COLUMN last_modified TIMESTAMP NOT NULL DEFAULT (DATE('now'));
`)
		return err
	},
}
