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

CREATE VIRTUAL TABLE objects_fts USING fts5(name, tokenize="trigram");

-- Trigger to insert into FTS table when a new object is added
CREATE TRIGGER objects_ai AFTER INSERT ON objects
BEGIN
    INSERT INTO objects_fts(rowid, name)
    VALUES (new.id, new.name);
END;

-- Trigger to update FTS table when an object is updated
CREATE TRIGGER objects_au AFTER UPDATE ON objects
BEGIN
    UPDATE objects_fts
    SET name = new.name
    WHERE rowid = old.id;
END;

-- Trigger to delete from FTS table when an object is deleted
CREATE TRIGGER objects_ad AFTER DELETE ON objects
BEGIN
    DELETE FROM objects_fts
    WHERE rowid = old.id;
END;
`)
		return err
	},
}
