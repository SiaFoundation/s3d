/*
	When changing the schema, a new migration function must be added to
	migrations.go
*/


CREATE TABLE buckets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER REFERENCES buckets(id) NOT NULL,
    name TEXT NOT NULL,
    object_id BLOB NOT NULL,
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(bucket_id, name)
);
CREATE INDEX objects_name ON objects(name);

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

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL -- used for migrations
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
