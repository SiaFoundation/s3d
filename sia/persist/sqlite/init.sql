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
    sia_meta BLOB NOT NULL,
    size INTEGER NOT NULL,
    content_md5 BLOB NOT NULL,
    last_modified TIMESTAMP NOT NULL DEFAULT (DATE('now')),
    UNIQUE(bucket_id, name)
);
CREATE INDEX objects_name ON objects(name);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL -- used for migrations
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
