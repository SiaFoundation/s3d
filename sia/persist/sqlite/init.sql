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
    bucket_id INTEGER REFERENCES buckets(id) NOT NULL,
    name TEXT NOT NULL,
    object_id BLOB NOT NULL,
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(bucket_id, name)
) WITHOUT ROWID;

CREATE TABLE multipart_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id BLOB NOT NULL UNIQUE,
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id)
);
CREATE INDEX multipart_uploads_bucket_id_name_idx ON multipart_uploads(bucket_id, name);

CREATE TABLE parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    part_number INTEGER NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,

    created_at INTEGER, -- nulled when the part is finalized
    filename TEXT, -- nulled when the part is finalized
    offset INTEGER, -- set when the part is finalized

    -- one of these foreign keys must be set
    multipart_upload_id INTEGER,
    object_id INTEGER,

    -- enforce atomicity
    CHECK (
        (multipart_upload_id IS NOT NULL AND object_id IS NULL) OR
        (multipart_upload_id IS NULL AND object_id IS NOT NULL AND created_at IS NULL AND filename IS NULL AND offset IS NOT NULL)
    ),

    FOREIGN KEY (multipart_upload_id) REFERENCES multipart_uploads(id) ON DELETE CASCADE,
    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,

    UNIQUE(multipart_upload_id, part_number),
    UNIQUE(object_id, part_number)
);
CREATE INDEX parts_multipart_upload_id_idx ON parts(multipart_upload_id) WHERE multipart_upload_id IS NOT NULL;
CREATE INDEX parts_object_id_idx ON parts(object_id) WHERE object_id IS NOT NULL;

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	app_key BLOB
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
