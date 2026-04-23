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
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    filename TEXT, -- name of file for regular uploads or dir for multipart uploads
    sia_object_id BLOB,
    sia_object BLOB,
    -- sia_object_id and sia_object are always set or nulled together
    CHECK ((sia_object_id IS NULL AND sia_object IS NULL) OR (sia_object_id IS NOT NULL AND sia_object IS NOT NULL)),
    -- object is either on disk, on Sia, or empty
    CHECK ((filename IS NOT NULL AND sia_object_id IS NULL) OR (filename IS NULL AND sia_object_id IS NOT NULL) OR (filename IS NULL AND sia_object_id IS NULL AND size = 0)),
    PRIMARY KEY (bucket_id, name)
) WITHOUT ROWID;
CREATE INDEX objects_sia_object_id_idx ON objects(sia_object_id);

CREATE TABLE multipart_uploads (
    upload_id BLOB PRIMARY KEY,
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id)
);
CREATE INDEX multipart_uploads_bucket_id_name_idx ON multipart_uploads(bucket_id, name);
CREATE INDEX multipart_uploads_bucket_id_name_upload_id_idx ON multipart_uploads(bucket_id, name, upload_id);

CREATE TABLE multipart_parts (
    upload_id BLOB NOT NULL,
    part_number INTEGER NOT NULL,
    filename TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE object_parts (
    bucket_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    filename TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    FOREIGN KEY (bucket_id, name) REFERENCES objects(bucket_id, name) ON DELETE CASCADE,
    PRIMARY KEY (bucket_id, name, part_number)
);

CREATE TABLE orphaned_objects (
    sia_object_id BLOB PRIMARY KEY
);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	app_key BLOB,
	objects_cursor_at INTEGER NOT NULL DEFAULT 0,
	objects_cursor_key BLOB NOT NULL DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000'
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
