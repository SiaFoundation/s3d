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
    object_id BLOB,
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    filename TEXT, -- name of file for regular uploads or dir for multipart uploads
    sia_object BLOB,
    cached_at INTEGER NOT NULL,
    -- file is either stored on disk, on Sia or empty.
    CHECK ((sia_object IS NULL AND object_id IS NULL AND filename IS NOT NULL) OR (sia_object IS NOT NULL AND object_id IS NOT NULL AND filename IS NULL) OR (object_id IS NULL AND filename IS NULL AND size = 0)),
    PRIMARY KEY (bucket_id, name)
) WITHOUT ROWID;
CREATE INDEX objects_object_id_idx ON objects(object_id);
CREATE INDEX objects_pending_idx ON objects(filename) WHERE filename IS NOT NULL;

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
    object_id BLOB PRIMARY KEY
);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	app_key BLOB
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
