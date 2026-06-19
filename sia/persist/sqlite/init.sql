/*
	When changing the schema, a new migration function must be added to
	migrations.go
*/

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE access_keys (
    access_key_id TEXT PRIMARY KEY,
    secret_key TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX access_keys_user_id_idx ON access_keys(user_id);

CREATE TABLE buckets (
    id INTEGER PRIMARY KEY,
    created_at INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE,
    user_id INTEGER NOT NULL,
    versioning_status TEXT NOT NULL DEFAULT '' CHECK (versioning_status IN ('', 'Enabled', 'Suspended')),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE INDEX buckets_user_id_idx ON buckets(user_id);

CREATE TABLE objects (
    bucket_id INTEGER REFERENCES buckets(id) NOT NULL,
    name TEXT NOT NULL,
    version_id TEXT NOT NULL DEFAULT '',
    seq INTEGER NOT NULL,
    is_delete_marker INTEGER NOT NULL DEFAULT FALSE,
    content_md5 BLOB NOT NULL,
    metadata TEXT NOT NULL,
    size INTEGER NOT NULL,
    parts_count INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    filename TEXT, -- name of file for regular uploads or dir for multipart uploads
    sia_object_id BLOB,
    sia_object BLOB,
    -- sia_object_id and sia_object are always set or nulled together
    CHECK ((sia_object_id IS NULL AND sia_object IS NULL) OR (sia_object_id IS NOT NULL AND sia_object IS NOT NULL)),
    -- non-empty objects must have a filename, a sia_object_id, or both (between uploading and pinning)
    CHECK ((size = 0 AND filename IS NULL AND sia_object_id IS NULL) OR (size > 0 AND (filename IS NOT NULL OR sia_object_id IS NOT NULL))),
    CHECK (is_delete_marker IN (FALSE, TRUE)),
    PRIMARY KEY (bucket_id, name, version_id)
) WITHOUT ROWID;
CREATE INDEX objects_sia_object_id_idx ON objects(sia_object_id);
CREATE INDEX objects_filename_idx ON objects(filename) WHERE filename IS NOT NULL;
CREATE INDEX objects_bucket_id_updated_at_idx ON objects(bucket_id, updated_at);
CREATE INDEX objects_bucket_name_seq_idx ON objects(bucket_id, name, seq DESC);

CREATE TABLE unpinned_objects (
    sia_object_id BLOB PRIMARY KEY,
    pin_before INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX unpinned_objects_next_attempt_at_idx ON unpinned_objects(next_attempt_at);

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
CREATE INDEX multipart_uploads_bucket_id_created_at_idx ON multipart_uploads(bucket_id, created_at);

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
    version_id TEXT NOT NULL DEFAULT '',
    part_number INTEGER NOT NULL,
    filename TEXT NOT NULL,
    content_md5 BLOB NOT NULL,
    content_length INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    FOREIGN KEY (bucket_id, name, version_id) REFERENCES objects(bucket_id, name, version_id) ON DELETE CASCADE,
    PRIMARY KEY (bucket_id, name, version_id, part_number)
);

CREATE TABLE orphaned_objects (
    sia_object_id BLOB PRIMARY KEY
);

CREATE TABLE bucket_lifecycle_configurations (
    bucket_id INTEGER PRIMARY KEY,
    configuration TEXT NOT NULL,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE TABLE stats (
    stat TEXT PRIMARY KEY NOT NULL,
    stat_value INTEGER NOT NULL CHECK (stat_value >= 0)
);

-- initialize the upload pipeline stat counters
INSERT INTO stats (stat, stat_value) VALUES
    ('pending_objects', 0),
    ('pending_size', 0),
    ('uploaded_objects', 0),
    ('uploaded_size', 0),
    ('unpinned_objects', 0),
    ('orphaned_objects', 0),
    ('multipart_uploads', 0);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	app_key BLOB,
	indexer_url TEXT,
	last_sync_at INTEGER NOT NULL DEFAULT 0,
	last_sync_key BLOB NOT NULL DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000',
	-- app_key and indexer_url are always set or nulled together
	CHECK ((app_key IS NULL AND indexer_url IS NULL) OR (app_key IS NOT NULL AND indexer_url IS NOT NULL))
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
