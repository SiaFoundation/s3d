package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// nolint:misspell
const initialSchema = `/*
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
    filename TEXT,
    sia_object_id BLOB,
    sia_object BLOB,
    CHECK ((sia_object_id IS NULL AND sia_object IS NULL) OR (sia_object_id IS NOT NULL AND sia_object IS NOT NULL)),
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
	last_sync_at INTEGER NOT NULL DEFAULT 0,
	last_sync_key BLOB NOT NULL DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000'
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 1); -- should not be changed`

func initDBVersion(tb testing.TB, fp string, target int64, log *zap.Logger) *Store {
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Fatal(err)
		}
	})
	if _, err := db.Exec(initialSchema); err != nil {
		tb.Fatal(err)
	}

	// set the number of open connections to 1 to prevent "database is locked"
	// errors
	db.SetMaxOpenConns(1)

	store := &Store{
		db:  db,
		log: log,
	}
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Fatal(err)
		}
	})

	if err := store.init(target); err != nil {
		tb.Fatal(err)
	}
	return store
}

func TestMigrationConsistency(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "hostd.sqlite3")

	// initialize the v1 database
	store := initDBVersion(t, fp, 1, log)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	expectedVersion := int64(len(migrations) + 1)
	store, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	v := getDBVersion(store.db)
	if v != expectedVersion {
		t.Fatalf("expected version %d, got %d", expectedVersion, v)
	} else if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// ensure the database does not change version when opened again
	store, err = OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	v = getDBVersion(store.db)
	if v != expectedVersion {
		t.Fatalf("expected version %d, got %d", expectedVersion, v)
	}

	fp2 := filepath.Join(t.TempDir(), "hostd.sqlite3")
	baseline, err := OpenDatabase(fp2, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer baseline.Close()

	getTableIndices := func(db *sql.DB) (map[string]bool, error) {
		const query = `SELECT name, tbl_name, sql FROM sqlite_schema WHERE type='index'`
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		indices := make(map[string]bool)
		for rows.Next() {
			var name, table string
			var sqlStr sql.NullString // auto indices have no sql
			if err := rows.Scan(&name, &table, &sqlStr); err != nil {
				return nil, err
			}
			indices[fmt.Sprintf("%s.%s.%s", name, table, sqlStr.String)] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return indices, nil
	}

	// ensure the migrated database has the same indices as the baseline
	baselineIndices, err := getTableIndices(baseline.db)
	if err != nil {
		t.Fatal(err)
	}

	migratedIndices, err := getTableIndices(store.db)
	if err != nil {
		t.Fatal(err)
	}

	for k := range baselineIndices {
		if !migratedIndices[k] {
			t.Errorf("missing index %s", k)
		}
	}

	for k := range migratedIndices {
		if !baselineIndices[k] {
			t.Errorf("unexpected index %s", k)
		}
	}

	getTables := func(db *sql.DB) (map[string]bool, error) {
		const query = `SELECT name FROM sqlite_schema WHERE type='table'`
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		tables := make(map[string]bool)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			tables[name] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return tables, nil
	}

	// ensure the migrated database has the same tables as the baseline
	baselineTables, err := getTables(baseline.db)
	if err != nil {
		t.Fatal(err)
	}

	migratedTables, err := getTables(store.db)
	if err != nil {
		t.Fatal(err)
	}

	for k := range baselineTables {
		if !migratedTables[k] {
			t.Errorf("missing table %s", k)
		}
	}
	for k := range migratedTables {
		if !baselineTables[k] {
			t.Errorf("unexpected table %s", k)
		}
	}

	// ensure each table has the same columns as the baseline
	getTableColumns := func(db *sql.DB, table string) (map[string]bool, error) {
		query := fmt.Sprintf(`PRAGMA table_info(%s)`, table) // cannot use parameterized query for PRAGMA statements
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		columns := make(map[string]bool)
		for rows.Next() {
			var cid int
			var name, colType string
			var defaultValue sql.NullString
			var notNull bool
			var primaryKey int // composite keys are indices
			if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &primaryKey); err != nil {
				return nil, err
			}
			// column ID is ignored since it may not match between the baseline and migrated databases
			key := fmt.Sprintf("%s.%s.%s.%t.%d", name, colType, defaultValue.String, notNull, primaryKey)
			columns[key] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return columns, nil
	}

	for k := range baselineTables {
		baselineColumns, err := getTableColumns(baseline.db, k)
		if err != nil {
			t.Fatal(err)
		}
		migratedColumns, err := getTableColumns(store.db, k)
		if err != nil {
			t.Fatal(err)
		}

		for c := range baselineColumns {
			if !migratedColumns[c] {
				t.Errorf("missing column %s.%s", k, c)
			}
		}

		for c := range migratedColumns {
			if !baselineColumns[c] {
				t.Errorf("unexpected column %s.%s", k, c)
			}
		}
	}
}
