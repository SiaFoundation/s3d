package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// nolint:misspell
const initialSchema = `/*
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
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE INDEX buckets_user_id_idx ON buckets(user_id);

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
INSERT INTO global_settings (id, db_version) VALUES (0, 1); -- should not be changed

-- seed data to verify migrations preserve existing rows
INSERT INTO users (id, name) VALUES (1, 'user');
INSERT INTO buckets (id, created_at, name, user_id) VALUES (1, 0, 'bucket', 1);
INSERT INTO objects (bucket_id, name, content_md5, metadata, size, updated_at, filename) VALUES (1, 'obj', x'00', '{}', 10, 0, 'obj.dat');
INSERT INTO object_parts (bucket_id, name, part_number, filename, content_md5, content_length, offset) VALUES
    (1, 'obj', 1, 'part1.dat', x'01', 5, 0),
    (1, 'obj', 2, 'part2.dat', x'02', 5, 5);`

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

// TestMigrationSiaObjectNormalization seeds a v1 database with sealed object
// blobs and verifies the normalization migration splits them into the
// sia_objects, sia_slabs, sia_slab_slices and sia_slab_sectors tables.
func TestMigrationSiaObjectNormalization(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(initialSchema); err != nil {
		t.Fatal(err)
	}

	// the v1 database predates the versioned slab encoding, so the seeded
	// blobs use the legacy encoding the normalization migration decodes
	newSlab := func(sectors int, offset, length uint32) legacySlabSlice {
		ss := legacySlabSlice{EncryptionKey: frand.Entropy256(), MinShards: 1, Offset: offset, Length: length}
		for range sectors {
			ss.Sectors = append(ss.Sectors, legacyPinnedSector{Root: frand.Entropy256(), HostKey: frand.Entropy256()})
		}
		return ss
	}
	seal := func(ss ...legacySlabSlice) legacySealedObject {
		so := legacySealedObject{
			EncryptedDataKey:     frand.Bytes(32),
			Slabs:                ss,
			EncryptedMetadataKey: frand.Bytes(32),
			EncryptedMetadata:    frand.Bytes(16),
			CreatedAt:            time.Unix(1000, 0),
			UpdatedAt:            time.Unix(2000, 0),
		}
		frand.Read(so.DataSignature[:])
		frand.Read(so.MetadataSignature[:])
		return so
	}

	// two sealed objects slicing one shared slab to exercise deduplication,
	// plus a copy sharing the first sealed object outright and a third
	// stand-alone object
	shared := newSlab(3, 0, 100)
	sharedTail := shared
	sharedTail.Offset, sharedTail.Length = 50, 50
	sealed1 := seal(newSlab(2, 0, 100), shared)
	sealed2 := seal(sharedTail, newSlab(1, 0, 100))
	sealed3 := seal(newSlab(1, 0, 100))

	insert := func(name string, sealed legacySealedObject) {
		t.Helper()
		var buf bytes.Buffer
		e := types.NewEncoder(&buf)
		sealed.EncodeTo(e)
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		converted := sealed.convert()
		id := converted.ID()
		if _, err := db.Exec(`
			INSERT INTO objects (bucket_id, name, content_md5, metadata, size, updated_at, filename, sia_object_id, sia_object)
			VALUES (1, ?, x'00', '{}', 10, 0, NULL, ?, ?)`, name, id[:], buf.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	insert("obj1", sealed1)
	insert("obj2", sealed2)
	insert("obj1-copy", sealed1)
	insert("obj3", sealed3)

	// seed rows referencing (or related to) the objects table to verify the
	// rebuild doesn't drop them via the ON DELETE CASCADE foreign keys
	if _, err := db.Exec(`INSERT INTO object_parts (bucket_id, name, part_number, filename, content_md5, content_length, offset) VALUES
		(1, 'obj1', 1, 'part1.dat', x'01', 5, 0),
		(1, 'obj1', 2, 'part2.dat', x'02', 5, 5)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO orphaned_objects (sia_object_id) VALUES (?)`, frand.Bytes(32)); err != nil {
		t.Fatal(err)
	}

	store := &Store{db: db, log: log}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if err := store.init(int64(len(migrations) + 1)); err != nil {
		t.Fatal(err)
	}

	// the blob column must be gone while the references remain
	assertCount := func(query string, want int) {
		t.Helper()
		var got int
		if err := db.QueryRow(query).Scan(&got); err != nil {
			t.Fatal(err)
		} else if got != want {
			t.Fatalf("%q: expected %d, got %d", query, want, got)
		}
	}
	assertCount(`SELECT COUNT(*) FROM pragma_table_info('objects') WHERE name = 'sia_object'`, 0)
	assertCount(`SELECT COUNT(*) FROM objects WHERE sia_object_id IS NOT NULL`, 4)

	// rows referencing objects survived the table rebuild
	assertCount(`SELECT COUNT(*) FROM object_parts WHERE bucket_id = 1 AND name = 'obj1'`, 2)
	assertCount(`SELECT COUNT(*) FROM orphaned_objects`, 1)

	// migrated rows predate the first snapshot, so they carry generation 0
	assertCount(`SELECT COUNT(*) FROM sia_objects WHERE created_at_gen = 0`, 3)
	assertCount(`SELECT COUNT(*) FROM orphaned_objects WHERE orphaned_at_gen = 0 AND created_at_gen = 0`, 1)

	// two sealed objects, four slices and three slabs (the shared one
	// deduplicated) with six sectors, all slabs at version 0
	assertCount(`SELECT COUNT(*) FROM sia_objects`, 3)
	assertCount(`SELECT COUNT(*) FROM sia_slab_slices`, 5)
	assertCount(`SELECT COUNT(*) FROM sia_slabs`, 4)
	assertCount(`SELECT COUNT(*) FROM sia_slabs WHERE version = 0`, 4)
	assertCount(`SELECT COUNT(*) FROM sia_slab_sectors`, 7)

	// the sealed objects round-trip through the normalized tables
	for _, legacy := range []legacySealedObject{sealed1, sealed2, sealed3} {
		want := sdk.SealedObject{SealedObject: legacy.convert()}
		var got sdk.SealedObject
		err := store.transaction(func(tx *txn) (err error) {
			got, err = siaObject(tx, want.ID())
			return
		})
		if err != nil {
			t.Fatal(err)
		}
		wantBlob, err := want.MarshalSia()
		if err != nil {
			t.Fatal(err)
		}
		gotBlob, err := got.MarshalSia()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wantBlob, gotBlob) {
			t.Fatalf("sealed object %v did not survive normalization", want.ID())
		}
	}
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

	// ensure the seeded object parts survived the migrations
	var partCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM object_parts WHERE bucket_id = 1 AND name = 'obj'`).Scan(&partCount); err != nil {
		t.Fatal(err)
	} else if partCount != 2 {
		t.Fatalf("expected 2 object parts, got %d", partCount)
	}
	var partsCount int
	if err := store.db.QueryRow(`SELECT parts_count FROM objects WHERE bucket_id = 1 AND name = 'obj'`).Scan(&partsCount); err != nil {
		t.Fatal(err)
	} else if partsCount != 2 {
		t.Fatalf("expected parts_count 2, got %d", partsCount)
	}

	// the stats table must have been backfilled from the seeded data. The
	// single seeded object has a filename and no sia_object_id (size 10), so it
	// is a pending upload; everything else is empty.
	expectedStats := map[string]int64{
		"pending_objects":   1,
		"pending_size":      10,
		"uploaded_objects":  0,
		"uploaded_size":     0,
		"unpinned_objects":  0,
		"orphaned_objects":  0,
		"multipart_uploads": 0,
	}
	for stat, want := range expectedStats {
		var got int64
		if err := store.db.QueryRow(`SELECT stat_value FROM stats WHERE stat = ?`, stat).Scan(&got); errors.Is(err, sql.ErrNoRows) {
			t.Errorf("stat %q missing from stats table", stat)
		} else if err != nil {
			t.Fatal(err)
		} else if got != want {
			t.Errorf("stat %q: expected %d, got %d", stat, want, got)
		}
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
