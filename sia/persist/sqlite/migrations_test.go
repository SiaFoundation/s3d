package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
INSERT INTO global_settings (id, db_version, app_key) VALUES (0, 1, x'0102'); -- should not be changed

-- seed data to verify migrations preserve existing rows
INSERT INTO users (id, name) VALUES (1, 'user');
INSERT INTO buckets (id, created_at, name, user_id) VALUES (1, 0, 'bucket', 1);
INSERT INTO objects (bucket_id, name, content_md5, metadata, size, updated_at, filename) VALUES (1, 'obj', x'00', '{"Content-Type":"text/plain","X-Amz-Meta-Colour":"blue","x-amz-date":"20260908T000000Z","X-Amz-Security-Token":"session","X-Amz-Website-Redirect-Location":"/a"}', 10, 0, 'obj.dat');
INSERT INTO multipart_uploads (upload_id, bucket_id, name, metadata, created_at) VALUES (x'01', 1, 'upload', '{"Content-Type":"text/plain","X-Amz-Security-Token":"session"}', 0);
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

	// the v1 fixture carries an app key and no indexer_url, the state the
	// backfill exists for, so it must have supplied the assumed default. The
	// literal is spelled out rather than compared against compatIndexerURL,
	// which would move with it and assert nothing.
	var indexerURL string
	if err := store.db.QueryRow(`SELECT indexer_url FROM global_settings`).Scan(&indexerURL); err != nil {
		t.Fatal(err)
	} else if indexerURL != "https://sia.storage" {
		t.Fatalf("expected the backfilled URL, got %q", indexerURL)
	}

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

	// the seeded object keeps only its object metadata headers
	var meta sqlMetaJSON
	if err := store.db.QueryRow(`SELECT metadata FROM objects WHERE bucket_id = 1 AND name = 'obj'`).Scan(&meta); err != nil {
		t.Fatal(err)
	} else if len(meta) != 2 || meta["Content-Type"] != "text/plain" || meta["X-Amz-Meta-Colour"] != "blue" {
		t.Fatalf("unexpected metadata %v", meta)
	}
	var uploadMeta sqlMetaJSON
	if err := store.db.QueryRow(`SELECT metadata FROM multipart_uploads WHERE upload_id = x'01'`).Scan(&uploadMeta); err != nil {
		t.Fatal(err)
	} else if len(uploadMeta) != 1 || uploadMeta["Content-Type"] != "text/plain" {
		t.Fatalf("unexpected upload metadata %v", uploadMeta)
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
	// single seeded object has a filename and no sia_object_id, size 10, so it
	// is a pending upload, and the seeded multipart upload is counted too.
	expectedStats := map[string]int64{
		"pending_objects":   1,
		"pending_size":      10,
		"uploaded_objects":  0,
		"uploaded_size":     0,
		"unpinned_objects":  0,
		"orphaned_objects":  0,
		"multipart_uploads": 1,
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

	// ensure each table's definition matches the baseline. table_info covers
	// columns but not CHECK constraints or foreign keys, which only exist in
	// the stored CREATE TABLE text.
	getTableSQL := func(db *sql.DB, table string) (string, error) {
		var stmt string
		err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name=$1`, table).Scan(&stmt)
		return normalizeSchemaSQL(stmt), err
	}

	for k := range baselineTables {
		if strings.HasPrefix(k, "sqlite_") {
			continue // internal tables have no stored sql
		}
		want, err := getTableSQL(baseline.db, k)
		if err != nil {
			t.Fatal(err)
		}
		got, err := getTableSQL(store.db, k)
		if err != nil {
			t.Fatal(err)
		}
		if want != got {
			t.Errorf("table %s differs\n baseline: %s\n migrated: %s", k, want, got)
		}
	}
}

// normalizeSchemaSQL strips comments, collapses whitespace and drops the quotes
// that ALTER TABLE RENAME leaves around a table name, so a migrated definition
// can be compared against the one in init.sql.
func normalizeSchemaSQL(stmt string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(stmt, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString(" ")
	}
	return strings.Join(strings.Fields(strings.ReplaceAll(b.String(), `"`, "")), " ")
}

// TestMigrationKeepsCustomIndexerURL checks the backfill only fills a missing
// URL and never overwrites one the operator configured.
func TestMigrationKeepsCustomIndexerURL(t *testing.T) {
	const customURL = "https://indexer.example"
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

	// version 3 is the first with an indexer_url column to set
	store := initDBVersion(t, fp, 3, log)
	if _, err := store.db.Exec(`UPDATE global_settings SET app_key = $1, indexer_url = $2`, frand.Bytes(64), customURL); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()

	var got string
	if err := migrated.db.QueryRow(`SELECT indexer_url FROM global_settings`).Scan(&got); err != nil {
		t.Fatal(err)
	} else if got != customURL {
		t.Fatalf("expected the configured URL %q, got %q", customURL, got)
	}
}

func TestMigrationObjectLockPreservesRows(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

	// seed every table the migration touches, at the version just before it runs
	const objectLockVersion = 14
	store := initDBVersion(t, fp, objectLockVersion-1, log)
	seed := []string{
		`INSERT INTO users (id, name) VALUES (100, 'alice')`,
		`INSERT INTO buckets (id, created_at, name, user_id, versioning_status) VALUES (100, 100, 'b1', 100, 'Enabled'), (200, 200, 'b2', 100, '')`,
		`INSERT INTO objects (bucket_id, name, version_id, seq, content_md5, metadata, size, updated_at)
			VALUES (100, 'k1', 'v1', 1, X'00', '{}', 0, 300), (100, 'k2', '', 2, X'00', '{}', 0, 400), (200, 'k3', '', 1, X'00', '{}', 0, 500)`,
		`INSERT INTO object_parts (bucket_id, name, version_id, part_number, filename, content_md5, content_length, offset)
			VALUES (100, 'k1', 'v1', 1, 'f1', X'00', 5, 0)`,
		`INSERT INTO multipart_uploads (upload_id, bucket_id, name, metadata, created_at) VALUES (X'aa', 100, 'mp1', '{}', 600)`,
		`INSERT INTO multipart_parts (upload_id, part_number, filename, content_md5, content_length, created_at) VALUES (X'aa', 1, 'f2', X'00', 7, 700)`,
		`INSERT INTO bucket_lifecycle_configurations (bucket_id, configuration) VALUES (100, '<LifecycleConfiguration/>')`,
	}
	for _, stmt := range seed {
		if _, err := store.db.Exec(stmt); err != nil {
			t.Fatalf("seeding %q: %v", stmt, err)
		}
	}

	tables := []string{"buckets", "objects", "object_parts", "multipart_uploads", "multipart_parts", "bucket_lifecycle_configurations"}
	count := func(db *sql.DB, table string) int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := make(map[string]int, len(tables))
	for _, table := range tables {
		before[table] = count(store.db, table)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	if v, want := getDBVersion(migrated.db), int64(len(migrations)+1); v != want {
		t.Fatalf("expected version %d, got %d", want, v)
	}

	for _, table := range tables {
		if got := count(migrated.db, table); got != before[table] {
			t.Errorf("expected %d rows in %s, got %d", before[table], table, got)
		}
	}

	// the rebuild must not disturb the values it carries over
	var name, versioning string
	var createdAt int64
	if err := migrated.db.QueryRow(`SELECT name, created_at, versioning_status FROM buckets WHERE id = 100`).Scan(&name, &createdAt, &versioning); err != nil {
		t.Fatal(err)
	} else if name != "b1" || createdAt != 100 || versioning != "Enabled" {
		t.Fatalf("bucket 100 changed: %q %d %q", name, createdAt, versioning)
	}

	var seq, contentLength int64
	if err := migrated.db.QueryRow(`SELECT seq FROM objects WHERE bucket_id = 100 AND name = 'k2'`).Scan(&seq); err != nil {
		t.Fatal(err)
	} else if seq != 2 {
		t.Fatalf("expected seq 2, got %d", seq)
	}
	if err := migrated.db.QueryRow(`SELECT content_length FROM object_parts WHERE bucket_id = 100`).Scan(&contentLength); err != nil {
		t.Fatal(err)
	} else if contentLength != 5 {
		t.Fatalf("expected content length 5, got %d", contentLength)
	}

	// existing rows must come out unlocked
	var mode, legalHold string
	var retainUntil sql.NullInt64
	if err := migrated.db.QueryRow(`SELECT object_lock_mode, object_lock_retain_until, object_lock_legal_hold FROM objects WHERE bucket_id = 100 AND name = 'k1'`).Scan(&mode, &retainUntil, &legalHold); err != nil {
		t.Fatal(err)
	} else if mode != "" || retainUntil.Valid || legalHold != "" {
		t.Fatalf("expected no lock, got %q %v %q", mode, retainUntil, legalHold)
	}

	// every restored row must still resolve through its foreign keys
	rows, err := migrated.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var table, parent string
		var rowid, fkid sql.NullInt64
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			t.Fatal(err)
		}
		t.Errorf("foreign key violation in %s referencing %s", table, parent)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
