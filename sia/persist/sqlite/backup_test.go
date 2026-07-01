package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestBackup(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := initTestDB(t, log)

	// add some data to the database on top of the user and access key created
	// by initTestDB
	if err := store.CreateBucket(testAccessKeyID, "bucket-one"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testAccessKeyID, "bucket-two"); err != nil {
		t.Fatal(err)
	}

	users, err := store.ListUsers()
	if err != nil {
		t.Fatal(err)
	}
	accessKeys, err := store.ListAccessKeys(nil)
	if err != nil {
		t.Fatal(err)
	}
	buckets, err := store.ListBuckets(testAccessKeyID)
	if err != nil {
		t.Fatal(err)
	}

	checkDatabase := func(t *testing.T, fp string) {
		t.Helper()

		// open the backup database
		backup, err := OpenDatabase(fp, log)
		if err != nil {
			t.Fatal(err)
		}
		defer backup.Close()

		// check that the data was backed up correctly
		if restored, err := backup.ListUsers(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(users, restored) {
			t.Fatalf("expected users %v, got %v", users, restored)
		}

		if restored, err := backup.ListAccessKeys(nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(accessKeys, restored) {
			t.Fatalf("expected access keys %v, got %v", accessKeys, restored)
		}

		if restored, err := backup.ListBuckets(testAccessKeyID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(buckets, restored) {
			t.Fatalf("expected buckets %v, got %v", buckets, restored)
		}
	}

	t.Run("Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := store.Backup(context.Background(), destPath); err != nil {
			t.Fatal(err)
		}
		checkDatabase(t, destPath)
	})

	t.Run("EmptyDestination", func(t *testing.T) {
		if err := store.Backup(context.Background(), ""); err == nil {
			t.Fatal("expected error for empty destination path")
		}
	})

	t.Run("ExistingDestination", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := store.Backup(context.Background(), destPath); err != nil {
			t.Fatal(err)
		}
		// a second backup to the same path must fail rather than overwrite
		if err := store.Backup(context.Background(), destPath); err == nil {
			t.Fatal("expected error for existing destination path")
		}
	})
}

// TestBackupConcurrentWrites asserts that writes proceed while a backup runs.
// The backup copies the database over a dedicated connection, so it must not
// hold the store's single pool connection for its duration.
func TestBackupConcurrentWrites(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))

	// seed enough data that the backup runs long enough to observe writes
	// landing against it
	if _, err := store.db.Exec(`CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	} else if _, err := store.db.Exec(`CREATE TABLE writes (id INTEGER PRIMARY KEY, val INTEGER)`); err != nil {
		t.Fatal(err)
	}

	const blobs = 512 // ~128MB at 256KB each
	blob := frand.Bytes(256 * 1024)
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < blobs; i++ {
		if _, err := tx.Exec(`INSERT INTO blobs (data) VALUES (?)`, blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// hammer writes through the pool connection while the backup runs
	var writes atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := store.db.Exec(`INSERT INTO writes (val) VALUES (?)`, frand.Uint64n(1<<62)); err == nil {
				writes.Add(1)
			}
		}
	}()

	destPath := filepath.Join(t.TempDir(), "backup.sqlite")
	if err := store.Backup(context.Background(), destPath); err != nil {
		close(stop)
		<-done
		t.Fatal(err)
	}
	close(stop)
	<-done

	// a backup that held the pool connection would block the writer for its
	// whole duration, leaving the count near zero
	if n := writes.Load(); n < 25 {
		t.Fatal("expected writes to proceed during backup, got", n)
	}

	// the copy must be consistent
	dest, err := sql.Open("sqlite3", sqliteFilepath(destPath))
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	var got int
	if err := dest.QueryRow(`SELECT COUNT(*) FROM blobs`).Scan(&got); err != nil {
		t.Fatal(err)
	} else if got != blobs {
		t.Fatal("inconsistent backup, blobs:", got)
	}
}
