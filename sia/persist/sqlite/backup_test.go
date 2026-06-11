package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"go.uber.org/zap/zaptest"
)

func TestBackup(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := initTestDB(t, log)
	srcPath := store.path

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

	t.Run("Store.Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := store.Backup(context.Background(), destPath); err != nil {
			t.Fatal(err)
		}
		checkDatabase(t, destPath)
	})

	t.Run("Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := Backup(context.Background(), srcPath, destPath); err != nil {
			t.Fatal(err)
		}
		checkDatabase(t, destPath)
	})

	t.Run("EmptyDestination", func(t *testing.T) {
		if err := Backup(context.Background(), srcPath, ""); err == nil {
			t.Fatal("expected error for empty destination path")
		}
	})

	t.Run("ExistingDestination", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := Backup(context.Background(), srcPath, destPath); err != nil {
			t.Fatal(err)
		}
		// a second backup to the same path must fail rather than overwrite
		if err := Backup(context.Background(), srcPath, destPath); err == nil {
			t.Fatal("expected error for existing destination path")
		}
	})

	t.Run("MissingSource", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if err := Backup(context.Background(), filepath.Join(t.TempDir(), "does-not-exist.sqlite"), destPath); err == nil {
			t.Fatal("expected error for missing source path")
		}
	})
}
