package sia_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestDeleteOrphanedUploads(t *testing.T) {
	// create uploads directory
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, sia.UploadsDirectory)

	// create store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	// create test user and access key
	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	// create bucket
	if err := store.CreateBucket(testutil.AccessKeyID, "bucket"); err != nil {
		t.Fatal(err)
	}

	// create sia backend
	memSDK := testutil.NewMemorySDK()
	backend, err := sia.New(t.Context(), memSDK, store, dir,
		sia.WithUploadDisabled(),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })

	// helper to create objects
	createObject := func(filename string, referenced bool) {
		t.Helper()
		if referenced {
			if _, _, err := store.PutObject(testutil.AccessKeyID, "bucket", filename, frand.Entropy128(), nil, 100, &filename); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(uploadsDir, filename), []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	// helper to create multipart uploads
	createMultipart := func(uid s3.UploadID, referenced bool) {
		t.Helper()
		if referenced {
			if err := store.CreateMultipartUpload(testutil.AccessKeyID, "bucket", uid.String(), uid, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.MkdirAll(filepath.Join(uploadsDir, uid.String(), "1"), 0700); err != nil {
			t.Fatal(err)
		} else if err := os.WriteFile(filepath.Join(uploadsDir, uid.String(), "1", "data.part"), []byte("part"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	// helpers to assert file existence
	assertExists := func(name string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(uploadsDir, name)); err != nil {
			t.Fatalf("expected %q to exist", name)
		}
	}

	// helper to assert file removal
	assertRemoved := func(name string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(uploadsDir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected %q to be removed", name)
		}
	}

	// add two objects, only one is referenced
	obj1 := "obj1.upload"
	obj2 := "obj2.upload"
	createObject(obj1, true)
	createObject(obj2, false)

	// add two multipart uploads, only one is referenced
	uid1 := s3.NewUploadID()
	uid2 := s3.NewUploadID()
	createMultipart(uid1, true)
	createMultipart(uid2, false)

	// run cleanup
	removed, err := backend.DeleteOrphanedUploads()
	if err != nil {
		t.Fatal(err)
	} else if removed != 2 {
		t.Fatalf("expected 2 orphaned uploads to be removed, got %d", removed)
	}

	// assert referenced entries are kept
	assertExists(obj1)
	assertExists(uid1.String())

	// assert orphaned entries are removed
	assertRemoved(obj2)
	assertRemoved(uid2.String())
}

// TestPruneSlabs verifies that the Sia backend's background orphan processing
// loop invokes PruneSlabs immediately on startup and once per interval
// thereafter, so slab garbage left behind by previously-unpinned objects is
// reclaimed promptly.
func TestPruneSlabs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		memSDK := testutil.NewMemorySDK()
		log := zaptest.NewLogger(t)
		dir := t.TempDir()
		store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		siaBackend, err := sia.New(t.Context(), memSDK, store, dir,
			sia.WithUploadDisabled(), sia.WithLogger(log))
		if err != nil {
			t.Fatal(err)
		}
		defer siaBackend.Close()

		synctest.Wait()
		if got := memSDK.PruneSlabsCalls(); got != 1 {
			t.Fatalf("after startup: expected 1 PruneSlabs call, got %d", got)
		}

		for i := 1; i <= 4; i++ {
			time.Sleep(sia.OrphanLoopInterval)
			synctest.Wait()
			want := 1 + i
			if got := memSDK.PruneSlabsCalls(); got != want {
				t.Fatalf("after %d intervals: expected %d PruneSlabs calls, got %d", i, want, got)
			}
		}
	})
}
