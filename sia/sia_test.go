package sia_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
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
	if err := store.CreateBucket(testutil.AccessKeyID, "bucket", false); err != nil {
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
			if _, _, err := store.PutObject(testutil.AccessKeyID, "bucket", filename, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 100, FileName: &filename}); err != nil {
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
			if err := store.CreateMultipartUpload(testutil.AccessKeyID, "bucket", uid.String(), uid, nil, nil); err != nil {
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

func TestCompleteRestore(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, sia.UploadsDirectory)
	if err := os.MkdirAll(uploadsDir, 0700); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testutil.AccessKeyID, "bucket", false); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testutil.AccessKeyID, "locked", true); err != nil {
		t.Fatal(err)
	}

	putObject := func(bucket, name string, onDisk bool, lock *s3.ObjectLockState) {
		t.Helper()
		filename := name + ".upload"
		if _, _, err := store.PutObject(testutil.AccessKeyID, bucket, name, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 100, FileName: &filename, ObjectLock: lock}); err != nil {
			t.Fatal(err)
		}
		if onDisk {
			if err := os.WriteFile(filepath.Join(uploadsDir, filename), []byte("data"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	createMultipart := func(name string, onDisk bool) s3.UploadID {
		t.Helper()
		uid := s3.NewUploadID()
		if err := store.CreateMultipartUpload(testutil.AccessKeyID, "bucket", name, uid, nil, nil); err != nil {
			t.Fatal(err)
		}
		if onDisk {
			if err := os.MkdirAll(filepath.Join(uploadsDir, uid.String()), 0700); err != nil {
				t.Fatal(err)
			}
		}
		return uid
	}
	addPart := func(name string, uid s3.UploadID, partNumber int, onDisk bool) {
		t.Helper()
		if _, _, err := store.AddMultipartPart(testutil.AccessKeyID, "bucket", name, uid, "data.part", partNumber, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
			t.Fatal(err)
		}
		if onDisk {
			partDir := filepath.Join(uploadsDir, uid.String(), strconv.Itoa(partNumber))
			if err := os.MkdirAll(partDir, 0700); err != nil {
				t.Fatal(err)
			} else if err := os.WriteFile(filepath.Join(partDir, "data.part"), []byte("part"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	completeMultipart := func(name string, uid s3.UploadID) {
		t.Helper()
		if _, _, err := store.CompleteMultipartUpload(testutil.AccessKeyID, "bucket", name, uid, frand.Entropy128(), 2*s3.MinUploadPartSize, s3.ObjectPreconditions{}); err != nil {
			t.Fatal(err)
		}
	}

	putObject("bucket", "kept", true, nil)
	putObject("bucket", "lost", false, nil)
	// a lost object under compliance retention cannot be deleted
	putObject("locked", "retained", false, &s3.ObjectLockState{Mode: s3.ObjectLockModeCompliance, RetainUntil: time.Now().Add(time.Hour)})
	uidKept := createMultipart("mp-kept", true)
	uidLost := createMultipart("mp-lost", false)

	// a completed multipart object is lost when any part file is missing
	uidDone := createMultipart("done-kept", true)
	addPart("done-kept", uidDone, 1, true)
	addPart("done-kept", uidDone, 2, true)
	completeMultipart("done-kept", uidDone)
	uidPartial := createMultipart("done-lost", true)
	addPart("done-lost", uidPartial, 1, true)
	addPart("done-lost", uidPartial, 2, false)
	completeMultipart("done-lost", uidPartial)

	// an upload in flight only drops the parts whose files are missing
	uidParts := createMultipart("mp-parts", true)
	addPart("mp-parts", uidParts, 1, true)
	addPart("mp-parts", uidParts, 2, false)

	// an uploaded object whose pin deadline passed before the snapshot was taken
	memSDK := testutil.NewMemorySDK()
	siaID := stageUpload(t, memSDK, store, "bucket", "unpinned", time.Now().Add(-time.Hour))

	start := func(store *sqlite.Store) {
		t.Helper()
		backend, err := sia.New(t.Context(), memSDK, store, dir, sia.WithUploadDisabled(), sia.WithLogger(log))
		if err != nil {
			t.Fatal(err)
		}
		backend.Close()
	}
	pendingNames := func(store *sqlite.Store) []string {
		t.Helper()
		pending, err := store.ObjectsForUpload()
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(pending))
		for _, obj := range pending {
			names = append(names, obj.Name)
		}
		slices.Sort(names)
		return names
	}
	assertStats := func(store *sqlite.Store, pendingObjects, multipartUploads int64) {
		t.Helper()
		if stats, err := store.UploadStats(); err != nil {
			t.Fatal(err)
		} else if stats.PendingObjects != pendingObjects {
			t.Fatalf("expected %d pending objects, got %d", pendingObjects, stats.PendingObjects)
		} else if stats.MultipartUploads != multipartUploads {
			t.Fatalf("expected %d multipart uploads, got %d", multipartUploads, stats.MultipartUploads)
		}
	}

	// a start on the live database leaves everything in place
	start(store)
	if names := pendingNames(store); !slices.Equal(names, []string{"done-kept", "done-lost", "kept", "lost", "retained"}) {
		t.Fatal("unexpected pending objects", names)
	}
	assertStats(store, 5, 3)

	// a start on a restored image cleans up after the restore
	image := filepath.Join(dir, "image.sqlite")
	if err := store.Backup(t.Context(), image); err != nil {
		t.Fatal(err)
	}
	restored, err := sqlite.OpenDatabase(image, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restored.Close() })
	start(restored)

	if flagged, err := restored.Restored(); err != nil {
		t.Fatal(err)
	} else if flagged {
		t.Fatal("expected the restored flag to be cleared")
	}
	if names := pendingNames(restored); !slices.Equal(names, []string{"done-kept", "kept", "retained"}) {
		t.Fatal("unexpected pending objects", names)
	}
	assertStats(restored, 3, 2)
	if _, err := restored.HasMultipartUpload(testutil.AccessKeyID, "bucket", "mp-kept", uidKept); err != nil {
		t.Fatal(err)
	} else if _, err := restored.HasMultipartUpload(testutil.AccessKeyID, "bucket", "mp-lost", uidLost); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal("expected the lost upload to be aborted, got", err)
	} else if parts, err := restored.MultipartParts(testutil.AccessKeyID, "bucket", "mp-parts", uidParts); err != nil {
		t.Fatal(err)
	} else if len(parts) != 1 || parts[0].PartNumber != 1 {
		t.Fatal("unexpected parts", parts)
	}

	// the unpinned object gets a fresh deadline and pins instead of expiring
	if toPin, err := restored.ObjectsForPinning(time.Now(), 10); err != nil {
		t.Fatal(err)
	} else if len(toPin) != 1 || toPin[0].SiaObject.ID != siaID {
		t.Fatal("unexpected unpinned objects", toPin)
	} else if !toPin[0].PinBefore.After(time.Now()) {
		t.Fatal("expected a fresh pin deadline, got", toPin[0].PinBefore)
	}
}

// TestPruneSlabs verifies that the Sia backend's background prune loop invokes
// PruneSlabs at startup and once per interval, so slab garbage left behind by
// uploads that never pinned their object is reclaimed.
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
			time.Sleep(sia.PruneSlabsInterval)
			synctest.Wait()
			if got := memSDK.PruneSlabsCalls(); got != i+1 {
				t.Fatalf("after %d intervals: expected %d PruneSlabs calls, got %d", i, i+1, got)
			}
		}
	})
}
