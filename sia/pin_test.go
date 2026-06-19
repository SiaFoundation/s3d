package sia_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"lukechampine.com/frand"
)

// stageUpload simulates the state left behind by a completed
// uploadObjectGroup: the data is in the SDK's in-memory map (so UnsealObject
// works during pinning), the objects row has sia_object_id set, and a
// corresponding unpinned_objects row exists with the given pin_before.
func stageUpload(t *testing.T, memSDK *testutil.MemorySDK, store *sqlite.Store, bucket, name string, pinBefore time.Time) (siaID [32]byte) {
	t.Helper()

	if err := store.CreateBucket(testutil.AccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	data := frand.Bytes(16)
	siaObj, err := memSDK.Upload(t.Context(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sealed := memSDK.SealObject(siaObj)

	fn := name + ".upload"
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(testutil.AccessKeyID, bucket, name, md5, nil, int64(len(data)), &fn); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, name, "", md5, sealed, pinBefore); err != nil {
		t.Fatal(err)
	}
	return sealed.ID()
}

// TestPinLoopRetriesOnFailure verifies that when PinObject returns an error,
// the pin loop bumps next_attempt_at instead of leaving the row immediately due
// again or dropping it.
func TestPinLoopRetriesOnFailure(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	const (
		bucket = "bucket"
		name   = "obj"
	)
	stageUpload(t, memSDK, store, bucket, name, time.Now().Add(time.Hour))

	memSDK.SetPinError(errors.New("indexer unavailable"))

	before := time.Now()
	backend.PinObjects(t.Context())

	if got := memSDK.PinAttempts(); got != 1 {
		t.Fatalf("expected 1 pin attempt, got %d", got)
	}

	// the failure should have bumped next_attempt_at into the future so the
	// row is no longer due
	due, err := store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("expected 0 due rows after retry back-off, got %d", len(due))
	}

	next, ok, err := store.NextPinningAttempt()
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected the row to remain in unpinned_objects after failure")
	} else if !next.After(before) {
		t.Fatalf("expected next_attempt_at after %v, got %v", before, next)
	}

	// the object is still uploaded (sia_object_id set) - failure didn't
	// demote it, just delayed the retry
	obj, err := store.GetObject(testutil.AccessKeyID, bucket, name, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject == nil {
		t.Fatal("expected sia_object to remain set after transient pin failure")
	} else if obj.FileName == nil {
		t.Fatal("expected filename to remain set until pinning succeeds")
	}
}

// TestPinLoopDemotesExpiredUploads verifies that when pin_before has passed
// before the loop can pin, the row is demoted via ScheduleObjectForReupload
// and reappears in the upload queue instead of being retried.
func TestPinLoopDemotesExpiredUploads(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	const (
		bucket = "bucket"
		name   = "obj"
	)
	priorID := stageUpload(t, memSDK, store, bucket, name, time.Now().Add(-time.Minute))

	backend.PinObjects(t.Context())

	// PinObject must not have been called for an expired row
	if got := memSDK.PinAttempts(); got != 0 {
		t.Fatalf("expected 0 pin attempts for expired row, got %d", got)
	}

	// the unpinned_objects row should be gone
	if _, ok, err := store.NextPinningAttempt(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected unpinned_objects row to be removed after demotion")
	}

	// the object should be back in the upload queue with sia_object_id cleared
	obj, err := store.GetObject(testutil.AccessKeyID, bucket, name, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject != nil {
		t.Fatal("expected sia_object to be cleared after demotion")
	} else if obj.FileName == nil {
		t.Fatal("expected filename to be preserved for re-upload")
	}

	uploads, err := store.ObjectsForUpload()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, u := range uploads {
		if u.Bucket == bucket && u.Name == name {
			found = true
		}
	}
	if !found {
		t.Fatal("expected demoted object to be picked up by ObjectsForUpload")
	}

	// the previous upload is orphaned: a pin attempt may have succeeded in
	// the indexer without MarkObjectPinned committing, so the old id is
	// conservatively routed through the orphan path (unpinning never-pinned
	// data is a no-op)
	orphans, err := store.OrphanedObjects(10)
	if err != nil {
		t.Fatal(err)
	}
	var orphaned bool
	for _, id := range orphans {
		if id == priorID {
			orphaned = true
		}
	}
	if !orphaned {
		t.Fatalf("expected demoted prior sia object ID %v to be orphaned, got %v", priorID, orphans)
	}
}

// TestPinLoopPinsCopyAfterSourceDeleted verifies that a copy of an
// uploaded-but-not-yet-pinned object remains pinnable after the source is
// deleted before the pin loop runs. The copy shares the source's sia_object_id
// and filename; once the source is gone the copy must still drive the pin
// lifecycle (pin in indexer, clear filename, release on-disk file).
func TestPinLoopPinsCopyAfterSourceDeleted(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	const (
		bucket  = "bucket"
		srcName = "src"
		dstName = "dst"
	)
	stageUpload(t, memSDK, store, bucket, srcName, time.Now().Add(time.Hour))

	// copy src -> dst while src is uploaded but not yet pinned
	if _, _, err := store.CopyObject(testutil.AccessKeyID, bucket, srcName, s3.NoVersion(), bucket, dstName, nil, true); err != nil {
		t.Fatal(err)
	}

	// delete src before the pin loop has a chance to run; src's
	// unpinned_objects row goes with it via FK cascade
	if _, _, _, err := store.DeleteObject(testutil.AccessKeyID, bucket, s3.ObjectID{Key: srcName}); err != nil {
		t.Fatal(err)
	}

	backend.PinObjects(t.Context())

	// the indexer should have been asked to pin the object at least once
	if got := memSDK.PinAttempts(); got == 0 {
		t.Fatal("expected pin loop to pin the surviving copy, got 0 pin attempts")
	}

	// dst's on-disk file should have been released and its filename cleared
	dst, err := store.GetObject(testutil.AccessKeyID, bucket, dstName, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if dst.SiaObject == nil {
		t.Fatal("expected dst sia_object to remain set")
	} else if dst.FileName != nil {
		t.Fatalf("expected dst filename to be cleared after pin, got %q", *dst.FileName)
	}
}
