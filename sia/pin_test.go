package sia_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func newPinLoopBackend(t *testing.T, memSDK *testutil.MemorySDK) (*sia.Sia, *sqlite.Store) {
	t.Helper()
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	backend, err := sia.New(t.Context(), memSDK, store, dir,
		sia.WithUploadDisabled(), sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })

	return backend, store
}

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
	if err := store.MarkObjectUploaded(bucket, name, md5, sealed, pinBefore); err != nil {
		t.Fatal(err)
	}
	return sealed.ID()
}

// TestPinLoopRetriesOnFailure verifies that when PinObject returns an error,
// the pin loop bumps next_attempt_at instead of leaving the row immediately due
// again or dropping it.
func TestPinLoopRetriesOnFailure(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := newPinLoopBackend(t, memSDK)

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
	obj, err := store.GetObject(testutil.AccessKeyID, bucket, name, nil)
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
	backend, store := newPinLoopBackend(t, memSDK)

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
	obj, err := store.GetObject(testutil.AccessKeyID, bucket, name, nil)
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

	// the previous Sia ID should be marked for orphan cleanup
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
		t.Fatalf("expected prior sia object ID %v to be orphaned, got %v", priorID, orphans)
	}
}
