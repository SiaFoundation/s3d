package sia_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	_ "time/tzdata"

	"github.com/SiaFoundation/s3d/build"
	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// downloadSnapshot fetches a snapshot's Sia object from the SDK and returns
// the decompressed database image.
func downloadSnapshot(t *testing.T, memSDK *testutil.MemorySDK, id types.Hash256) []byte {
	t.Helper()
	data, ok := memSDK.ObjectData(id)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	image, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

// downloadMetadata fetches and decodes a snapshot object's metadata.
func downloadMetadata(t *testing.T, memSDK *testutil.MemorySDK, id types.Hash256) objects.SnapshotMetadata {
	t.Helper()
	raw, ok := memSDK.ObjectMetadata(id)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	var meta objects.SnapshotMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

// snapshotEvent builds the event the indexer emits for a live snapshot
// object, carrying the object's metadata.
func snapshotEvent(t *testing.T, memSDK *testutil.MemorySDK, id types.Hash256, at time.Time) sdk.ObjectEvent {
	t.Helper()
	raw, ok := memSDK.ObjectMetadata(id)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	obj := sdk.NewEmptyObject().WithMetadata(raw)
	return sdk.ObjectEvent{ID: id, UpdatedAt: at, Object: obj}
}

func TestCreateSnapshot(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	// create a snapshot
	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// exactly one object was uploaded and pinned
	if memSDK.ObjectCount() != 1 {
		t.Fatal("unexpected", memSDK.ObjectCount())
	} else if memSDK.PinAttempts() != 1 {
		t.Fatal("unexpected", memSDK.PinAttempts())
	}

	// the snapshot is listed with its sia object id and the tag is on the object
	snapshots, err := store.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}
	meta := downloadMetadata(t, memSDK, snap.SiaObjectID)
	if meta.Type != objects.SnapshotType {
		t.Fatal("unexpected", meta.Type)
	} else if meta.DBVersion != store.DBVersion() {
		t.Fatal("unexpected", meta.DBVersion)
	} else if meta.Encoding != objects.SnapshotEncodingGzip {
		t.Fatal("unexpected", meta.Encoding)
	} else if meta.Generation != 1 {
		t.Fatal("unexpected", meta.Generation)
	} else if meta.ObjectCount != snap.ObjectCount {
		t.Fatal("unexpected", meta.ObjectCount)
	} else if meta.S3DVersion != build.Version() {
		t.Fatal("unexpected", meta.S3DVersion)
	} else if meta.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created at")
	}

	// the uploaded snapshot decompresses to a SQLite database
	if db := downloadSnapshot(t, memSDK, snap.SiaObjectID); !bytes.HasPrefix(db, []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected snapshot header")
	}

	// no temporary snapshot files or sidecars are left behind
	entries, err := os.ReadDir(filepath.Join(backend.Dir, sia.TmpDirectory))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot-") {
			t.Fatal("leftover temp file", e.Name())
		}
	}

	// the backend lists the snapshot it recorded and reports the sync state
	backend.SyncMetadata(t.Context())
	if listed, err := backend.ListSnapshots(t.Context()); err != nil {
		t.Fatal(err)
	} else if len(listed.Snapshots) != 1 {
		t.Fatal("unexpected", len(listed.Snapshots))
	} else if listed.Snapshots[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", listed.Snapshots[0].SiaObjectID)
	} else if !listed.Synced {
		t.Fatal("expected synced")
	}

	// a failing sync clears the synced flag
	memSDK.SetEventsError(errors.New("events failed"))
	backend.SyncMetadata(t.Context())
	if listed, err := backend.ListSnapshots(t.Context()); err != nil {
		t.Fatal(err)
	} else if listed.Synced {
		t.Fatal("expected not synced")
	}
	memSDK.SetEventsError(nil)

	// a pin failure rolls the snapshot back, the staged object is never stored
	memSDK.SetPinError(errors.New("pin failed"))
	if _, err := backend.CreateSnapshot(t.Context()); err == nil {
		t.Fatal("expected error")
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("rollback left an extra snapshot", len(snapshots))
	} else if memSDK.ObjectCount() != 1 {
		t.Fatal("rollback left an extra object", memSDK.ObjectCount())
	}

	// the rollback marked the staged snapshot for deletion. The deletion pass
	// leaves it alone until the confirm delay has passed, the pin may still
	// land, then a not found reply drops the record
	backend.SyncMetadata(t.Context())
	assertDeleting(t, store, 1)
	backend.ProcessSnapshotDeletions(t.Context(), time.Now())
	assertDeleting(t, store, 1)
	backend.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	assertDeleting(t, store, 0)

	// deleting the completed snapshot unpins its object once the delay has
	// passed and keeps the record until a second pass confirms the object is
	// gone
	if err := store.RollbackSnapshot(snap.ID); err != nil {
		t.Fatal(err)
	}
	backend.ProcessSnapshotDeletions(t.Context(), time.Now())
	if !memSDK.Pinned(snap.SiaObjectID) {
		t.Fatal("snapshot object unpinned before the confirm delay")
	}
	backend.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	if memSDK.Pinned(snap.SiaObjectID) {
		t.Fatal("snapshot object still pinned")
	}
	assertDeleting(t, store, 1)
	backend.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	assertDeleting(t, store, 0)
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 0 {
		t.Fatal("unexpected", len(snapshots))
	}

	// deleting a snapshot that is already gone reports not found
	if err := backend.DeleteSnapshot(t.Context(), snap.SiaObjectID); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}

	// an id that belongs to an ordinary object is not a snapshot, so it is
	// reported not found and its data is left pinned
	other, err := memSDK.AddObject(t.Context(), strings.NewReader("not a snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.DeleteSnapshot(t.Context(), memSDK.ObjectID(other)); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	} else if !memSDK.Pinned(memSDK.ObjectID(other)) {
		t.Fatal("unexpected", memSDK.ObjectID(other))
	}

	// an object the network has already dropped still clears its record, so a
	// delete is never blocked by an object that is gone
	memSDK.SetPinError(nil)
	gone, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := memSDK.DeleteObject(t.Context(), gone.SiaObjectID); err != nil {
		t.Fatal(err)
	}
	if err := backend.DeleteSnapshot(t.Context(), gone.SiaObjectID); err != nil {
		t.Fatal(err)
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 0 {
		t.Fatal("unexpected", len(snapshots))
	}

	// any other unpin failure keeps the record so the delete can be retried
	kept, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	memSDK.SetDeleteError(errors.New("unpin failed"))
	if err := backend.DeleteSnapshot(t.Context(), kept.SiaObjectID); err == nil {
		t.Fatal("expected error")
	}
	memSDK.SetDeleteError(nil)
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != kept.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}
}

// TestCreateSnapshotFlushesPendingObjects verifies that a snapshot flushes the
// objects still buffered on disk and counts them.
func TestCreateSnapshotFlushesPendingObjects(t *testing.T) {
	backend, store := testutil.NewBackend(t)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const bucket = "snapshot-bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "pending", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}

	// the upload loop is disabled, so the object is still buffered on disk
	if stats, err := store.UploadStats(); err != nil {
		t.Fatal(err)
	} else if stats.PendingObjects != 1 {
		t.Fatal("unexpected", stats.PendingObjects)
	}

	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// the flush drained the pending object and the snapshot captured it
	if stats, err := store.UploadStats(); err != nil {
		t.Fatal(err)
	} else if stats.PendingObjects != 0 {
		t.Fatal("unexpected", stats.PendingObjects)
	}
	if snap.ObjectCount != 1 {
		t.Fatal("unexpected", snap.ObjectCount)
	}
}

// TestCreateSnapshotPendingUploadMissing verifies that a pending object whose
// local file is gone does not block a snapshot.
func TestCreateSnapshotPendingUploadMissing(t *testing.T) {
	backend, store := testutil.NewBackend(t)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const bucket = "snapshot-bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "pending", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}

	// the upload loop is disabled, so the object is still buffered on disk
	if stats, err := store.UploadStats(); err != nil {
		t.Fatal(err)
	} else if stats.PendingObjects != 1 {
		t.Fatal("unexpected", stats.PendingObjects)
	}

	uploadDir := filepath.Join(backend.Dir, sia.UploadsDirectory)
	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("unexpected", len(entries))
	} else if err := os.Remove(filepath.Join(uploadDir, entries[0].Name())); err != nil {
		t.Fatal(err)
	}

	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	} else if snapshots[0].ObjectCount != 0 || snap.ObjectCount != 0 {
		t.Fatal("expected the snapshot to leave the buffered object out", snapshots[0].ObjectCount, snap.ObjectCount)
	}

	// the live database still lists the object
	if stats, err := store.UploadStats(); err != nil {
		t.Fatal(err)
	} else if stats.PendingObjects != 1 {
		t.Fatal("unexpected", stats.PendingObjects)
	}
}

// TestCreateSnapshotSyncRace verifies that CreateSnapshot succeeds when the
// sync loop completes the snapshot before CreateSnapshot marks it pinned
// itself, instead of rolling back the completed snapshot.
func TestCreateSnapshotSyncRace(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	// the sync loop observes the pin before CreateSnapshot resumes
	memSDK.SetPinHook(func(obj *sdk.Object) {
		memSDK.SetEvents([]sdk.ObjectEvent{snapshotEvent(t, memSDK, memSDK.ObjectID(obj), time.Now())})
		backend.SyncMetadata(t.Context())
	})

	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// the completed snapshot is listed and not marked for deletion
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}
	assertDeleting(t, store, 0)
}

// TestStuckPinningSnapshot verifies that a snapshot left awaiting its pin by a
// dead process keeps withholding its orphans until the deletion pass confirms
// the indexer does not hold its object, since that pin may still have been
// committing when the process died.
func TestStuckPinningSnapshot(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)
	store, backend := openBackend(t, memSDK, log, t.TempDir())

	const bucket = "bucket"
	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testutil.AccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}

	// a record left awaiting its pin, the state a crash during PinObject leaves
	snap, _, err := store.CreateSnapshot(false)
	if err != nil {
		t.Fatal(err)
	}
	unconfirmed := frand.Entropy256()
	if err := store.MarkSnapshotPinning(snap.ID, unconfirmed, snap.ObjectCount); err != nil {
		t.Fatal(err)
	}

	// an object deleted after that snapshot started, so the snapshot may
	// reference it
	objID := stageUpload(t, memSDK, store, bucket, "a", time.Now().Add(time.Hour))
	if err := backend.PinObjects(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(bucketID(t, store, bucket), s3.ObjectID{Key: "a"}, false); err != nil {
		t.Fatal(err)
	}

	// a drained sync leaves the record alone, so the object stays withheld
	backend.SyncMetadata(t.Context())
	assertDeleting(t, store, 1)
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("an object the unconfirmed snapshot may reference was released", orphans)
	}
	backend.ProcessOrphans(t.Context())
	if !memSDK.Pinned(objID) {
		t.Fatal("an object the unconfirmed snapshot may reference was unpinned")
	}

	// the deletion pass does not touch the record before the confirm delay
	backend.ProcessSnapshotDeletions(t.Context(), time.Now())
	assertDeleting(t, store, 1)

	// past the delay a not found reply drops the record and releases the object
	backend.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	assertDeleting(t, store, 0)
	backend.ProcessOrphans(t.Context())
	if memSDK.Pinned(objID) {
		t.Fatal("an object only a removed snapshot referenced is still pinned")
	}
}

// assertDeleting fatals unless the number of snapshots awaiting deletion,
// marked for it or stuck awaiting their pin, is want.
func assertDeleting(t *testing.T, store *sqlite.Store, want int) {
	t.Helper()
	ids, err := store.SnapshotsForDeletion(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != want {
		t.Fatal("unexpected", len(ids))
	}
}

// pinSnapshotObject uploads and pins a snapshot object carrying the metadata
// CreateSnapshot writes for snap, the network state a pin that landed leaves.
func pinSnapshotObject(t *testing.T, memSDK *testutil.MemorySDK, store *sqlite.Store, snap s3.Snapshot, gen int64) types.Hash256 {
	t.Helper()
	meta, err := json.Marshal(objects.SnapshotMetadata{
		Type:        objects.SnapshotType,
		CreatedAt:   snap.CreatedAt,
		DBVersion:   store.DBVersion(),
		Encoding:    objects.SnapshotEncodingGzip,
		Generation:  gen,
		ObjectCount: snap.ObjectCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	obj, err := memSDK.Upload(t.Context(), sdk.NewEmptyObject().WithMetadata(meta),
		bytes.NewReader([]byte("backup")))
	if err != nil {
		t.Fatal(err)
	}
	if err := memSDK.PinObject(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
	return memSDK.ObjectID(obj)
}

// openBackend opens the store and backend in dir. Reusing a directory
// simulates a restart.
func openBackend(t *testing.T, memSDK *testutil.MemorySDK, log *zap.Logger, dir string) (*sqlite.Store, *sia.Sia) {
	t.Helper()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	backend, err := sia.New(t.Context(), memSDK, store, dir, sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	return store, backend
}

// TestSnapshotStartup verifies how a restart resolves snapshots a crash left
// awaiting their pin: one whose pin landed is completed by its event, one
// whose pin never reached the indexer is reaped once the confirm delay has
// passed, and neither happens while the sync gate is closed.
func TestSnapshotStartup(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	storeA, backendA := openBackend(t, memSDK, log, dir)

	// a snapshot whose pin landed before the process died, so the indexer
	// holds its object while the record still awaits the pin
	landed, landedGen, err := storeA.CreateSnapshot(false)
	if err != nil {
		t.Fatal(err)
	}
	landedObjID := pinSnapshotObject(t, memSDK, storeA, landed, landedGen)
	if err := storeA.MarkSnapshotPinning(landed.ID, landedObjID, landed.ObjectCount); err != nil {
		t.Fatal(err)
	}

	// a snapshot whose pin never reached the indexer
	lost, _, err := storeA.CreateSnapshot(false)
	if err != nil {
		t.Fatal(err)
	}
	lostObjID := frand.Entropy256()
	if err := storeA.MarkSnapshotPinning(lost.ID, lostObjID, lost.ObjectCount); err != nil {
		t.Fatal(err)
	}
	if err := backendA.Close(); err != nil {
		t.Fatal(err)
	}

	// the restart leaves both records awaiting their pin
	storeB, backendB := openBackend(t, memSDK, log, dir)
	assertDeleting(t, storeB, 2)

	// nothing is reaped while the event stream cannot be drained, the sync
	// has to get the chance to complete a record whose pin landed
	memSDK.SetEventsError(errors.New("indexer unavailable"))
	backendB.SyncMetadata(t.Context())
	backendB.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	assertDeleting(t, storeB, 2)
	memSDK.SetEventsError(nil)

	// the sync completes the landed snapshot from its event
	memSDK.SetEvents([]sdk.ObjectEvent{snapshotEvent(t, memSDK, landedObjID, time.Now())})
	backendB.SyncMetadata(t.Context())
	if snapshots, err := storeB.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].ID != landed.ID {
		t.Fatal("mismatch", snapshots[0].ID)
	} else if snapshots[0].SiaObjectID != landedObjID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}
	assertDeleting(t, storeB, 1)

	// past the confirm delay the deletion pass reaps the lost snapshot, the
	// completed one stays pinned
	backendB.ProcessSnapshotDeletions(t.Context(), time.Now().Add(sia.SnapshotConfirmDelay))
	assertDeleting(t, storeB, 0)
	if known, err := storeB.HasSnapshotObject(lostObjID); err != nil {
		t.Fatal(err)
	} else if known {
		t.Fatal("expected the snapshot whose pin never landed to be removed")
	}
	if !memSDK.Pinned(landedObjID) {
		t.Fatal("expected snapshot object to stay pinned")
	}
	if snapshots, err := storeB.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	}
	if err := backendB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRecovery(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)

	assertSnapshots := func(store *sqlite.Store, want ...types.Hash256) []s3.Snapshot {
		t.Helper()
		snapshots, err := store.ListSnapshots()
		if err != nil {
			t.Fatal(err)
		} else if len(snapshots) != len(want) {
			t.Fatal("unexpected", len(snapshots))
		}
		for i := range want {
			if snapshots[i].SiaObjectID != want[i] {
				t.Fatal("mismatch", snapshots[i].SiaObjectID)
			}
		}
		return snapshots
	}

	storeA, backendA := openBackend(t, memSDK, log, t.TempDir())
	eventTime := time.Now().Truncate(time.Second)

	// create two snapshots, syncing each one's event, the second backs up the
	// first's record
	snap1, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	memSDK.SetEvents([]sdk.ObjectEvent{snapshotEvent(t, memSDK, snap1.SiaObjectID, eventTime)})
	backendA.SyncMetadata(t.Context())
	snap2, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	memSDK.SetEvents([]sdk.ObjectEvent{
		snapshotEvent(t, memSDK, snap1.SiaObjectID, eventTime),
		snapshotEvent(t, memSDK, snap2.SiaObjectID, eventTime.Add(time.Second)),
	})
	backendA.SyncMetadata(t.Context())

	// delete the first snapshot and unpin its object, its record now only
	// lives on inside the second snapshot
	if _, err := storeA.DeleteSnapshotsBySiaObject(snap1.SiaObjectID); err != nil {
		t.Fatal(err)
	} else if err := memSDK.DeleteObject(t.Context(), snap1.SiaObjectID); err != nil {
		t.Fatal(err)
	}

	// a third, scheduled snapshot only exists on the network, not in the
	// second snapshot
	snap3, err := backendA.CreateScheduledSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	} else if !downloadMetadata(t, memSDK, snap3.SiaObjectID).Scheduled {
		t.Fatal("expected scheduled metadata")
	}
	if err := backendA.Close(); err != nil {
		t.Fatal(err)
	}

	// restore the second snapshot into a fresh directory
	dirB := t.TempDir()
	if err := os.WriteFile(filepath.Join(dirB, "s3d.sqlite"), downloadSnapshot(t, memSDK, snap2.SiaObjectID), 0600); err != nil {
		t.Fatal(err)
	}
	memSDK.SetEvents(nil)
	storeB, backendB := openBackend(t, memSDK, log, dirB)

	// the image's own in-flight record was removed on startup, the deleted
	// first snapshot's record dangles until the sync drops it
	assertSnapshots(storeB, snap1.SiaObjectID)

	// the sync replays the first snapshot's deletion and adopts the second
	// and third from the network
	events := []sdk.ObjectEvent{
		{ID: snap1.SiaObjectID, UpdatedAt: eventTime.Add(2 * time.Second), Deleted: true},
		snapshotEvent(t, memSDK, snap2.SiaObjectID, eventTime.Add(3*time.Second)),
		snapshotEvent(t, memSDK, snap3.SiaObjectID, eventTime.Add(4*time.Second)),
	}
	memSDK.SetEvents(events)
	backendB.SyncMetadata(t.Context())
	if snapshots := assertSnapshots(storeB, snap3.SiaObjectID, snap2.SiaObjectID); snapshots[1].CreatedAt.Unix() != snap2.CreatedAt.Unix() {
		t.Fatal("mismatch", snapshots[1].CreatedAt)
	} else if !snapshots[0].Scheduled || snapshots[1].Scheduled {
		t.Fatal("scheduled flag not adopted", snapshots[0].Scheduled, snapshots[1].Scheduled)
	}

	// the generation counter continues past the adopted snapshots and their
	// completion bumps: adopting the second snapshot bumps the restored
	// counter to 4, the third raises it to 5 and bumps to 6, so this
	// snapshot is created at 7
	snapB, err := backendB.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if meta := downloadMetadata(t, memSDK, snapB.SiaObjectID); meta.Generation != 7 {
		t.Fatal("unexpected", meta.Generation)
	}
	if err := backendB.Close(); err != nil {
		t.Fatal(err)
	}

	// a fresh database with the same app scope recovers every live snapshot
	// from the network alone
	storeC, backendC := openBackend(t, memSDK, log, t.TempDir())
	memSDK.SetEvents(append(events, snapshotEvent(t, memSDK, snapB.SiaObjectID, eventTime.Add(5*time.Second))))
	backendC.SyncMetadata(t.Context())
	assertSnapshots(storeC, snapB.SiaObjectID, snap3.SiaObjectID, snap2.SiaObjectID)
	if err := backendC.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListRemoteSnapshots(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	snap1, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// an ordinary object carries no snapshot tag and must be ignored
	other, err := memSDK.AddObject(t.Context(), strings.NewReader("not a snapshot"))
	if err != nil {
		t.Fatal(err)
	}

	at := time.Now().Truncate(time.Second)
	events := []sdk.ObjectEvent{
		snapshotEvent(t, memSDK, snap2.SiaObjectID, at.Add(2*time.Second)),
		snapshotEvent(t, memSDK, snap1.SiaObjectID, at.Add(time.Second)),
		{ID: memSDK.ObjectID(other), UpdatedAt: at.Add(3 * time.Second), Object: other},
	}
	memSDK.SetEvents(events)

	// the network is enumerated newest first
	remote, err := sia.ListRemoteSnapshots(t.Context(), memSDK)
	if err != nil {
		t.Fatal(err)
	} else if len(remote) != 2 {
		t.Fatal("unexpected", len(remote))
	} else if remote[0].ObjectID != snap2.SiaObjectID {
		t.Fatal("mismatch", remote[0].ObjectID)
	} else if remote[1].ObjectID != snap1.SiaObjectID {
		t.Fatal("mismatch", remote[1].ObjectID)
	} else if remote[0].Metadata.Generation <= remote[1].Metadata.Generation {
		t.Fatal("unexpected", remote[0].Metadata.Generation, remote[1].Metadata.Generation)
	} else if remote[0].Metadata.CreatedAt.Unix() != snap2.CreatedAt.Unix() {
		t.Fatal("mismatch", remote[0].Metadata.CreatedAt)
	}

	// the snapshot downloads and decompresses to a database image
	var buf bytes.Buffer
	if err := sia.DownloadSnapshot(t.Context(), memSDK, remote[0], &buf); err != nil {
		t.Fatal(err)
	} else if !bytes.HasPrefix(buf.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected snapshot header")
	}

	// a deleted snapshot drops out of the listing
	memSDK.SetEvents(append(events, sdk.ObjectEvent{ID: snap1.SiaObjectID, UpdatedAt: at.Add(4 * time.Second), Deleted: true}))
	if remote, err := sia.ListRemoteSnapshots(t.Context(), memSDK); err != nil {
		t.Fatal(err)
	} else if len(remote) != 1 {
		t.Fatal("unexpected", len(remote))
	} else if remote[0].ObjectID != snap2.SiaObjectID {
		t.Fatal("mismatch", remote[0].ObjectID)
	}

	// the snapshot created later carries the lower generation, so ordering on
	// generation alone would return the two the wrong way round
	newerID := pinSnapshotObject(t, memSDK, store, s3.Snapshot{CreatedAt: at.Add(time.Hour)}, 1)
	olderID := pinSnapshotObject(t, memSDK, store, s3.Snapshot{CreatedAt: at}, 2)
	memSDK.SetEvents([]sdk.ObjectEvent{
		snapshotEvent(t, memSDK, olderID, at.Add(5*time.Second)),
		snapshotEvent(t, memSDK, newerID, at.Add(6*time.Second)),
	})
	if remote, err := sia.ListRemoteSnapshots(t.Context(), memSDK); err != nil {
		t.Fatal(err)
	} else if len(remote) != 2 {
		t.Fatal("unexpected", len(remote))
	} else if remote[0].ObjectID != newerID {
		t.Fatal("mismatch", remote[0].ObjectID)
	} else if remote[1].ObjectID != olderID {
		t.Fatal("mismatch", remote[1].ObjectID)
	}
}

// TestFetchRemoteSnapshot verifies that fetching a snapshot by its object ID
// does not enumerate the account.
func TestFetchRemoteSnapshot(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, _ := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	other, err := memSDK.AddObject(t.Context(), strings.NewReader("not a snapshot"))
	if err != nil {
		t.Fatal(err)
	}

	// the background sync loop enumerates too, so stop it before counting the
	// calls this fetch makes
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	before := memSDK.ObjectEventCalls()
	remote, err := sia.FetchRemoteSnapshot(t.Context(), memSDK, snap.SiaObjectID)
	if err != nil {
		t.Fatal(err)
	} else if remote.ObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", remote.ObjectID)
	} else if remote.Metadata.ObjectCount != snap.ObjectCount {
		t.Fatal("unexpected", remote.Metadata.ObjectCount)
	} else if calls := memSDK.ObjectEventCalls(); calls != before {
		t.Fatal("fetching by id enumerated the account", calls-before, "times")
	}

	// the fetched snapshot is usable, not just described
	var buf bytes.Buffer
	if err := sia.DownloadSnapshot(t.Context(), memSDK, remote, &buf); err != nil {
		t.Fatal(err)
	} else if !bytes.HasPrefix(buf.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected snapshot header")
	}

	// a cancelled context fails the download instead of reporting a database
	// the caller would then rename into place
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	buf.Reset()
	if err := sia.DownloadSnapshot(cancelled, memSDK, remote, &buf); !errors.Is(err, context.Canceled) {
		t.Fatal("unexpected", err)
	}

	// an object that is not a snapshot is rejected rather than restored
	if _, err := sia.FetchRemoteSnapshot(t.Context(), memSDK, memSDK.ObjectID(other)); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "not a snapshot") {
		t.Fatal("unexpected", err)
	}

	// so is an id that does not exist
	if _, err := sia.FetchRemoteSnapshot(t.Context(), memSDK, types.Hash256(frand.Entropy256())); err == nil {
		t.Fatal("expected an error")
	}
}

// TestSnapshotDueAt verifies the due slot on the day a daylight saving gap
// swallows the scheduled time of day.
func TestSnapshotDueAt(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	// clocks move from 02:00 to 03:00 on 2026-03-29, so 02:30 normalizes to 03:30
	yesterday := time.Date(2026, time.March, 28, 2, 30, 0, 0, loc)
	today := time.Date(2026, time.March, 29, 3, 30, 0, 0, loc)
	for _, tc := range []struct {
		now  time.Time
		want time.Time
	}{
		{time.Date(2026, time.March, 29, 0, 0, 0, 0, loc), yesterday},
		{time.Date(2026, time.March, 29, 3, 29, 0, 0, loc), yesterday},
		{today, today},
	} {
		if due := sia.SnapshotDueAt(tc.now, 2, 30); !due.Equal(tc.want) {
			t.Fatal("unexpected due slot", tc.now, due)
		}
	}
}

// TestScheduledSnapshots verifies the due check, retention and the retry delay
// of the scheduled snapshot pass.
func TestScheduledSnapshots(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))
	ctx := t.Context()

	// the listing is complete, retention may run
	backend.SyncMetadata(ctx)

	// the slot passed an hour ago
	base := time.Now()
	slot := base.Add(-time.Hour)
	hour, minute := slot.Hour(), slot.Minute()

	scheduled := func() []s3.Snapshot {
		t.Helper()
		snapshots, err := store.ListSnapshots()
		if err != nil {
			t.Fatal(err)
		}
		var out []s3.Snapshot
		for _, snap := range snapshots {
			if snap.Scheduled {
				out = append(out, snap)
			}
		}
		return out
	}
	assertScheduled := func(n int) []s3.Snapshot {
		t.Helper()
		snapshots := scheduled()
		if len(snapshots) != n {
			t.Fatal("unexpected", len(snapshots))
		}
		for _, snap := range snapshots {
			if !memSDK.Pinned(snap.SiaObjectID) {
				t.Fatal("expected pinned", snap.SiaObjectID)
			} else if !downloadMetadata(t, memSDK, snap.SiaObjectID).Scheduled {
				t.Fatal("expected scheduled metadata", snap.SiaObjectID)
			}
		}
		return snapshots
	}

	// no scheduled snapshot exists, so the first pass takes one at once
	backend.RunScheduledSnapshot(ctx, base, hour, minute, 2)
	first := assertScheduled(1)[0]

	// a pass inside the same slot does nothing
	backend.RunScheduledSnapshot(ctx, base.Add(30*time.Minute), hour, minute, 2)
	assertScheduled(1)

	// the next day's slot takes another
	backend.RunScheduledSnapshot(ctx, base.AddDate(0, 0, 1), hour, minute, 2)
	second := assertScheduled(2)[0]
	if second.SiaObjectID == first.SiaObjectID {
		t.Fatal("expected a new snapshot")
	}

	// a missed slot is caught up, which pushes the first snapshot past the
	// retention count
	backend.RunScheduledSnapshot(ctx, base.AddDate(0, 0, 3), hour, minute, 2)
	if snapshots := assertScheduled(2); snapshots[1].SiaObjectID != second.SiaObjectID {
		t.Fatal("expected the second snapshot to be kept")
	} else if memSDK.Pinned(first.SiaObjectID) {
		t.Fatal("expected the first snapshot to be pruned")
	}

	// a manual snapshot is never pruned and does not count
	manual, err := backend.CreateSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	backend.RunScheduledSnapshot(ctx, base.AddDate(0, 0, 4), hour, minute, 2)
	if assertScheduled(2); !memSDK.Pinned(manual.SiaObjectID) {
		t.Fatal("expected the manual snapshot to be kept")
	} else if memSDK.Pinned(second.SiaObjectID) {
		t.Fatal("expected the second snapshot to be pruned")
	}

	// zero keeps every scheduled snapshot
	backend.RunScheduledSnapshot(ctx, base.AddDate(0, 0, 5), hour, minute, 0)
	assertScheduled(3)

	// a lower count is applied inside the slot without taking a snapshot
	backend.RunScheduledSnapshot(ctx, base.AddDate(0, 0, 5).Add(time.Minute), hour, minute, 2)
	assertScheduled(2)

	// a failed attempt is retried only after the retry delay
	memSDK.SetPinError(errors.New("pin failed"))
	failedAt := base.AddDate(0, 0, 6)
	backend.RunScheduledSnapshot(ctx, failedAt, hour, minute, 0)
	assertScheduled(2)
	memSDK.SetPinError(nil)
	backend.RunScheduledSnapshot(ctx, failedAt.Add(sia.ScheduledSnapshotRetryDelay-time.Minute), hour, minute, 0)
	assertScheduled(2)
	backend.RunScheduledSnapshot(ctx, failedAt.Add(sia.ScheduledSnapshotRetryDelay), hour, minute, 0)
	assertScheduled(3)
}

// TestScheduledSnapshotLoop verifies the background loop snapshots at startup,
// again when the slot passes, and once a day after that, pruning to the
// retention count each time.
func TestScheduledSnapshotLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		memSDK := testutil.NewMemorySDK()
		log := zaptest.NewLogger(t)
		dir := t.TempDir()
		store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// the slot is two minutes away
		slot := time.Now().Add(2 * time.Minute)
		backend, err := sia.New(t.Context(), memSDK, store, dir,
			sia.WithUploadDisabled(), sia.WithSnapshotSchedule(slot.Hour(), slot.Minute(), 1), sia.WithLogger(log))
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()

		assertSnapshot := func(prev types.Hash256) types.Hash256 {
			t.Helper()
			snapshots, err := store.ListSnapshots()
			if err != nil {
				t.Fatal(err)
			} else if len(snapshots) != 1 {
				t.Fatal("unexpected", len(snapshots))
			} else if !snapshots[0].Scheduled {
				t.Fatal("expected a scheduled snapshot")
			} else if snapshots[0].SiaObjectID == prev {
				t.Fatal("expected a new snapshot")
			} else if prev != (types.Hash256{}) && memSDK.Pinned(prev) {
				t.Fatal("expected the previous snapshot to be pruned")
			}
			return snapshots[0].SiaObjectID
		}

		// the first pass snapshots at once
		synctest.Wait()
		id := assertSnapshot(types.Hash256{})

		// the slot passing takes another, nothing happens in between
		time.Sleep(time.Minute)
		synctest.Wait()
		if snapshots, err := store.ListSnapshots(); err != nil {
			t.Fatal(err)
		} else if len(snapshots) != 1 || snapshots[0].SiaObjectID != id {
			t.Fatal("unexpected snapshot before the slot")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		id = assertSnapshot(id)

		// then once a day
		for range 3 {
			time.Sleep(24 * time.Hour)
			synctest.Wait()
			id = assertSnapshot(id)
		}
	})
}
