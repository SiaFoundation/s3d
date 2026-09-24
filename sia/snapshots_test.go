package sia_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// snapshotEvent builds the event the indexer emits for a live snapshot object,
// carrying the object's slabs and metadata.
func snapshotEvent(t *testing.T, memSDK *testutil.MemorySDK, id types.Hash256, at time.Time) sdk.ObjectEvent {
	t.Helper()
	obj, ok := memSDK.StoredObject(id)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	return sdk.ObjectEvent{Key: id, UpdatedAt: at, Object: &obj}
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

	// the backend lists the snapshot it recorded
	if listed, err := backend.ListSnapshots(t.Context()); err != nil {
		t.Fatal(err)
	} else if len(listed) != 1 {
		t.Fatal("unexpected", len(listed))
	} else if listed[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", listed[0].SiaObjectID)
	}

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
	if err := backend.DeleteSnapshot(t.Context(), other.ID()); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	} else if !memSDK.Pinned(other.ID()) {
		t.Fatal("unexpected", other.ID())
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
	}
}

// TestCreateSnapshotSyncRace verifies that CreateSnapshot succeeds when the
// sync loop completes the snapshot before CreateSnapshot marks it pinned
// itself, instead of rolling back the completed snapshot.
func TestCreateSnapshotSyncRace(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	// the sync loop observes the pin before CreateSnapshot resumes
	memSDK.SetPinHook(func(obj sdk.Object) {
		memSDK.SetEvents([]sdk.ObjectEvent{snapshotEvent(t, memSDK, obj.ID(), time.Now())})
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
	} else if err := store.CreateBucket(testutil.AccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// a record left awaiting its pin, the state a crash during PinObject leaves
	snap, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	unconfirmed := frand.Entropy256()
	if err := store.MarkSnapshotPinning(snap.ID, unconfirmed); err != nil {
		t.Fatal(err)
	}

	// an object deleted after that snapshot started, so its backup may
	// reference it
	objID := stageUpload(t, memSDK, store, bucket, "a", time.Now().Add(time.Hour))
	if err := backend.PinObjects(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(testutil.AccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	// a drained sync leaves the record alone, so the object stays withheld
	backend.SyncMetadata(t.Context())
	assertDeleting(t, store, 1)
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("an object the unconfirmed backup may reference was released", orphans)
	}
	backend.ProcessOrphans(t.Context())
	if !memSDK.Pinned(objID) {
		t.Fatal("an object the unconfirmed backup may reference was unpinned")
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

// pinSnapshotObject uploads and pins a backup object carrying the metadata
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
	obj := sdk.NewEmptyObject()
	obj.UpdateMetadata(meta)
	if err := memSDK.Upload(t.Context(), &obj, bytes.NewReader([]byte("backup"))); err != nil {
		t.Fatal(err)
	} else if err := memSDK.PinObject(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
	return obj.ID()
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
	landed, landedGen, err := storeA.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	landedObjID := pinSnapshotObject(t, memSDK, storeA, landed, landedGen)
	if err := storeA.MarkSnapshotPinning(landed.ID, landedObjID); err != nil {
		t.Fatal(err)
	}

	// a snapshot whose pin never reached the indexer
	lost, _, err := storeA.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	lostObjID := frand.Entropy256()
	if err := storeA.MarkSnapshotPinning(lost.ID, lostObjID); err != nil {
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

	// a third snapshot only exists on the network, not in the second snapshot
	snap3, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
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
		{Key: snap1.SiaObjectID, UpdatedAt: eventTime.Add(2 * time.Second), Deleted: true},
		snapshotEvent(t, memSDK, snap2.SiaObjectID, eventTime.Add(3*time.Second)),
		snapshotEvent(t, memSDK, snap3.SiaObjectID, eventTime.Add(4*time.Second)),
	}
	memSDK.SetEvents(events)
	backendB.SyncMetadata(t.Context())
	if snapshots := assertSnapshots(storeB, snap3.SiaObjectID, snap2.SiaObjectID); snapshots[1].CreatedAt.Unix() != snap2.CreatedAt.Unix() {
		t.Fatal("mismatch", snapshots[1].CreatedAt)
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
	backend, _ := testutil.NewBackend(t, testutil.WithSDK(memSDK))

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
		{Key: other.ID(), UpdatedAt: at.Add(3 * time.Second), Object: &other},
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
	if err := sia.DownloadSnapshot(memSDK, remote[0], &buf); err != nil {
		t.Fatal(err)
	} else if !bytes.HasPrefix(buf.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected snapshot header")
	}

	// a deleted snapshot drops out of the listing
	memSDK.SetEvents(append(events, sdk.ObjectEvent{Key: snap1.SiaObjectID, UpdatedAt: at.Add(4 * time.Second), Deleted: true}))
	if remote, err := sia.ListRemoteSnapshots(t.Context(), memSDK); err != nil {
		t.Fatal(err)
	} else if len(remote) != 1 {
		t.Fatal("unexpected", len(remote))
	} else if remote[0].ObjectID != snap2.SiaObjectID {
		t.Fatal("mismatch", remote[0].ObjectID)
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
	if err := sia.DownloadSnapshot(memSDK, remote, &buf); err != nil {
		t.Fatal(err)
	} else if !bytes.HasPrefix(buf.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected snapshot header")
	}

	// an object that is not a snapshot is rejected rather than restored
	if _, err := sia.FetchRemoteSnapshot(t.Context(), memSDK, other.ID()); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "not a snapshot") {
		t.Fatal("unexpected", err)
	}

	// so is an id that does not exist
	if _, err := sia.FetchRemoteSnapshot(t.Context(), memSDK, types.Hash256(frand.Entropy256())); err == nil {
		t.Fatal("expected an error")
	}
}
