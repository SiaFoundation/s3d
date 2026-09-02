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
	backend.SetSnapshotObserveTimeout(0)

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

	// the sync completes the record once the indexer confirms the object
	backend.SyncMetadata(t.Context())

	// the snapshot is recorded with a sia object id and the tag is on the object
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

	// the rollback marked the staged snapshot for deletion, an early not
	// found reply does not confirm it
	assertDeleting(t, store, 1)
	backend.ProcessSnapshotDeletions(t.Context())
	assertDeleting(t, store, 1)

	// after the confirm delay a not found reply drops the record
	backend.SetSnapshotConfirmDelay(0)
	backend.ProcessSnapshotDeletions(t.Context())
	assertDeleting(t, store, 0)

	// deleting the completed snapshot keeps its record until a second pass
	// confirms the object is gone. The delay is restored first so a background
	// pass cannot drop the record before it is observed
	backend.SetSnapshotConfirmDelay(time.Hour)
	if err := store.RollbackSnapshot(snap.ID); err != nil {
		t.Fatal(err)
	}
	backend.ProcessSnapshotDeletions(t.Context())
	if memSDK.Pinned(snap.SiaObjectID) {
		t.Fatal("Sia object still pinned")
	}
	assertDeleting(t, store, 1)
	backend.SetSnapshotConfirmDelay(0)
	backend.ProcessSnapshotDeletions(t.Context())
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
}

// TestCreateSnapshotListed verifies that a snapshot is listed by the time
// CreateSnapshot returns when the indexer publishes the pin's event.
func TestCreateSnapshotListed(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	memSDK.SetPublishOnPin(true)
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))
	backend.SetSnapshotObserveTimeout(10 * time.Second)

	snap, err := backend.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].ID != snap.ID {
		t.Fatal("mismatch", snapshots[0].ID)
	}
}

// TestReconcileLatePin verifies that a snapshot left awaiting its pin by a
// dead process keeps withholding its orphans while the indexer reports no such
// object, since that pin may still have been committing when the process died.
func TestReconcileLatePin(t *testing.T) {
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

	// the reconcile marks the record for deletion, it does not remove it, so
	// the object stays withheld until the deletion pass confirms the object
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
}

// assertDeleting fatals unless the number of snapshots marked for deletion is
// want, and returns them.
func assertDeleting(t *testing.T, store *sqlite.Store, want int) []objects.DeletingSnapshot {
	t.Helper()
	snapshots, err := store.SnapshotsForDeletion()
	if err != nil {
		t.Fatal(err)
	} else if len(snapshots) != want {
		t.Fatal("unexpected", len(snapshots))
	}
	return snapshots
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
	backend.SetSnapshotObserveTimeout(0)
	return store, backend
}

func TestSnapshotStartup(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	// create a snapshot whose pin landed but was never observed by the sync
	_, backendA := openBackend(t, memSDK, log, dir)
	snap, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := backendA.Close(); err != nil {
		t.Fatal(err)
	}

	// a restart leaves the record awaiting its pin. Its event is still
	// unpublished, so the reconcile resolves it against the indexer instead
	storeB, backendB := openBackend(t, memSDK, log, dir)
	memSDK.SetEvents(nil)

	// an unreachable indexer resolves nothing, the record must survive rather
	// than be treated as a pin that never landed
	memSDK.SetObjectError(errors.New("indexer unavailable"))
	backendB.SyncMetadata(t.Context())
	if known, err := storeB.HasSnapshotObject(snap.SiaObjectID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected the record to survive an unreachable indexer")
	}
	memSDK.SetObjectError(nil)

	backendB.SyncMetadata(t.Context())
	if snapshots, err := storeB.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].ID != snap.ID {
		t.Fatal("mismatch", snapshots[0].ID)
	} else if snapshots[0].SiaObjectID != snap.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}

	// the backup is never marked for deletion and its object stays pinned
	assertDeleting(t, storeB, 0)
	backendB.ProcessSnapshotDeletions(t.Context())
	if !memSDK.Pinned(snap.SiaObjectID) {
		t.Fatal("expected snapshot object to stay pinned")
	}

	if err := backendB.Close(); err != nil {
		t.Fatal(err)
	}

	// a record left behind by an earlier process whose pin never reached the
	// network, so the indexer never held its object
	snap2, _, err := storeB.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	lostObjID := frand.Entropy256()
	if err := storeB.MarkSnapshotPinning(snap2.ID, lostObjID); err != nil {
		t.Fatal(err)
	}

	// after a restart the indexer reports no such object, so the record is
	// marked for deletion rather than removed. It keeps withholding until the
	// deletion pass confirms the object is gone
	storeC, backendC := openBackend(t, memSDK, log, dir)
	memSDK.SetEvents(nil)
	backendC.SyncMetadata(t.Context())
	if deleting := assertDeleting(t, storeC, 1); deleting[0].ObjectID != lostObjID {
		t.Fatal("mismatch", deleting[0].ObjectID)
	}

	// the pass confirms it and drops the record
	backendC.SetSnapshotConfirmDelay(0)
	backendC.ProcessSnapshotDeletions(t.Context())
	assertDeleting(t, storeC, 0)
	if known, err := storeC.HasSnapshotObject(lostObjID); err != nil {
		t.Fatal(err)
	} else if known {
		t.Fatal("expected the snapshot whose pin never landed to be removed")
	}
	if snapshots, err := storeC.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	}
	if err := backendC.Close(); err != nil {
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

	// create two snapshots, completing each record via the sync, the second
	// backs up the first's completed record
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
		// completing a snapshot bumps the counter, so the absolute values
		// depend on when the sync observed each pin. Only the order is fixed
		t.Fatal("expected the newer snapshot to hold the higher generation", remote[0].Metadata.Generation)
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

// TestFetchRemoteSnapshot covers the recovery shortcut: fetching a snapshot by
// its object ID must not enumerate the account, because enumeration reads and
// decrypts every object and so grows with how much is stored.
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
