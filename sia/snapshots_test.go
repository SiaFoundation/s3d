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
	"go.uber.org/zap/zaptest"
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

	// a pin failure rolls the snapshot and object back
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
}

func TestSnapshotRecovery(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)

	openBackend := func(dir string) (*sqlite.Store, *sia.Sia) {
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

	// snapshotEvent builds the event the indexer emits for a live snapshot
	// object, carrying the object's metadata
	snapshotEvent := func(id types.Hash256, at time.Time) sdk.ObjectEvent {
		t.Helper()
		raw, ok := memSDK.ObjectMetadata(id)
		if !ok {
			t.Fatal("snapshot object not found")
		}
		obj := sdk.NewEmptyObject()
		obj.UpdateMetadata(raw)
		return sdk.ObjectEvent{Key: id, UpdatedAt: at, Object: &obj}
	}

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

	storeA, backendA := openBackend(t.TempDir())

	// create two snapshots, the second backs up the first's record
	snap1, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// delete the first snapshot and unpin its object, its record now only
	// lives on inside the second snapshot
	if err := storeA.DeleteSnapshot(snap1.ID); err != nil {
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
	storeB, backendB := openBackend(dirB)

	// the image's own in-flight record was removed on startup, the deleted
	// first snapshot's record dangles until the sync drops it
	assertSnapshots(storeB, snap1.SiaObjectID)

	// the sync replays the first snapshot's deletion and adopts the second
	// and third from the network
	eventTime := time.Now().Truncate(time.Second)
	events := []sdk.ObjectEvent{
		{Key: snap1.SiaObjectID, UpdatedAt: eventTime.Add(time.Second), Deleted: true},
		snapshotEvent(snap2.SiaObjectID, eventTime.Add(2*time.Second)),
		snapshotEvent(snap3.SiaObjectID, eventTime.Add(3*time.Second)),
	}
	memSDK.SetEvents(events)
	backendB.SyncMetadata(t.Context())
	if snapshots := assertSnapshots(storeB, snap2.SiaObjectID, snap3.SiaObjectID); snapshots[0].CreatedAt.Unix() != snap2.CreatedAt.Unix() {
		t.Fatal("mismatch", snapshots[0].CreatedAt)
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
	storeC, backendC := openBackend(t.TempDir())
	memSDK.SetEvents(append(events, snapshotEvent(snapB.SiaObjectID, eventTime.Add(4*time.Second))))
	backendC.SyncMetadata(t.Context())
	assertSnapshots(storeC, snap2.SiaObjectID, snap3.SiaObjectID, snapB.SiaObjectID)
	if err := backendC.Close(); err != nil {
		t.Fatal(err)
	}
}
