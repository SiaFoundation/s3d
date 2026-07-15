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
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
)

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
	raw, ok := memSDK.ObjectMetadata(snap.SiaObjectID)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	var meta objects.SnapshotMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	} else if meta.Type != objects.SnapshotType {
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

	// the uploaded backup decompresses to a SQLite database
	data, ok := memSDK.ObjectData(snap.SiaObjectID)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	db, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.HasPrefix(db, []byte("SQLite format 3\x00")) {
		t.Fatal("unexpected backup header")
	}

	// no temporary backup files or sidecars are left behind
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

	openBackend := func(dir string, store *sqlite.Store) *sia.Sia {
		t.Helper()
		backend, err := sia.New(t.Context(), memSDK, store, dir, sia.WithLogger(log))
		if err != nil {
			t.Fatal(err)
		}
		return backend
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

	dirA := t.TempDir()
	storeA, err := sqlite.OpenDatabase(filepath.Join(dirA, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer storeA.Close()
	backendA := openBackend(dirA, storeA)

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
	// lives on inside the second snapshot's backup
	if err := storeA.DeleteSnapshot(snap1.ID); err != nil {
		t.Fatal(err)
	} else if err := memSDK.DeleteObject(t.Context(), snap1.SiaObjectID); err != nil {
		t.Fatal(err)
	}

	// a third snapshot only exists on the network, not in the second backup
	snap3, err := backendA.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := backendA.Close(); err != nil {
		t.Fatal(err)
	}

	// restore the second snapshot's backup into a fresh directory
	data, ok := memSDK.ObjectData(snap2.SiaObjectID)
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
	dirB := t.TempDir()
	if err := os.WriteFile(filepath.Join(dirB, "s3d.sqlite"), image, 0600); err != nil {
		t.Fatal(err)
	}
	storeB, err := sqlite.OpenDatabase(filepath.Join(dirB, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()
	backendB := openBackend(dirB, storeB)

	// the image's own in-flight record was removed on startup, the deleted
	// first snapshot's record dangles until the sync drops it
	if snapshots, err := storeB.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap1.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}

	// the sync replays the first snapshot's deletion and adopts the second
	// and third from the network
	eventTime := time.Now().Truncate(time.Second)
	memSDK.SetEvents([]sdk.ObjectEvent{
		{Key: snap1.SiaObjectID, UpdatedAt: eventTime.Add(time.Second), Deleted: true},
		snapshotEvent(snap2.SiaObjectID, eventTime.Add(2*time.Second)),
		snapshotEvent(snap3.SiaObjectID, eventTime.Add(3*time.Second)),
	})
	backendB.SyncMetadata(t.Context())
	if snapshots, err := storeB.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 2 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap2.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	} else if snapshots[1].SiaObjectID != snap3.SiaObjectID {
		t.Fatal("mismatch", snapshots[1].SiaObjectID)
	} else if snapshots[0].CreatedAt.Unix() != snap2.CreatedAt.Unix() {
		t.Fatal("mismatch", snapshots[0].CreatedAt)
	}

	// the generation counter continues past the adopted snapshots
	snapB, err := backendB.CreateSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := memSDK.ObjectMetadata(snapB.SiaObjectID)
	if !ok {
		t.Fatal("snapshot object not found")
	}
	var meta objects.SnapshotMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	} else if meta.Generation != 4 {
		t.Fatal("unexpected", meta.Generation)
	}
	if err := backendB.Close(); err != nil {
		t.Fatal(err)
	}

	// a fresh database with the same app scope recovers every live snapshot
	// from the network alone
	dirC := t.TempDir()
	storeC, err := sqlite.OpenDatabase(filepath.Join(dirC, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer storeC.Close()
	backendC := openBackend(dirC, storeC)
	memSDK.SetEvents([]sdk.ObjectEvent{
		{Key: snap1.SiaObjectID, UpdatedAt: eventTime.Add(time.Second), Deleted: true},
		snapshotEvent(snap2.SiaObjectID, eventTime.Add(2*time.Second)),
		snapshotEvent(snap3.SiaObjectID, eventTime.Add(3*time.Second)),
		snapshotEvent(snapB.SiaObjectID, eventTime.Add(4*time.Second)),
	})
	backendC.SyncMetadata(t.Context())
	if snapshots, err := storeC.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 3 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != snap2.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	} else if snapshots[1].SiaObjectID != snap3.SiaObjectID {
		t.Fatal("mismatch", snapshots[1].SiaObjectID)
	} else if snapshots[2].SiaObjectID != snapB.SiaObjectID {
		t.Fatal("mismatch", snapshots[2].SiaObjectID)
	}
	if err := backendC.Close(); err != nil {
		t.Fatal(err)
	}
}
