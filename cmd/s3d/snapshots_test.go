package main

import (
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestSelectSnapshot(t *testing.T) {
	remote := func(dbVersion int64) sia.RemoteSnapshot {
		return sia.RemoteSnapshot{
			ObjectID: types.Hash256(frand.Entropy256()),
			Metadata: objects.SnapshotMetadata{
				Type:      objects.SnapshotType,
				CreatedAt: time.Now(),
				DBVersion: dbVersion,
				Encoding:  objects.SnapshotEncodingGzip,
			},
		}
	}

	version := sqlite.SchemaVersion()
	snapshots := []sia.RemoteSnapshot{remote(version - 1), remote(version)}

	// an empty network has nothing to restore
	if _, err := selectSnapshot(nil, "latest"); err == nil {
		t.Fatal("expected error")
	}

	// latest picks the newest snapshot
	if snap, err := selectSnapshot(snapshots, "latest"); err != nil {
		t.Fatal(err)
	} else if snap.ObjectID != snapshots[1].ObjectID {
		t.Fatal("mismatch", snap.ObjectID)
	}

	// an explicit object id picks that snapshot
	if snap, err := selectSnapshot(snapshots, snapshots[0].ObjectID.String()); err != nil {
		t.Fatal(err)
	} else if snap.ObjectID != snapshots[0].ObjectID {
		t.Fatal("mismatch", snap.ObjectID)
	}

	// an object id that is not on the network reports not found
	if _, err := selectSnapshot(snapshots, types.Hash256(frand.Entropy256()).String()); err == nil {
		t.Fatal("expected error")
	}

	// a malformed object id is rejected
	if _, err := selectSnapshot(snapshots, "not-a-hash"); err == nil {
		t.Fatal("expected error")
	}

	// a snapshot written by a newer build cannot be restored
	newer := remote(version + 1)
	if _, err := selectSnapshot([]sia.RemoteSnapshot{newer}, "latest"); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "newer") {
		t.Fatal("unexpected", err)
	}

	// an older schema restores and is migrated on startup
	if _, err := selectSnapshot(snapshots, snapshots[0].ObjectID.String()); err != nil {
		t.Fatal(err)
	}

	var zero types.Hash256
	if _, err := selectSnapshot(snapshots, zero.String()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRestoreDir(t *testing.T) {
	const dataDir = "/srv/s3d/main"

	// without an output directory a restore overwrites the configured one
	if dir := restoreDir(dataDir, ""); dir != dataDir {
		t.Fatal("unexpected", dir)
	}

	// an output directory leaves the configured one untouched
	if dir := restoreDir(dataDir, "/srv/s3d/drill"); dir != "/srv/s3d/drill" {
		t.Fatal("unexpected", dir)
	}
}
