package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSnapshots(t *testing.T) {
	const bucket = "test-bucket"

	store := initTestDB(t, zaptest.NewLogger(t))

	if err := store.CreateBucket(testAccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// upload and pin an object
	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())
	objID := sealed.ID()
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "a", md5, nil, 1, new(string)); err != nil {
		t.Fatal(err)
	} else if err := store.MarkObjectUploaded(bucket, "a", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(objID); err != nil {
		t.Fatal(err)
	}

	// a pending object with only an on-disk file
	pending := "pending-file"
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "pending", frand.Entropy128(), nil, 5, &pending); err != nil {
		t.Fatal(err)
	}

	// create a snapshot
	path := filepath.Join(t.TempDir(), "snap.sqlite")
	s1, err := store.CreateSnapshot(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(path); err != nil {
		t.Fatal("backup file not created", err)
	} else if s1.ID == 0 {
		t.Fatal("expected non-zero snapshot id")
	} else if s1.ObjectCount != 1 {
		t.Fatal("unexpected", s1.ObjectCount)
	} else if s1.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created at")
	}
	store.assertCount(1, "snapshots")

	// an uploaded snapshot is listed once its object id is recorded
	var siaObjectID types.Hash256
	frand.Read(siaObjectID[:])
	if err := store.SetSnapshotSiaObject(s1.ID, siaObjectID); err != nil {
		t.Fatal(err)
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	} else if snapshots[0].SiaObjectID != siaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	} else if snapshots[0].ObjectCount != 1 {
		t.Fatal("unexpected", snapshots[0].ObjectCount)
	}

	// setting the object id on a missing snapshot reports not found
	if err := store.SetSnapshotSiaObject(s1.ID+100, siaObjectID); !errors.Is(err, objects.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}

	// deleting a non-existent snapshot reports not found
	if err := store.DeleteSnapshot(s1.ID + 100); !errors.Is(err, objects.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}

	// delete the object while the first snapshot still references it
	if _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", len(orphans))
	}

	// a later snapshot taken after the object was deleted does not capture it
	path2 := filepath.Join(t.TempDir(), "snap2.sqlite")
	s2, err := store.CreateSnapshot(t.Context(), path2)
	if err != nil {
		t.Fatal(err)
	} else if s2.ObjectCount != 0 {
		t.Fatal("unexpected", s2.ObjectCount)
	}

	// the un-uploaded second snapshot is excluded from the list
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	}

	// deleting the later snapshot leaves the object withheld by the earlier one
	if err := store.DeleteSnapshot(s2.ID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", len(orphans))
	}

	// deleting the snapshot that captured it releases the object
	if err := store.DeleteSnapshot(s1.ID); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "snapshots")
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}
}
