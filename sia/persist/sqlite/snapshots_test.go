package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
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
	if err := store.CreateSnapshot(t.Context(), path); err != nil {
		t.Fatal(err)
	}

	// the backup file is written
	if _, err := os.Stat(path); err != nil {
		t.Fatal("backup file not created", err)
	}

	// only the uploaded object is registered, the pending one has no sia_object_id
	store.assertCount(1, "snapshots")
	store.assertCount(1, "snapshot_objects")

	// the snapshot is listed with its metadata
	snapshots, err := store.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	s := snapshots[0]
	if s.ID == 0 {
		t.Fatal("expected non-zero snapshot id")
	} else if s.Path != path {
		t.Fatalf("expected path %q, got %q", path, s.Path)
	} else if s.ObjectCount != 1 {
		t.Fatalf("expected object count 1, got %d", s.ObjectCount)
	} else if s.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created at")
	}

	// delete the object, still held by the snapshot
	if _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans while snapshotted, got %d", len(orphans))
	}

	// delete the snapshot, releasing the object
	if err := store.DeleteSnapshot(s.ID); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "snapshots")
	store.assertCount(0, "snapshot_objects")
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatalf("expected orphan %v after snapshot delete, got %v", objID, orphans)
	}

	// deleting a snapshot that no longer exists reports not found
	if err := store.DeleteSnapshot(s.ID); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("expected ErrSnapshotNotFound, got", err)
	} else if err := store.DeleteSnapshot(99999); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("expected ErrSnapshotNotFound, got", err)
	}
}
