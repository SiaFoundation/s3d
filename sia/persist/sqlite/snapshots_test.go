package sqlite

import (
	"errors"
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
	} else if err := store.MarkObjectUploaded(bucket, "a", "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
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
	s1, s1Gen, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	} else if s1.ID == 0 {
		t.Fatal("expected non-zero snapshot id")
	} else if s1Gen == 0 {
		t.Fatal("expected non-zero generation")
	} else if s1.ObjectCount != 1 {
		t.Fatal("unexpected", s1.ObjectCount)
	} else if s1.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created at")
	}
	store.assertCount(1, "snapshots")

	// the pending snapshot counts as an upload in flight
	if known, inFlight, err := store.CheckSnapshotObject(frand.Entropy256()); err != nil {
		t.Fatal(err)
	} else if known {
		t.Fatal("unexpected known object")
	} else if !inFlight {
		t.Fatal("expected snapshot in flight")
	}

	// an uploaded snapshot is listed once its object id is recorded
	var siaObjectID types.Hash256
	frand.Read(siaObjectID[:])
	if err := store.MarkSnapshotPinned(s1.ID, siaObjectID); err != nil {
		t.Fatal(err)
	}

	// the recorded object is known and no upload is in flight anymore
	if known, inFlight, err := store.CheckSnapshotObject(siaObjectID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected known object")
	} else if inFlight {
		t.Fatal("unexpected snapshot in flight")
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
	if err := store.MarkSnapshotPinned(s1.ID+100, siaObjectID); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}

	// deleting a non-existent snapshot reports not found
	if err := store.DeleteSnapshot(s1.ID + 100); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}

	// delete the object while the first snapshot still references it
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	orphanGen := func() (gen int64) {
		t.Helper()
		if err := store.db.QueryRow("SELECT orphaned_at_gen FROM orphaned_objects WHERE sia_object_id = $1", sqlHash256(objID)).Scan(&gen); err != nil {
			t.Fatal(err)
		}
		return
	}

	// the orphan records the generation it was orphaned at, one past the
	// snapshot's completion bump, and is withheld
	if gen := orphanGen(); gen != s1Gen+1 {
		t.Fatal("unexpected", gen)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", len(orphans))
	}

	// a later snapshot taken after the object was deleted does not capture it
	s2, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	} else if s2.ObjectCount != 0 {
		t.Fatal("unexpected", s2.ObjectCount)
	}

	// the second pending snapshot counts as an upload in flight again
	if known, inFlight, err := store.CheckSnapshotObject(siaObjectID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected known object")
	} else if !inFlight {
		t.Fatal("expected snapshot in flight")
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

	// deleting snapshots by an unknown sia object id removes nothing
	if n, err := store.DeleteSnapshotsBySiaObject(frand.Entropy256()); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("unexpected", n)
	}

	// deleting the snapshot that captured it by object id releases the object
	if n, err := store.DeleteSnapshotsBySiaObject(siaObjectID); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}
	store.assertCount(0, "snapshots")
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}

	// deleting a snapshot that no longer exists reports not found
	if err := store.DeleteSnapshot(s1.ID); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("expected ErrSnapshotNotFound, got", err)
	} else if err := store.DeleteSnapshot(99999); !errors.Is(err, s3.ErrSnapshotNotFound) {
		t.Fatal("expected ErrSnapshotNotFound, got", err)
	}

	// adopting a snapshot recreates its record and raises the orphan's
	// generation so the adopted snapshot withholds it again
	adopted, err := store.AdoptSnapshot(siaObjectID, s1.CreatedAt, s1Gen+10, s1.ObjectCount)
	if err != nil {
		t.Fatal(err)
	} else if adopted.SiaObjectID != siaObjectID {
		t.Fatal("mismatch", adopted.SiaObjectID)
	} else if adopted.ObjectCount != s1.ObjectCount {
		t.Fatal("unexpected", adopted.ObjectCount)
	}
	store.assertCount(1, "snapshots")
	if gen := orphanGen(); gen != s1Gen+10 {
		t.Fatal("unexpected", gen)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", len(orphans))
	}

	// adopting the same object again returns the existing record and leaves
	// the orphan generation alone
	if again, err := store.AdoptSnapshot(siaObjectID, s1.CreatedAt, s1Gen+10, s1.ObjectCount); err != nil {
		t.Fatal(err)
	} else if again.ID != adopted.ID {
		t.Fatal("mismatch", again.ID)
	}
	store.assertCount(1, "snapshots")
	if gen := orphanGen(); gen != s1Gen+10 {
		t.Fatal("unexpected", gen)
	}

	// the generation counter is bumped past the adopted generation and its
	// completion bump
	snap3, gen, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	} else if gen != s1Gen+12 {
		t.Fatal("unexpected", gen)
	}

	// deleting the adopted snapshot releases the orphan, the newer pending
	// snapshot does not withhold it
	if err := store.DeleteSnapshot(adopted.ID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}

	addObject := func(key string) types.Hash256 {
		t.Helper()
		obj := newTestObject()
		sealed := obj.Seal(types.GeneratePrivateKey())
		md5 := frand.Entropy128()
		if _, _, err := store.PutObject(testAccessKeyID, bucket, key, md5, nil, 1, new(string)); err != nil {
			t.Fatal(err)
		} else if err := store.MarkObjectUploaded(bucket, key, "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		} else if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
			t.Fatal(err)
		}
		return sealed.ID()
	}

	// an object deleted while the third snapshot's upload is in flight is
	// withheld, it may be captured in the backup
	bID := addObject("b")
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}

	// completing the third snapshot keeps it withheld, it existed before the
	// backup finished
	if err := store.MarkSnapshotPinned(snap3.ID, frand.Entropy256()); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}
	if err := store.RemoveOrphanedObject(objID); err != nil {
		t.Fatal(err)
	}

	// an object created after the snapshot completed is provably absent from
	// its backup, deleting it releases it immediately
	cID := addObject("c")
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "c"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != cID {
		t.Fatal("unexpected", orphans)
	}
	if err := store.RemoveOrphanedObject(cID); err != nil {
		t.Fatal(err)
	}

	// a copy made after a snapshot completes inherits the source's creation
	// stamp, the shared id predates the snapshot and must stay withheld
	dID := addObject("d")
	snap4, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinned(snap4.ID, frand.Entropy256()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CopyObject(testAccessKeyID, bucket, "d", s3.VersionRequest{}, bucket, "d-copy", nil, false); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "d"}); err != nil {
		t.Fatal(err)
	} else if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "d-copy"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", orphans)
	}

	// an object created after a snapshot is adopted cannot appear in the
	// adopted backup either
	adopted2, err := store.AdoptSnapshot(frand.Entropy256(), time.Now(), s1Gen+30, 0)
	if err != nil {
		t.Fatal(err)
	}
	eID := addObject("e")
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "e"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != eID {
		t.Fatal("unexpected", orphans)
	}
	if err := store.RemoveOrphanedObject(eID); err != nil {
		t.Fatal(err)
	}

	// deleting every snapshot releases the remaining orphans
	if err := store.DeleteSnapshot(snap3.ID); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSnapshot(snap4.ID); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSnapshot(adopted2.ID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 2 {
		t.Fatal("unexpected", orphans)
	} else if (orphans[0] != bID && orphans[1] != bID) || (orphans[0] != dID && orphans[1] != dID) {
		t.Fatal("unexpected", orphans)
	}
}

// TestAdoptSnapshotWithholdsExistingOrphans verifies that an adopted snapshot
// withholds orphans recorded before the adoption even when an earlier adoption
// advanced the local counter to its generation. This can happen when snapshots
// from different histories remain on the network after multiple restores.
func TestAdoptSnapshotWithholdsExistingOrphans(t *testing.T) {
	const bucket = "test-bucket"

	store := initTestDB(t, zaptest.NewLogger(t))

	if err := store.CreateBucket(testAccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// upload and pin an object that exists in the database being restored
	obj := newTestObject()
	sealed := obj.Seal(types.GeneratePrivateKey())
	objID := sealed.ID()
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "a", md5, nil, 1, new(string)); err != nil {
		t.Fatal(err)
	} else if err := store.MarkObjectUploaded(bucket, "a", "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(objID); err != nil {
		t.Fatal(err)
	}

	// model the restored database image. Its own snapshot row was incomplete
	// when the image was made, so startup removes the row but retains the
	// generation counter.
	restored, restoredGen, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := store.DeleteIncompleteSnapshots(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}

	// this history deletes the object before discovering the snapshots still
	// present on the network
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	// adopt the restored image's own snapshot. It withholds the orphan and its
	// completion bump advances the counter to the next generation.
	earlier, err := store.AdoptSnapshot(frand.Entropy256(), restored.CreatedAt, restoredGen, restored.ObjectCount)
	if err != nil {
		t.Fatal(err)
	}

	// another history created a snapshot immediately after restoring the same
	// image, so its generation is now equal to the counter advanced above. Its
	// backup may still reference the locally orphaned object.
	adopted, err := store.AdoptSnapshot(frand.Entropy256(), time.Now(), restoredGen+1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", orphans)
	}

	// deleting the earlier snapshot must leave the orphan withheld by the later
	// snapshot
	if err := store.DeleteSnapshot(earlier.ID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("unexpected", orphans)
	}

	// deleting the later snapshot releases the orphan
	if err := store.DeleteSnapshot(adopted.ID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}
}
