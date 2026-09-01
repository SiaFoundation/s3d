package sqlite

import (
	"errors"
	"slices"
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
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "a", objects.PutOptions{ContentMD5: md5, Length: 1, FileName: new(string)}); err != nil {
		t.Fatal(err)
	} else if err := store.MarkObjectUploaded(bucket, "a", "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(objID); err != nil {
		t.Fatal(err)
	}

	// a pending object with only an on-disk file
	pending := "pending-file"
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "pending", objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 5, FileName: &pending}); err != nil {
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

	// an unknown object is not referenced by any snapshot
	if known, err := store.HasSnapshotObject(frand.Entropy256()); err != nil {
		t.Fatal(err)
	} else if known {
		t.Fatal("unexpected known object")
	}

	// a snapshot is listed once it is marked pinned
	var siaObjectID types.Hash256
	frand.Read(siaObjectID[:])
	if err := store.MarkSnapshotPinning(s1.ID, siaObjectID); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinned(siaObjectID); err != nil {
		t.Fatal(err)
	}

	// the recorded object is known
	if known, err := store.HasSnapshotObject(siaObjectID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected known object")
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

	// completing an unknown object reports not found, as does completing an
	// already completed snapshot
	if err := store.MarkSnapshotPinned(frand.Entropy256()); !errors.Is(err, objects.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	} else if err := store.MarkSnapshotPinned(siaObjectID); !errors.Is(err, objects.ErrSnapshotNotFound) {
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

	// the un-uploaded second snapshot is excluded from the list
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 {
		t.Fatal("unexpected", len(snapshots))
	}

	// rolling back the later snapshot leaves the object withheld by the
	// earlier one
	if err := store.RollbackSnapshot(s2.ID); err != nil {
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

	addObject := func(key string) types.Hash256 {
		t.Helper()
		obj := newTestObject()
		sealed := obj.Seal(types.GeneratePrivateKey())
		md5 := frand.Entropy128()
		if _, _, err := store.PutObject(testAccessKeyID, bucket, key, objects.PutOptions{ContentMD5: md5, Length: 1, FileName: new(string)}); err != nil {
			t.Fatal(err)
		} else if err := store.MarkObjectUploaded(bucket, key, "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		} else if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
			t.Fatal(err)
		}
		return sealed.ID()
	}

	// an object that predates the third snapshot, so its backup holds it
	aID := addObject("a2")

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
	if _, err := store.DeleteSnapshotsBySiaObject(adopted.SiaObjectID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}

	// objects deleted while the third snapshot's upload is in flight are
	// withheld, the one that predates it and the one created during it may
	// both be captured in the backup
	bID := addObject("b")
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
		t.Fatal(err)
	} else if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a2"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatal("unexpected", orphans)
	}

	// completing the third snapshot keeps it withheld, it existed before the
	// backup finished
	snap3ObjID := frand.Entropy256()
	if err := store.MarkSnapshotPinning(snap3.ID, snap3ObjID); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinned(snap3ObjID); err != nil {
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
	snap4ObjID := frand.Entropy256()
	if snap4, _, err := store.CreateSnapshot(); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinning(snap4.ID, snap4ObjID); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinned(snap4ObjID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CopyObject(testAccessKeyID, bucket, "d", s3.NoVersion(), bucket, "d-copy", s3.CopyObjectOptions{}); err != nil {
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

	// a metadata sync re-seals an object without moving its creation stamp, so
	// an object the completed backup references stays withheld
	fObj := newTestObject()
	fSealed := fObj.Seal(types.GeneratePrivateKey())
	fMD5 := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "f", objects.PutOptions{ContentMD5: fMD5, Length: 1, FileName: new(string)}); err != nil {
		t.Fatal(err)
	} else if err := store.MarkObjectUploaded(bucket, "f", "", fMD5, fSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(fSealed.ID()); err != nil {
		t.Fatal(err)
	}
	snap5ObjID := frand.Entropy256()
	if snap5, _, err := store.CreateSnapshot(); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinning(snap5.ID, snap5ObjID); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinned(snap5ObjID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateSiaObjects([]objects.SiaObject{{ID: fSealed.ID(), Sealed: fSealed}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "f"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("a re-sealed object the backup references was released", orphans)
	}

	// deleting every snapshot releases the remaining orphans
	if _, err := store.DeleteSnapshotsBySiaObject(snap3ObjID); err != nil {
		t.Fatal(err)
	} else if _, err := store.DeleteSnapshotsBySiaObject(snap4ObjID); err != nil {
		t.Fatal(err)
	} else if _, err := store.DeleteSnapshotsBySiaObject(adopted2.SiaObjectID); err != nil {
		t.Fatal(err)
	} else if _, err := store.DeleteSnapshotsBySiaObject(snap5ObjID); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.OrphanedObjects(100); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 4 {
		t.Fatal("unexpected", orphans)
	} else if !slices.Contains(orphans, aID) || !slices.Contains(orphans, bID) ||
		!slices.Contains(orphans, dID) || !slices.Contains(orphans, fSealed.ID()) {
		t.Fatal("unexpected", orphans)
	}

	// a rollback deletes a snapshot whose backup was never uploaded
	rb1, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RollbackSnapshot(rb1.ID); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "snapshots")

	// a rollback marks a snapshot awaiting its pin for deletion
	rb2, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	rb2ObjID := frand.Entropy256()
	if err := store.MarkSnapshotPinning(rb2.ID, rb2ObjID); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSnapshotPinning(rb2.ID, rb2ObjID); !errors.Is(err, objects.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}
	if err := store.RollbackSnapshot(rb2.ID); err != nil {
		t.Fatal(err)
	}
	if known, err := store.HasSnapshotObject(rb2ObjID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected known object")
	}
	if snapshots, err := store.SnapshotsForDeletion(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 1 || snapshots[0].ObjectID != rb2ObjID {
		t.Fatal("unexpected", snapshots)
	} else if snapshots[0].Since.IsZero() {
		t.Fatal("expected non-zero deleting since")
	}

	// a late pin observation does not complete a snapshot marked for deletion
	if err := store.MarkSnapshotPinned(rb2ObjID); !errors.Is(err, objects.ErrSnapshotNotFound) {
		t.Fatal("unexpected", err)
	}
	if snapshots, err := store.ListSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 0 {
		t.Fatal("unexpected", len(snapshots))
	}

	// confirming the deletion removes the record
	if n, err := store.DeleteSnapshotsBySiaObject(rb2ObjID); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}

	// startup deletes created snapshots and marks the ones awaiting a pin stale
	if _, _, err := store.CreateSnapshot(); err != nil {
		t.Fatal(err)
	}
	rb3, _, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	rb3ObjID := frand.Entropy256()
	if err := store.MarkSnapshotPinning(rb3.ID, rb3ObjID); err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.RollbackIncompleteSnapshots(); err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatal("unexpected", deleted)
	}

	// the snapshot awaiting its pin survives the rollback and is reported for
	// reconciling against the indexer
	if pinning, err := store.PinningSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(pinning) != 1 {
		t.Fatal("unexpected", pinning)
	} else if pinning[0].ID != rb3.ID || pinning[0].ObjectID != rb3ObjID {
		t.Fatal("mismatch", pinning[0])
	}

	// removing it leaves nothing to reconcile
	if err := store.DeleteSnapshot(rb3.ID); err != nil {
		t.Fatal(err)
	} else if pinning, err := store.PinningSnapshots(); err != nil {
		t.Fatal(err)
	} else if len(pinning) != 0 {
		t.Fatal("unexpected", pinning)
	}
}
