package sia_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// TestSnapshotOrphanLifecycle verifies that a snapshot withholds a deleted
// object from the orphan loop until the snapshot is removed and the generation
// floor moves past the object.
func TestSnapshotOrphanLifecycle(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	backend, store := testutil.NewBackend(t, testutil.WithSDK(memSDK))

	const bucket = "bucket"
	if err := store.CreateBucket(testutil.AccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// upload an object to the SDK and record it as uploaded in the store
	upload := func(name string) types.Hash256 {
		t.Helper()
		data := frand.Bytes(16)
		siaObj, err := memSDK.Upload(t.Context(), bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		sealed := memSDK.SealObject(siaObj)
		fn := name + ".upload"
		md5 := frand.Entropy128()
		if _, _, err := store.PutObject(testutil.AccessKeyID, bucket, name, md5, nil, int64(len(data)), &fn); err != nil {
			t.Fatal(err)
		} else if err := store.MarkObjectUploaded(bucket, name, md5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		return sealed.ID()
	}

	// pin two objects so both are live on the network
	idA := upload("a")
	idB := upload("b")
	if err := backend.PinObjects(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := memSDK.ObjectCount(); got != 2 {
		t.Fatal("expected 2 pinned objects, got", got)
	}

	// snapshot S1 captures both objects, then delete A
	s1Path := filepath.Join(t.TempDir(), "s1.sqlite")
	if err := backend.BackupSQLite3(t.Context(), s1Path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.DeleteObject(testutil.AccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	// the orphan loop must not unpin A while S1 references it
	backend.ProcessOrphans(t.Context())
	if got := memSDK.ObjectCount(); got != 2 {
		t.Fatal("expected A still pinned while snapshotted, got", got)
	} else if !memSDK.Pinned(idA) {
		t.Fatal("expected A still pinned")
	}

	// snapshot S2 is taken after A was deleted, then delete B
	s2Path := filepath.Join(t.TempDir(), "s2.sqlite")
	if err := backend.BackupSQLite3(t.Context(), s2Path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.DeleteObject(testutil.AccessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
		t.Fatal(err)
	}

	// both orphans are withheld while their snapshots survive
	backend.ProcessOrphans(t.Context())
	if got := memSDK.ObjectCount(); got != 2 {
		t.Fatal("expected both objects still pinned while snapshotted, got", got)
	}

	snapshots, err := store.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	} else if len(snapshots) != 2 {
		t.Fatal("expected 2 snapshots, got", len(snapshots))
	}

	// deleting S1 lowers the floor past A's generation, so the orphan loop
	// unpins A while B remains withheld by the newer S2
	if err := store.DeleteSnapshot(snapshots[0].ID); err != nil {
		t.Fatal(err)
	}
	backend.ProcessOrphans(t.Context())
	if got := memSDK.ObjectCount(); got != 1 {
		t.Fatal("expected only B pinned after S1 deleted, got", got)
	} else if memSDK.Pinned(idA) {
		t.Fatal("expected A unpinned after its snapshot was deleted")
	} else if !memSDK.Pinned(idB) {
		t.Fatal("expected B still pinned while S2 survives")
	}

	// deleting S2 releases B as well
	if err := store.DeleteSnapshot(snapshots[1].ID); err != nil {
		t.Fatal(err)
	}
	backend.ProcessOrphans(t.Context())
	if got := memSDK.ObjectCount(); got != 0 {
		t.Fatal("expected no pinned objects after all snapshots deleted, got", got)
	} else if memSDK.Pinned(idB) {
		t.Fatal("expected B unpinned after its snapshot was deleted")
	}
}
