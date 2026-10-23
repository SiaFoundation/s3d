//go:build siastorage_mock

package sia_test

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// The rest of the sia tests run against testutil.MemorySDK, which fakes the
// SDK entirely. These drive the real one, so they are the only coverage of
// [sia.IndexdSDK] itself: the JSON the native side seals objects into, the
// objects it hands back, and the transfers.
//
// They need an archive built with the mock cargo feature, which swaps the host
// transport for an in-memory one and is never committed. Build it with
// `make testlib` in sia-storage-go, then run these with
// `go test -tags siastorage_mock ./sia`.

// mockHosts is high enough to cover the default erasure coding with room for
// the SDK to pick among hosts.
const mockHosts = 40

// mockSDK brings up in-process hosts and the adapter s3d drives them through.
func mockSDK(t *testing.T) *sia.IndexdSDK {
	t.Helper()

	net := sdk.NewMockNetwork(mockHosts)

	client, err := net.SDK(t.Context(), frand.Entropy256())
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return sia.NewSDK(client)
}

// uploadObject packs data into a single object and returns it.
func uploadObject(t *testing.T, s *sia.IndexdSDK, data []byte) *sdk.Object {
	t.Helper()

	upload, err := s.UploadPacked(t.Context())
	if err != nil {
		t.Fatalf("packed upload: %v", err)
	}
	defer upload.Close()

	n, err := upload.Add(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("add: %v", err)
	} else if n != int64(len(data)) {
		t.Fatalf("added %d bytes of %d", n, len(data))
	}

	objs, err := upload.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	} else if len(objs) != 1 {
		t.Fatalf("finalize returned %d objects for one add", len(objs))
	}
	return objs[0]
}

// assertSealed fails on any field that came back zero, which is what a field
// the two sides spell differently looks like after the decode.
func assertSealed(t *testing.T, sealed sdk.SealedObject) {
	t.Helper()

	if len(sealed.EncryptedDataKey) == 0 {
		t.Fatal("encryptedDataKey is empty")
	}
	if sealed.DataSignature == (types.Signature{}) {
		t.Fatal("dataSignature is zero")
	}
	if sealed.CreatedAt.IsZero() {
		t.Fatal("createdAt is zero")
	}
	if sealed.UpdatedAt.IsZero() {
		t.Fatal("updatedAt is zero")
	}
	if len(sealed.Slabs) == 0 {
		t.Fatal("the object has no slab slices")
	}
	for i, ss := range sealed.Slabs {
		if ss.EncryptionKey == (slabs.EncryptionKey{}) {
			t.Fatalf("slice %d has a zero encryption key", i)
		}
		if ss.MinShards == 0 {
			t.Fatalf("slice %d has zero minShards", i)
		}
		if ss.Length == 0 {
			t.Fatalf("slice %d has zero length", i)
		}
		if len(ss.Sectors) == 0 {
			t.Fatalf("slice %d has no sectors", i)
		}
		for j, sec := range ss.Sectors {
			if sec.Root == (types.Hash256{}) {
				t.Fatalf("slice %d sector %d has a zero root", i, j)
			}
			if sec.HostKey == (types.PublicKey{}) {
				t.Fatalf("slice %d sector %d has a zero host key", i, j)
			}
		}
	}
}

// TestIndexdSDKObjectRoundTrip walks the path an object takes through s3d:
// packed upload, seal into the struct the store persists, unseal back into an
// object, pin, and download whole and by range.
func TestIndexdSDKObjectRoundTrip(t *testing.T) {
	s := mockSDK(t)
	ctx := t.Context()

	if _, err := s.Account(ctx); err != nil {
		t.Fatalf("account: %v", err)
	}
	if size, err := s.OptimalDataSize(ctx); err != nil {
		t.Fatalf("optimal data size: %v", err)
	} else if size <= 0 {
		t.Fatalf("optimal data size is %d", size)
	}

	data := frand.Bytes(2 << 20)
	obj := uploadObject(t, s, data)

	sealed, err := s.SealObject(obj)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	assertSealed(t, sealed)
	// the ID is a digest of the slices, so it only agrees if the slices
	// survived the crossing intact
	if sealed.ID() != obj.ID() {
		t.Fatalf("sealed ID %v, object ID %v", sealed.ID(), obj.ID())
	}

	reopened, err := s.UnsealObject(sealed)
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if reopened.ID() != obj.ID() {
		t.Fatalf("reopened ID %v, object ID %v", reopened.ID(), obj.ID())
	}
	if reopened.Size() != uint64(len(data)) {
		t.Fatalf("reopened object is %d bytes, uploaded %d", reopened.Size(), len(data))
	}

	if err := s.PinObject(ctx, reopened); err != nil {
		t.Fatalf("pin: %v", err)
	}

	rc, err := s.Download(ctx, reopened, nil)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the object did not round trip")
	}

	// a ranged read is the path a GET with a Range header takes
	rnge := &s3.ObjectRange{Start: 1 << 10, Length: 4 << 10}
	rc, err = s.Download(ctx, reopened, rnge)
	if err != nil {
		t.Fatalf("ranged download: %v", err)
	}
	got, err = io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ranged read: %v", err)
	}
	if want := data[rnge.Start : rnge.Start+rnge.Length]; !bytes.Equal(got, want) {
		t.Fatalf("ranged read returned %d bytes, wanted %d", len(got), len(want))
	}
}

// TestIndexdSDKSealedObjectSurvivesStore is the crossing the port is most
// exposed to: the store decomposes a sealed object into its own tables, so a
// field the native side fills but the schema has no column for is lost
// silently, and only shows up as an object that no longer opens.
func TestIndexdSDKSealedObjectSurvivesStore(t *testing.T) {
	const (
		bucket = "bucket"
		name   = "obj"
	)

	s := mockSDK(t)
	ctx := t.Context()
	log := zaptest.NewLogger(t)

	store, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testutil.AccessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}

	data := frand.Bytes(1 << 20)
	obj := uploadObject(t, s, data)

	sealed, err := s.SealObject(obj)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	contentMD5 := frand.Entropy128()
	fn := name + ".upload"
	if _, _, err := store.PutObject(testutil.AccessKeyID, bucket, name, objects.PutOptions{
		ContentMD5: contentMD5,
		Length:     int64(len(data)),
		FileName:   &fn,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, name, "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	stored, err := store.GetObject(aws.String(testutil.AccessKeyID), bucket, name, s3.NoVersion(), nil, s3.ActionGetObject)
	if err != nil {
		t.Fatal(err)
	} else if stored.SiaObject == nil {
		t.Fatal("the stored object has no sia object")
	}
	assertSealed(t, stored.SiaObject.Sealed)
	if stored.SiaObject.ID != sealed.ID() {
		t.Fatalf("stored ID %v, sealed ID %v", stored.SiaObject.ID, sealed.ID())
	}

	// what the store gives back has to still open, which is the test that the
	// columns cover everything the signatures are taken over
	reopened, err := s.UnsealObject(stored.SiaObject.Sealed)
	if err != nil {
		t.Fatalf("unseal after store: %v", err)
	}

	if err := s.PinObject(ctx, reopened); err != nil {
		t.Fatalf("pin: %v", err)
	}

	rc, err := s.Download(ctx, reopened, nil)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the object did not round trip through the store")
	}
}

// TestIndexdSDKObjectEvents covers the cursor translation: s3d persists a
// [slabs.Cursor], the SDK wants its own, and a zero one has to mean "from the
// beginning" rather than "after the zero time".
func TestIndexdSDKObjectEvents(t *testing.T) {
	s := mockSDK(t)
	ctx := t.Context()

	obj := uploadObject(t, s, frand.Bytes(1<<20))

	sealed, err := s.SealObject(obj)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := s.PinObject(ctx, obj); err != nil {
		t.Fatalf("pin: %v", err)
	}

	events, err := s.ObjectEvents(ctx, slabs.Cursor{}, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("a zero cursor returned no events, so it was not read as the beginning")
	}

	var found *sdk.ObjectEvent
	for i := range events {
		if events[i].ID == sealed.ID() {
			found = &events[i]
		}
	}
	if found == nil {
		t.Fatalf("the pinned object is absent from %d events", len(events))
	}
	if found.Deleted {
		t.Fatal("a write was reported as a deletion")
	}
	if found.Object == nil {
		t.Fatal("a write event carried no object")
	}
	if found.UpdatedAt.IsZero() {
		t.Fatal("updatedAt is zero")
	}

	// resuming from the last event is how syncMetadata advances; it must not
	// hand back what it just consumed
	last := events[len(events)-1]
	rest, err := s.ObjectEvents(ctx, slabs.Cursor{After: last.UpdatedAt, Key: last.ID}, 100)
	if err != nil {
		t.Fatalf("events after cursor: %v", err)
	}
	for _, ev := range rest {
		if ev.ID == last.ID && ev.UpdatedAt.Equal(last.UpdatedAt) {
			t.Fatal("the cursor returned the event it was built from")
		}
	}
}

// TestIndexdSDKUpload drives the streaming upload CreateSnapshot takes, which
// the packed path does not cover. It is also the only caller that sets
// metadata on the object it uploads, so this is the one test where the sealed
// form carries encrypted metadata and it has to survive the seal to be
// readable when a snapshot is restored.
func TestIndexdSDKUpload(t *testing.T) {
	s := mockSDK(t)
	ctx := t.Context()

	meta := []byte(`{"bucket":"bucket","name":"obj"}`)
	data := frand.Bytes(2 << 20)

	obj, err := s.Upload(ctx, sdk.NewEmptyObject().WithMetadata(meta), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if got := obj.Metadata(); !bytes.Equal(got, meta) {
		t.Fatalf("uploaded object carries metadata %s, wanted %s", got, meta)
	}
	if obj.Size() != uint64(len(data)) {
		t.Fatalf("uploaded object is %d bytes, wrote %d", obj.Size(), len(data))
	}

	sealed, err := s.SealObject(obj)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	assertSealed(t, sealed)
	if len(sealed.EncryptedMetadata) == 0 {
		t.Fatal("encryptedMetadata is empty")
	}
	if len(sealed.EncryptedMetadataKey) == 0 {
		t.Fatal("encryptedMetadataKey is empty")
	}
	if sealed.ID() != obj.ID() {
		t.Fatalf("sealed ID %v, object ID %v", sealed.ID(), obj.ID())
	}

	reopened, err := s.UnsealObject(sealed)
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if got := reopened.Metadata(); !bytes.Equal(got, meta) {
		t.Fatalf("metadata did not survive the seal: %s", got)
	}

	if err := s.PinObject(ctx, reopened); err != nil {
		t.Fatalf("pin: %v", err)
	}

	rc, err := s.Download(ctx, reopened, nil)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the object did not round trip")
	}
}

// TestIndexdSDKDeleteObject covers the delete the orphan loop performs, and
// the event the metadata sync reads it back as.
func TestIndexdSDKDeleteObject(t *testing.T) {
	s := mockSDK(t)
	ctx := t.Context()

	obj := uploadObject(t, s, frand.Bytes(1<<20))
	if err := s.PinObject(ctx, obj); err != nil {
		t.Fatalf("pin: %v", err)
	}
	id := obj.ID()

	if err := s.DeleteObject(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events, err := s.ObjectEvents(ctx, slabs.Cursor{}, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var found *sdk.ObjectEvent
	for i := range events {
		if events[i].ID == id {
			found = &events[i]
		}
	}
	if found == nil {
		t.Fatalf("the deleted object is absent from %d events", len(events))
	}
	if !found.Deleted {
		t.Fatal("the deletion was not reported as one")
	}
	// syncBatch takes the deleted branch before it looks at the object, and a
	// deletion carries none
	if found.Object != nil {
		t.Fatal("a deletion carried an object")
	}
}

// TestIndexdSDKPruneSlabs covers what the prune loop is for: deleting an
// object leaves its slabs pinned, and only a prune releases the storage.
func TestIndexdSDKPruneSlabs(t *testing.T) {
	s := mockSDK(t)
	ctx := t.Context()

	obj := uploadObject(t, s, frand.Bytes(1<<20))
	if err := s.PinObject(ctx, obj); err != nil {
		t.Fatalf("pin: %v", err)
	}

	pinned, err := s.Account(ctx)
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	if pinned.PinnedSize == 0 {
		t.Fatal("the pinned object is not counted against the account")
	}

	if err := s.DeleteObject(ctx, obj.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	deleted, err := s.Account(ctx)
	if err != nil {
		t.Fatalf("account after delete: %v", err)
	}
	if deleted.PinnedSize != pinned.PinnedSize {
		t.Fatalf("deleting released storage on its own: %d, was %d", deleted.PinnedSize, pinned.PinnedSize)
	}

	if err := s.PruneSlabs(ctx, time.Now()); err != nil {
		t.Fatalf("prune: %v", err)
	}
	pruned, err := s.Account(ctx)
	if err != nil {
		t.Fatalf("account after prune: %v", err)
	}
	if pruned.PinnedSize != 0 {
		t.Fatalf("pinned size is %d after pruning, was %d", pruned.PinnedSize, pinned.PinnedSize)
	}
}
