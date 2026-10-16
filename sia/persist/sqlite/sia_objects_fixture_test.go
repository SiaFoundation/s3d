package sqlite

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

// TestStoredSealedObjectFixture replays a sealed object the native side
// produced, captured from a siastorage_mock run. It needs no cgo itself, so
// ordinary CI covers the two crossings the store depends on.
//
// The sealed form reaches Go as JSON, which matches on field name, so a rename
// on either side lands as a zero rather than an error. Checking the fixture
// populates what the schema has columns for catches that, and the round trip
// catches a field the store has nowhere to put.
func TestStoredSealedObjectFixture(t *testing.T) {
	buf, err := os.ReadFile(filepath.Join("testdata", "sealed_object.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sealed sdk.SealedObject
	if err := json.Unmarshal(buf, &sealed); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	// A renamed field decodes as a zero, so each one the store persists is
	// checked rather than assumed.
	if len(sealed.EncryptedDataKey) == 0 {
		t.Error("encryptedDataKey is empty")
	}
	if sealed.DataSignature == (types.Signature{}) {
		t.Error("dataSignature is zero")
	}
	if sealed.CreatedAt.IsZero() {
		t.Error("createdAt is zero")
	}
	if sealed.UpdatedAt.IsZero() {
		t.Error("updatedAt is zero")
	}
	if len(sealed.Slabs) == 0 {
		t.Fatal("slabs is empty")
	}
	for i, ss := range sealed.Slabs {
		if ss.MinShards == 0 {
			t.Errorf("slab %d: minShards is zero", i)
		}
		if ss.Length == 0 {
			t.Errorf("slab %d: length is zero", i)
		}
		if ss.EncryptionKey == (slabs.EncryptionKey{}) {
			t.Errorf("slab %d: encryptionKey is zero", i)
		}
		if len(ss.Sectors) == 0 {
			t.Errorf("slab %d: no sectors", i)
			continue
		}
		for j, sector := range ss.Sectors {
			if sector.Root == (types.Hash256{}) {
				t.Errorf("slab %d sector %d: root is zero", i, j)
			}
			if sector.HostKey == (types.PublicKey{}) {
				t.Errorf("slab %d sector %d: hostKey is zero", i, j)
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	store := initTestDB(t, zap.NewNop())
	var got sdk.SealedObject
	if err := store.transaction(func(tx *txn) error {
		if err := upsertSiaObject(tx, sealed); err != nil {
			return err
		}
		var err error
		got, err = siaObject(tx, sealed.ID())
		return err
	}); err != nil {
		t.Fatal(err)
	}

	want, err := sealed.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}
	gotBlob, err := got.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(gotBlob) {
		t.Fatal("sealed object did not survive the store")
	}
}
