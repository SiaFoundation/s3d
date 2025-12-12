package sqlite

import (
	"bytes"
	"path/filepath"
	"testing"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func TestAppKey(t *testing.T) {
	store, err := OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := types.GeneratePrivateKey()
	if err := store.SetAppKey(key); err != nil {
		t.Fatal(err)
	} else if retrieved, err := store.AppKey(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(retrieved, key) {
		t.Fatalf("expected key %x, got %x", key, retrieved)
	}
}
