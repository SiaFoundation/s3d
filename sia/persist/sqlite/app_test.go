package sqlite

import (
	"bytes"
	"errors"
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

	if _, _, err := store.AppKey(); !errors.Is(err, ErrNoAppKey) {
		t.Fatal(err)
	}

	key := types.GeneratePrivateKey()
	const indexerURL = "https://indexer.example"
	if err := store.SetAppKey(key, indexerURL); err != nil {
		t.Fatal(err)
	} else if retrieved, gotURL, err := store.AppKey(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(retrieved, key) {
		t.Fatalf("expected key %x, got %x", key, retrieved)
	} else if gotURL != indexerURL {
		t.Fatalf("expected indexer URL %q, got %q", indexerURL, gotURL)
	}
}
