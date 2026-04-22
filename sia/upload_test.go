package sia

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestUploadGroup(t *testing.T) {
	p := uploadGroup{
		slabSize:       100,
		uploadWastePct: 0.1,
	}

	// objects are accepted freely while waste is above threshold
	if !p.tryAdd(objects.ObjectForUpload{Length: 92, Name: "a"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// after threshold is met, objects that fit in the last slab are accepted
	if !p.tryAdd(objects.ObjectForUpload{Length: 5, Name: "b"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// objects that don't fit and don't reduce waste are rejected
	if p.tryAdd(objects.ObjectForUpload{Length: 50, Name: "c"}) {
		t.Fatal("expected tryAdd to fail")
	}

	// objects that don't fit but reduce waste are accepted
	if !p.tryAdd(objects.ObjectForUpload{Length: 103, Name: "d"}) {
		t.Fatal("expected tryAdd to succeed")
	}
	if p.wastePct() != 0 {
		t.Fatalf("expected 0 waste, got %f", p.wastePct())
	}
}

// uploadStore is a minimal Store stub for testing prepareUploads.
type uploadStore struct {
	Store
	objects []objects.ObjectForUpload
}

func (s *uploadStore) ObjectsForUpload() ([]objects.ObjectForUpload, error) {
	return s.objects, nil
}

func TestPrepareUploads(t *testing.T) {
	// "a"(92) meets the 10% waste threshold on its own, then "b"(108)
	// is accepted because it reduces waste to 0% by filling two slabs
	// exactly, "c"(42) cannot reduce waste further and remains pending
	store := &uploadStore{
		objects: []objects.ObjectForUpload{
			{Name: "a", Length: 92},
			{Name: "b", Length: 108},
			{Name: "c", Length: 42},
		},
	}
	s := Sia{
		store:          store,
		slabSize:       100,
		uploadWastePct: 0.10,
		logger:         zaptest.NewLogger(t),
	}

	ready := s.prepareUploads()

	if len(ready) != 1 {
		t.Fatalf("expected 1 ready group, got %d", len(ready))
	}
	if ready[0].totalSize != 200 {
		t.Fatalf("expected totalSize 200, got %d", ready[0].totalSize)
	}
	if len(ready[0].objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(ready[0].objects))
	}
}

// TestOpenAndRemoveUpload tests that an upload file can be removed while it is
// still open, and the open handle can still be read from and matches the
// original data.
func TestOpenAndRemoveUpload(t *testing.T) {
	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, UploadsDirectory)
	if err := os.MkdirAll(uploadsDir, 0700); err != nil {
		t.Fatal(err)
	}

	s := &Sia{directory: dir}

	// write random data to an upload file
	data := frand.Bytes(256)
	fileName := "test-upload"
	if err := os.WriteFile(filepath.Join(uploadsDir, fileName), data, 0600); err != nil {
		t.Fatal(err)
	}

	// open the upload
	rc, err := s.openUpload("bucket", "name", &fileName, false, 0)
	if err != nil {
		t.Fatal(err)
	}

	// remove the upload while the file handle is still open
	if err := s.removeUpload(fileName); err != nil {
		t.Fatal(err)
	}

	// read from the still open handle and compare
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()

	if !bytes.Equal(got, data) {
		t.Fatal("data read from open handle does not match original")
	}
}
