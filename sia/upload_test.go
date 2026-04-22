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
	const (
		slabSize       = 256
		uploadWastePct = 0.1
	)
	newGroup := func() uploadGroup {
		return uploadGroup{
			slabSize:       slabSize,
			uploadWastePct: uploadWastePct,
		}
	}
	p := newGroup()

	// assert remaining space on empty group is a full slab
	if p.remainingSpace() != slabSize {
		t.Fatalf("expected %d, got %d", slabSize, p.remainingSpace())
	}

	// assert 100% waste on empty group
	if p.wastePct() != 1 {
		t.Fatalf("expected 1, got %f", p.wastePct())
	}

	// assert tryAdd succeeds when waste is high
	if !p.tryAdd(objects.ObjectForUpload{Length: 100, Name: "a"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert remaining space decreased
	if p.remainingSpace() != 156 {
		t.Fatalf("expected 156, got %d", p.remainingSpace())
	}

	// assert filling to exact slab boundary succeeds
	if !p.tryAdd(objects.ObjectForUpload{Length: 156, Name: "b"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert zero waste and zero remaining space on exact slab boundary
	if p.wastePct() != 0 {
		t.Fatalf("expected 0, got %f", p.wastePct())
	}
	if p.remainingSpace() != 0 {
		t.Fatalf("expected 0, got %d", p.remainingSpace())
	}

	// assert any object is rejected when remaining space is zero
	if p.tryAdd(objects.ObjectForUpload{Length: 1}) {
		t.Fatal("expected tryAdd to fail at slab boundary")
	}

	// assert slab spanning object is rejected when waste is low
	if p.tryAdd(objects.ObjectForUpload{Length: slabSize + 1}) {
		t.Fatal("expected tryAdd to fail")
	}

	// start a new group with space left in the slab
	p2 := newGroup()
	if !p2.tryAdd(objects.ObjectForUpload{Length: 200, Name: "x"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert object fitting in remaining space is accepted
	if !p2.tryAdd(objects.ObjectForUpload{Length: 50, Name: "y"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert object exceeding remaining space is rejected when waste is low
	if p2.tryAdd(objects.ObjectForUpload{Length: p2.remainingSpace() + 1}) {
		t.Fatal("expected tryAdd to fail")
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
	// four objects sized so FFD puts 92+8 in one group and 85+42 in another,
	// only the first group meets 10% waste, then gap fill reclaims 85 but
	// not 42
	store := &uploadStore{
		objects: []objects.ObjectForUpload{
			{Name: "a", Length: 92},
			{Name: "b", Length: 85},
			{Name: "c", Length: 42},
			{Name: "d", Length: 8},
		},
	}
	s := Sia{
		store:          store,
		slabSize:       100,
		uploadWastePct: 0.10,
		logger:         zaptest.NewLogger(t),
	}

	ready := s.prepareUploads()

	// assert single ready group with the three objects that fit
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready group, got %d", len(ready))
	}
	if ready[0].totalSize != 185 {
		t.Fatalf("expected totalSize 185, got %d", ready[0].totalSize)
	}
	if len(ready[0].objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(ready[0].objects))
	}

	// assert the 42 byte object was excluded
	for _, obj := range ready[0].objects {
		if obj.Name == "c" {
			t.Fatal("42 byte object should have been excluded")
		}
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
