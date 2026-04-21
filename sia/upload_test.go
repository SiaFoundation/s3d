package sia

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestUploadGroup(t *testing.T) {
	p := uploadGroup{
		slabSize:       100,
		maxGroupSize:   600,
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

	// start a fresh group to test last slab filling and max group size
	p = uploadGroup{
		slabSize:       100,
		maxGroupSize:   600,
		uploadWastePct: 0.1,
	}

	// add an object that creates waste in the last slab
	// 550 bytes = 6 slabs, 50 bytes wasted, waste = 50/600 = 8.3%
	if !p.tryAdd(objects.ObjectForUpload{Length: 550, Name: "e"}) {
		t.Fatal("expected tryAdd to succeed")
	}
	if p.remainingSpace() != 50 {
		t.Fatalf("expected 50 remaining, got %d", p.remainingSpace())
	}

	// fill the remaining 50 bytes in the last slab
	if !p.tryAdd(objects.ObjectForUpload{Length: 50, Name: "f"}) {
		t.Fatal("expected tryAdd to succeed")
	}
	if p.wastePct() != 0 {
		t.Fatalf("expected 0 waste, got %f", p.wastePct())
	}

	// adding anything more exceeds maxGroupSize
	if p.tryAdd(objects.ObjectForUpload{Length: 1, Name: "g"}) {
		t.Fatal("expected tryAdd to fail due to max group size")
	}

	// start a fresh group with an object that exceeds maxGroupSize
	// 810 bytes with maxGroupSize=600, 90 bytes wasted in the last slab
	p = uploadGroup{
		slabSize:       100,
		maxGroupSize:   600,
		uploadWastePct: 0.1,

		objects:   []objects.ObjectForUpload{{Length: 810, Name: "h"}},
		totalSize: 810,
	}

	// small objects that fit in the last slab should be accepted
	if !p.tryAdd(objects.ObjectForUpload{Length: 40, Name: "i"}) {
		t.Fatal("expected tryAdd to succeed for object fitting last slab")
	}

	// object that does not fit in remaining space should not be accepted, even if it reduces waste
	if p.tryAdd(objects.ObjectForUpload{Length: 140, Name: "k"}) {
		t.Fatal("expected tryAdd to fail for object not fitting last slab even if it reduces waste")
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

// TestOpenAndRemoveUpload tests the locking mechanism that defers file
// deletion until all locks are released.
func TestOpenAndRemoveUpload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not allow deleting open files")
	}

	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, UploadsDirectory)
	if err := os.MkdirAll(uploadsDir, 0700); err != nil {
		t.Fatal(err)
	}

	s := &Sia{directory: dir, lockedUploads: make(map[string]*lockedUpload)}

	// write random data to an upload file
	data := frand.Bytes(256)
	fileName := "test-upload"
	filePath := filepath.Join(uploadsDir, fileName)
	if err := os.WriteFile(filePath, data, 0600); err != nil {
		t.Fatal(err)
	}

	// acquire a lock on the upload
	unlock := s.lockUpload(fileName)

	// removeUpload while the lock is held should mark deleted but not
	// remove the file from disk
	if err := s.removeUpload(fileName); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatal("file should still exist on disk while lock is held:", err)
	}

	// acquiring a second lock should also work
	unlock2 := s.lockUpload(fileName)

	// releasing the first lock should not remove the file yet because the
	// second lock is still held
	unlock()
	if _, err := os.Stat(filePath); err != nil {
		t.Fatal("file should still exist on disk while second lock is held:", err)
	}

	// releasing the second lock should trigger the deferred removal
	unlock2()
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatal("file should have been removed after all locks were released")
	}

	// verify the lockedUploads map is cleaned up
	s.lockedUploadsMu.Lock()
	if _, ok := s.lockedUploads[fileName]; ok {
		t.Fatal("lockedUploads entry should have been cleaned up")
	}
	s.lockedUploadsMu.Unlock()

	// removing a file without any lock should delete it immediately
	fileName2 := "test-upload-2"
	filePath2 := filepath.Join(uploadsDir, fileName2)
	if err := os.WriteFile(filePath2, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.removeUpload(fileName2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filePath2); !os.IsNotExist(err) {
		t.Fatal("file should have been removed immediately when no lock is held")
	}
}
