package sia

import (
	"bytes"
	"fmt"
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

func TestTryAddComparison(t *testing.T) {
	const (
		slabSize       = 40 << 20 // 40 MiB
		uploadWastePct = 0.10
		numObjects     = 200
	)

	// generate a fixed set of random objects with sizes between 1 and 50 MiB
	rng := frand.NewCustom(make([]byte, 32), 1024, 20)
	var candidates []objects.ObjectForUpload
	for i := range numObjects {
		size := int64(1 + rng.Intn(50<<20))
		candidates = append(candidates, objects.ObjectForUpload{
			Name:   fmt.Sprintf("obj-%d", i),
			Length: size,
		})
	}

	type tryAddFunc func(p *uploadGroup, obj objects.ObjectForUpload) bool

	runGrouping := func(tryAdd tryAddFunc) []uploadGroup {
		newGroup := func(obj objects.ObjectForUpload) uploadGroup {
			return uploadGroup{
				slabSize:       slabSize,
				uploadWastePct: uploadWastePct,
				objects:        []objects.ObjectForUpload{obj},
				totalSize:      obj.Length,
			}
		}

		var groups []uploadGroup
		for _, obj := range candidates {
			var added bool
			for i := range groups {
				added = tryAdd(&groups[i], obj)
				if added {
					break
				}
			}
			if !added {
				groups = append(groups, newGroup(obj))
			}
		}

		var ready []uploadGroup
		var remaining []objects.ObjectForUpload
		for _, g := range groups {
			if g.wastePct() < uploadWastePct {
				ready = append(ready, g)
			} else {
				remaining = append(remaining, g.objects...)
			}
		}

		for _, obj := range remaining {
			for i := range ready {
				if tryAdd(&ready[i], obj) {
					break
				}
			}
		}
		return ready
	}

	oldGroups := runGrouping(func(p *uploadGroup, obj objects.ObjectForUpload) bool {
		return p.tryAddOld(obj)
	})
	newGroups := runGrouping(func(p *uploadGroup, obj objects.ObjectForUpload) bool {
		return p.tryAdd(obj)
	})

	type stats struct {
		packs         int
		objectsPacked int
		totalSlabs    int64
		maxWaste      float64
		totalWaste    float64
		singleObject  int
		maxSlabs      int64
	}

	collect := func(groups []uploadGroup) stats {
		var s stats
		s.packs = len(groups)
		for _, g := range groups {
			s.objectsPacked += len(g.objects)
			s.totalSlabs += g.slabs()
			w := g.wastePct()
			s.totalWaste += w
			if w > s.maxWaste {
				s.maxWaste = w
			}
			if len(g.objects) == 1 {
				s.singleObject++
			}
			if g.slabs() > s.maxSlabs {
				s.maxSlabs = g.slabs()
			}
		}
		return s
	}

	oldStats := collect(oldGroups)
	newStats := collect(newGroups)

	t.Logf("%-20s %10s %10s", "", "Old", "New")
	t.Logf("%-20s %10d %10d", "Packs", oldStats.packs, newStats.packs)
	t.Logf("%-20s %10d %10d", "Objects packed", oldStats.objectsPacked, newStats.objectsPacked)
	t.Logf("%-20s %10d %10d", "Total slabs", oldStats.totalSlabs, newStats.totalSlabs)
	t.Logf("%-20s %10.2f%% %9.2f%%", "Max waste", oldStats.maxWaste*100, newStats.maxWaste*100)
	t.Logf("%-20s %10.2f%% %9.2f%%", "Avg waste", oldStats.totalWaste/float64(oldStats.packs)*100, newStats.totalWaste/float64(newStats.packs)*100)
	t.Logf("%-20s %10d %10d", "Single-obj packs", oldStats.singleObject, newStats.singleObject)
	t.Logf("%-20s %10d %10d", "Max slabs/pack", oldStats.maxSlabs, newStats.maxSlabs)

	// the new algorithm should cap slabs per pack
	if newStats.maxSlabs > maxSlabsPerUpload {
		t.Errorf("new algorithm exceeded slab cap: %d > %d", newStats.maxSlabs, maxSlabsPerUpload)
	}

	// the new algorithm should have lower max waste
	if newStats.maxWaste > oldStats.maxWaste {
		t.Errorf("new algorithm has higher max waste: %.2f%% > %.2f%%", newStats.maxWaste*100, oldStats.maxWaste*100)
	}

	// the new algorithm should have fewer single-object packs
	if newStats.singleObject > oldStats.singleObject {
		t.Errorf("new algorithm has more single-object packs: %d > %d", newStats.singleObject, oldStats.singleObject)
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
