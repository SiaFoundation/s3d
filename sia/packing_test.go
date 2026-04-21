package sia

import (
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap/zaptest"
)

func TestPackedObjects(t *testing.T) {
	const (
		slabSize        = 256
		packingWastePct = 0.1
	)
	newPack := func() packedObjects {
		return packedObjects{
			slabSize:        slabSize,
			packingWastePct: packingWastePct,
		}
	}
	p := newPack()

	// assert remaining space on empty group is a full slab
	if p.remainingSpace() != slabSize {
		t.Fatalf("expected %d, got %d", slabSize, p.remainingSpace())
	}

	// assert 100% waste on empty group
	if p.wastePct() != 1 {
		t.Fatalf("expected 1, got %f", p.wastePct())
	}

	// assert tryAdd succeeds when waste is high
	if !p.tryAdd(objects.PackedObject{Length: 100, Name: "a"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert remaining space decreased
	if p.remainingSpace() != 156 {
		t.Fatalf("expected 156, got %d", p.remainingSpace())
	}

	// assert filling to exact slab boundary succeeds
	if !p.tryAdd(objects.PackedObject{Length: 156, Name: "b"}) {
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
	if p.tryAdd(objects.PackedObject{Length: 1}) {
		t.Fatal("expected tryAdd to fail at slab boundary")
	}

	// assert slab spanning object is rejected when waste is low
	if p.tryAdd(objects.PackedObject{Length: slabSize + 1}) {
		t.Fatal("expected tryAdd to fail")
	}

	// start a new pack with space left in the slab
	p2 := newPack()
	if !p2.tryAdd(objects.PackedObject{Length: 200, Name: "x"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert object fitting in remaining space is accepted
	if !p2.tryAdd(objects.PackedObject{Length: 50, Name: "y"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert object exceeding remaining space is rejected when waste is low
	if p2.tryAdd(objects.PackedObject{Length: p2.remainingSpace() + 1}) {
		t.Fatal("expected tryAdd to fail")
	}
}

// packingStore is a minimal Store stub for testing preparePackedObjects.
type packingStore struct {
	Store
	objects []objects.PackedObject
}

func (s *packingStore) ObjectsForPacking() ([]objects.PackedObject, error) {
	return s.objects, nil
}

func TestPreparePackedObjects(t *testing.T) {
	// four objects sized so FFD puts 92+8 in one group and 85+42 in another,
	// only the first group meets 10% waste, then gap fill reclaims 85 but
	// not 42
	store := &packingStore{
		objects: []objects.PackedObject{
			{Name: "a", Length: 92},
			{Name: "b", Length: 85},
			{Name: "c", Length: 42},
			{Name: "d", Length: 8},
		},
	}
	s := Sia{
		store:           store,
		slabSize:        100,
		packingWastePct: 0.10,
		logger:          zaptest.NewLogger(t),
	}

	ready := s.preparePackedObjects()

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
