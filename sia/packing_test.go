package sia

import (
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
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
