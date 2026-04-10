package sia

import (
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
)

func TestPackedObjects(t *testing.T) {
	const slabSize = 256
	p := packedObjects{slabSize: slabSize}

	// assert remaining space on empty group is a full slab
	if p.remainingSpace() != slabSize {
		t.Fatalf("expected %d, got %d", slabSize, p.remainingSpace())
	}

	// assert 100% waste on empty group
	if p.wastePct() != 1 {
		t.Fatalf("expected 1, got %f", p.wastePct())
	}

	// add a small object
	if !p.tryAdd(objects.PackedObject{Length: 100, Name: "a"}) {
		t.Fatal("expected tryAdd to succeed")
	}

	// assert remaining space decreased
	if p.remainingSpace() != 156 {
		t.Fatalf("expected 156, got %d", p.remainingSpace())
	}

	// assert object that doesn't fit in remainder is rejected
	if p.tryAdd(objects.PackedObject{Length: 200}) {
		t.Fatal("expected tryAdd to fail for object exceeding remainder")
	}

	// assert object larger than a slab is accepted (spans slabs)
	if !p.tryAdd(objects.PackedObject{Length: slabSize + 1, Name: "b"}) {
		t.Fatal("expected tryAdd to succeed for slab spanning object")
	}

	// assert state after two adds
	if p.totalSize != 357 {
		t.Fatalf("expected totalSize 357, got %d", p.totalSize)
	} else if len(p.objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(p.objects))
	}

	// assert object exceeding max slabs is rejected
	p2 := packedObjects{slabSize: slabSize, totalSize: (packedUploadMaxSlabs - 1) * slabSize}
	if p2.tryAdd(objects.PackedObject{Length: slabSize + 1}) {
		t.Fatal("expected tryAdd to fail when exceeding max slabs")
	}

	// assert zero waste on exact slab boundary
	p3 := packedObjects{slabSize: slabSize, totalSize: slabSize}
	if p3.wastePct() != 0 {
		t.Fatalf("expected 0, got %f", p3.wastePct())
	}
}
