package sia

import (
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
)

func TestPackedObjects(t *testing.T) {
	const slabSize = 256

	t.Run("slabRemaining", func(t *testing.T) {
		tests := []struct {
			name      string
			totalSize int64
			expected  int64
		}{
			{"empty", 0, slabSize},
			{"partial slab", 100, 156},
			{"exact slab", slabSize, slabSize},
			{"one and a half slabs", slabSize + 128, 128},
			{"two exact slabs", 2 * slabSize, slabSize},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := packedObjects{slabSize: slabSize, totalSize: tt.totalSize}
				if got := p.slabRemaining(); got != tt.expected {
					t.Fatalf("expected %d, got %d", tt.expected, got)
				}
			})
		}
	})

	t.Run("fits", func(t *testing.T) {
		tests := []struct {
			name      string
			totalSize int64
			objLen    int64
			expected  bool
		}{
			// object fits in remaining slab space
			{"small object fits", 100, 50, true},

			// object exceeds remaining slab space but is larger than a full slab
			{"large object crosses slab boundary", 100, slabSize + 1, true},

			// object exceeds remaining slab space and is smaller than a slab
			{"object does not fit in remainder", 100, 200, false},

			// object would exceed max slabs
			{"exceeds max slabs", (packedUploadMaxSlabs - 1) * slabSize, slabSize + 1, false},

			// object fits exactly in remaining space
			{"exact fit", 100, 156, true},

			// empty group fits any object within max slabs
			{"empty group small object", 0, 100, true},
			{"empty group full slab", 0, slabSize, true},
			{"empty group multi slab", 0, 2 * slabSize, true},
			{"empty group exceeds max slabs", 0, packedUploadMaxSlabs*slabSize + 1, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := packedObjects{slabSize: slabSize, totalSize: tt.totalSize}
				obj := objects.PackedObject{Length: tt.objLen}
				if got := p.fits(obj); got != tt.expected {
					t.Fatalf("expected %v, got %v", tt.expected, got)
				}
			})
		}
	})

	t.Run("add", func(t *testing.T) {
		p := packedObjects{slabSize: slabSize}
		p.add(objects.PackedObject{Length: 100, Name: "a"})
		p.add(objects.PackedObject{Length: 50, Name: "b"})

		if p.totalSize != 150 {
			t.Fatalf("expected totalSize 150, got %d", p.totalSize)
		} else if len(p.objects) != 2 {
			t.Fatalf("expected 2 objects, got %d", len(p.objects))
		}
	})
}
