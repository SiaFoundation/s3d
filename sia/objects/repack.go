package objects

import (
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

// RepackObjectRef describes one Sia object referenced by a slab. Size is the
// object's full logical size across all of its slab slices. Ready is false
// while the object is still in the upload/pin pipeline.
type RepackObjectRef struct {
	ID    types.Hash256
	Label string
	Size  int64
	Ready bool
}

// RepackSlab describes the live occupancy of one slab and the Sia objects that
// currently reference it.
type RepackSlab struct {
	ID          types.Hash256
	MinShards   int
	TotalShards int
	LiveBytes   int64
	ObjectRefs  []RepackObjectRef
}

// RepackSnapshot is a point-in-time view of all slabs used to compute repack
// statistics and plans without mutating the database.
type RepackSnapshot struct {
	Slabs []RepackSlab
}

// RepackReplacement atomically moves every live S3 metadata row that references
// OldID to New. The caller must pin New in indexd before committing the swap.
type RepackReplacement struct {
	OldID types.Hash256
	New   sdk.SealedObject
}
