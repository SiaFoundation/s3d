package sia

import (
	"testing"

	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

func TestUploadProgressDiagnostics(t *testing.T) {
	d := newUploadProgressDiagnostics()
	hostA := types.GeneratePrivateKey().PublicKey()
	hostB := types.GeneratePrivateKey().PublicKey()

	d.record(sdk.ShardProgress{HostKey: hostA, SlabIndex: 0, ShardIndex: 0})
	d.record(sdk.ShardProgress{HostKey: hostB, SlabIndex: 0, ShardIndex: 1})
	d.record(sdk.ShardProgress{HostKey: hostA, SlabIndex: 1, ShardIndex: 0})
	// Duplicate callbacks for the same shard must not inflate the count.
	d.record(sdk.ShardProgress{HostKey: hostA, SlabIndex: 1, ShardIndex: 0})

	if got := len(d.slabs); got != 2 {
		t.Fatalf("expected 2 slabs, got %d", got)
	}
	if got := len(d.slabs[0].successfulShards); got != 2 {
		t.Fatalf("expected 2 successful shards for slab 0, got %d", got)
	}
	if got := len(d.slabs[1].successfulShards); got != 1 {
		t.Fatalf("expected 1 successful shard for slab 1, got %d", got)
	}
}
