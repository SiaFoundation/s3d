package sia

import (
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

func TestBuildRepackPlan(t *testing.T) {
	idA := types.Hash256{1}
	idB := types.Hash256{2}
	idLarge := types.Hash256{3}

	snapshot := objects.RepackSnapshot{Slabs: []objects.RepackSlab{
		{
			ID:          types.Hash256{10},
			MinShards:   24,
			TotalShards: 48,
			LiveBytes:   4 << 20,
			ObjectRefs: []objects.RepackObjectRef{
				{ID: idA, Size: 2 << 20, Ready: true},
				{ID: idB, Size: 2 << 20, Ready: true},
			},
		},
		{
			// This slab shares idB with the first slab. Selecting it should
			// consume only the one additional MiB of idA/idB not already in
			// the plan, rather than double-counting shared objects.
			ID:          types.Hash256{11},
			MinShards:   24,
			TotalShards: 48,
			LiveBytes:   2 << 20,
			ObjectRefs: []objects.RepackObjectRef{
				{ID: idB, Size: 2 << 20, Ready: true},
			},
		},
		{
			ID:          types.Hash256{12},
			MinShards:   24,
			TotalShards: 48,
			LiveBytes:   1 << 20,
			ObjectRefs: []objects.RepackObjectRef{
				{ID: idLarge, Size: 20 << 20, Ready: true},
			},
		},
	}}

	opts := s3.RepackOptions{
		MaxSlabFillPercent: 10,
		MaxObjectSize:      10 << 20,
		ReuploadBudget:     4 << 20,
	}
	plan := buildRepackPlan(snapshot, opts, 96<<20)

	if plan.preview.Slabs != 3 {
		t.Fatalf("expected 3 slabs, got %d", plan.preview.Slabs)
	} else if plan.preview.CandidateSlabs != 3 {
		t.Fatalf("expected 3 candidate slabs, got %d", plan.preview.CandidateSlabs)
	} else if plan.preview.SelectedSlabs != 2 {
		t.Fatalf("expected 2 selected slabs, got %d", plan.preview.SelectedSlabs)
	} else if plan.preview.ObjectsToRewrite != 2 {
		t.Fatalf("expected 2 objects to rewrite, got %d", plan.preview.ObjectsToRewrite)
	} else if plan.preview.RewriteBytes != 4<<20 {
		t.Fatalf("expected 4 MiB rewrite, got %d", plan.preview.RewriteBytes)
	} else if plan.preview.SkippedLargeSlabs != 1 {
		t.Fatalf("expected one large-object slab to be skipped, got %d", plan.preview.SkippedLargeSlabs)
	}

	logicalCapacity := int64(3 * 24 * proto.SectorSize)
	if plan.preview.LogicalCapacity != logicalCapacity {
		t.Fatalf("expected logical capacity %d, got %d", logicalCapacity, plan.preview.LogicalCapacity)
	}
	if plan.preview.EstimatedLogicalReclaim <= 0 || plan.preview.EstimatedPhysicalReclaim <= plan.preview.EstimatedLogicalReclaim {
		t.Fatalf("unexpected reclaim estimates: logical=%d physical=%d",
			plan.preview.EstimatedLogicalReclaim, plan.preview.EstimatedPhysicalReclaim)
	}
}

func TestBuildRepackPlanBudget(t *testing.T) {
	snapshot := objects.RepackSnapshot{Slabs: []objects.RepackSlab{
		{
			ID:          types.Hash256{1},
			MinShards:   24,
			TotalShards: 48,
			LiveBytes:   1 << 20,
			ObjectRefs: []objects.RepackObjectRef{
				{ID: types.Hash256{2}, Size: 2 << 20, Ready: true},
			},
		},
		{
			ID:          types.Hash256{3},
			MinShards:   24,
			TotalShards: 48,
			LiveBytes:   1 << 20,
			ObjectRefs: []objects.RepackObjectRef{
				{ID: types.Hash256{4}, Size: 2 << 20, Ready: true},
			},
		},
	}}

	plan := buildRepackPlan(snapshot, s3.RepackOptions{
		MaxSlabFillPercent: 10,
		MaxObjectSize:      10 << 20,
		ReuploadBudget:     2 << 20,
	}, 96<<20)
	if plan.preview.SelectedSlabs != 1 {
		t.Fatalf("expected one selected slab, got %d", plan.preview.SelectedSlabs)
	} else if plan.preview.SkippedBudgetSlabs != 1 {
		t.Fatalf("expected one budget-limited slab, got %d", plan.preview.SkippedBudgetSlabs)
	} else if plan.preview.RewriteBytes != 2<<20 {
		t.Fatalf("expected 2 MiB rewrite, got %d", plan.preview.RewriteBytes)
	}
}

func TestBuildRepackPlanBusyObject(t *testing.T) {
	snapshot := objects.RepackSnapshot{Slabs: []objects.RepackSlab{{
		ID:          types.Hash256{1},
		MinShards:   24,
		TotalShards: 48,
		LiveBytes:   1 << 20,
		ObjectRefs: []objects.RepackObjectRef{
			{ID: types.Hash256{2}, Size: 1 << 20, Ready: false},
		},
	}}}
	plan := buildRepackPlan(snapshot, s3.RepackOptions{
		MaxSlabFillPercent: 10,
		MaxObjectSize:      10 << 20,
		ReuploadBudget:     1 << 30,
	}, 96<<20)
	if plan.preview.SelectedSlabs != 0 || plan.preview.BusySlabs != 1 {
		t.Fatalf("expected busy slab to be skipped: selected=%d busy=%d",
			plan.preview.SelectedSlabs, plan.preview.BusySlabs)
	}
}

func TestRepackPhysicalSlabSize(t *testing.T) {
	snapshot := objects.RepackSnapshot{Slabs: []objects.RepackSlab{
		{MinShards: 24, TotalShards: 48},
		{MinShards: 24, TotalShards: 60},
		{MinShards: 10, TotalShards: 20},
	}}
	got := repackNewPhysicalSlabSize(snapshot, 24*int64(proto.SectorSize))
	want := int64(60) * int64(proto.SectorSize)
	if got != want {
		t.Fatalf("expected conservative physical slab size %d, got %d", want, got)
	}
}
