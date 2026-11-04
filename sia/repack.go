package sia

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const (
	defaultRepackFillPercent = 10.0
	defaultRepackMaxObject   = int64(10 << 20) // 10 MiB
	defaultRepackBudget      = int64(1 << 30)  // 1 GiB
)

type repackPlan struct {
	preview s3.RepackPreview
	objects []objects.RepackObjectRef
}

func normalizeRepackOptions(opts s3.RepackOptions) (s3.RepackOptions, error) {
	if opts.MaxSlabFillPercent == 0 {
		opts.MaxSlabFillPercent = defaultRepackFillPercent
	}
	if opts.MaxObjectSize == 0 {
		opts.MaxObjectSize = defaultRepackMaxObject
	}
	if opts.ReuploadBudget == 0 {
		opts.ReuploadBudget = defaultRepackBudget
	}

	switch {
	case opts.MaxSlabFillPercent <= 0 || opts.MaxSlabFillPercent >= 100:
		return opts, fmt.Errorf("max slab fill percent must be between 0 and 100")
	case opts.MaxObjectSize < 0:
		return opts, fmt.Errorf("max object size must not be negative")
	case opts.ReuploadBudget < 0:
		return opts, fmt.Errorf("re-upload budget must not be negative")
	default:
		return opts, nil
	}
}

func repackSlabCapacity(slab objects.RepackSlab) int64 {
	return int64(slab.MinShards) * int64(proto.SectorSize)
}

func repackSlabPhysicalCapacity(slab objects.RepackSlab) int64 {
	return int64(slab.TotalShards) * int64(proto.SectorSize)
}

func repackSlabWaste(slab objects.RepackSlab) int64 {
	capacity := repackSlabCapacity(slab)
	if slab.LiveBytes >= capacity {
		return 0
	}
	return capacity - slab.LiveBytes
}

func repackSlabPhysicalWaste(slab objects.RepackSlab) int64 {
	waste := repackSlabWaste(slab)
	if waste == 0 || slab.MinShards <= 0 {
		return 0
	}
	return waste * int64(slab.TotalShards) / int64(slab.MinShards)
}

// repackNewPhysicalSlabSize derives a conservative physical slab size from
// existing slabs that use the same logical shard count as the current uploader.
// If multiple redundancy layouts exist, the largest one is used so the reclaim
// estimate is never inflated by choosing a lower redundancy.
func repackNewPhysicalSlabSize(snapshot objects.RepackSnapshot, logicalSlabSize int64) int64 {
	if logicalSlabSize <= 0 || logicalSlabSize%int64(proto.SectorSize) != 0 {
		return 0
	}
	minShards := int(logicalSlabSize / int64(proto.SectorSize))
	var totalShards int
	for _, slab := range snapshot.Slabs {
		if slab.MinShards == minShards && slab.TotalShards > totalShards {
			totalShards = slab.TotalShards
		}
	}
	if totalShards == 0 {
		return 0
	}
	return int64(totalShards) * int64(proto.SectorSize)
}

func buildRepackPlan(snapshot objects.RepackSnapshot, opts s3.RepackOptions, newSlabSize int64) repackPlan {
	preview := s3.RepackPreview{Options: opts}
	for _, slab := range snapshot.Slabs {
		logicalCapacity := repackSlabCapacity(slab)
		preview.Slabs++
		preview.LogicalCapacity += logicalCapacity
		preview.LiveBytes += slab.LiveBytes
		preview.LogicalWaste += repackSlabWaste(slab)
		preview.PhysicalCapacity += repackSlabPhysicalCapacity(slab)
		preview.PhysicalWaste += repackSlabPhysicalWaste(slab)
	}
	if preview.LogicalCapacity > 0 {
		preview.WastePercent = 100 * float64(preview.LogicalWaste) / float64(preview.LogicalCapacity)
	}

	// Prefer the emptiest slabs first: they provide the best chance of freeing
	// a whole slab for the least amount of live data rewritten.
	slabs := append([]objects.RepackSlab(nil), snapshot.Slabs...)
	sort.Slice(slabs, func(i, j int) bool {
		ci, cj := repackSlabCapacity(slabs[i]), repackSlabCapacity(slabs[j])
		if ci == 0 {
			return false
		} else if cj == 0 {
			return true
		}
		fi := float64(slabs[i].LiveBytes) / float64(ci)
		fj := float64(slabs[j].LiveBytes) / float64(cj)
		if fi == fj {
			return repackSlabWaste(slabs[i]) > repackSlabWaste(slabs[j])
		}
		return fi < fj
	})

	selected := make(map[types.Hash256]objects.RepackObjectRef)
	var selectedOrder []types.Hash256
	var selectedBytes, selectedLogicalCapacity, selectedPhysicalCapacity int64

	for _, slab := range slabs {
		capacity := repackSlabCapacity(slab)
		if capacity <= 0 {
			continue
		}
		fillPercent := 100 * float64(slab.LiveBytes) / float64(capacity)
		if fillPercent >= opts.MaxSlabFillPercent {
			continue
		}
		preview.CandidateSlabs++

		if len(slab.ObjectRefs) == 0 {
			// A slab with no live object references is already garbage and
			// should be handled by normal orphan/prune processing.
			preview.BusySlabs++
			continue
		}

		busy := false
		tooLarge := false
		var additional int64
		var additionalIDs []types.Hash256
		for _, ref := range slab.ObjectRefs {
			if !ref.Ready {
				busy = true
				break
			}
			if opts.MaxObjectSize > 0 && ref.Size > opts.MaxObjectSize {
				tooLarge = true
				break
			}
			if _, exists := selected[ref.ID]; !exists {
				additional += ref.Size
				additionalIDs = append(additionalIDs, ref.ID)
			}
		}
		switch {
		case busy:
			preview.BusySlabs++
			continue
		case tooLarge:
			preview.SkippedLargeSlabs++
			continue
		case opts.ReuploadBudget > 0 && selectedBytes+additional > opts.ReuploadBudget:
			preview.SkippedBudgetSlabs++
			continue
		}

		for _, id := range additionalIDs {
			for _, ref := range slab.ObjectRefs {
				if ref.ID == id {
					selected[id] = ref
					selectedOrder = append(selectedOrder, id)
					selectedBytes += ref.Size
					break
				}
			}
		}
		preview.SelectedSlabs++
		selectedLogicalCapacity += repackSlabCapacity(slab)
		selectedPhysicalCapacity += repackSlabPhysicalCapacity(slab)
	}

	planObjects := make([]objects.RepackObjectRef, 0, len(selectedOrder))
	for _, id := range selectedOrder {
		planObjects = append(planObjects, selected[id])
	}
	preview.ObjectsToRewrite = len(planObjects)
	preview.RewriteBytes = selectedBytes

	// Estimate net reclaim conservatively: every selected sparse slab becomes
	// unreferenced, then the capacity required by the newly packed replacement
	// slabs is subtracted. Slabs outside the selected set that may also be
	// released are deliberately not counted.
	if len(planObjects) > 0 && newSlabSize > 0 {
		newSlabs := (selectedBytes + newSlabSize - 1) / newSlabSize
		newLogicalCapacity := newSlabs * newSlabSize
		if selectedLogicalCapacity > newLogicalCapacity {
			preview.EstimatedLogicalReclaim = selectedLogicalCapacity - newLogicalCapacity
		}

		newPhysicalSlabSize := repackNewPhysicalSlabSize(snapshot, newSlabSize)
		if newPhysicalSlabSize > 0 {
			newPhysicalCapacity := newSlabs * newPhysicalSlabSize
			if selectedPhysicalCapacity > newPhysicalCapacity {
				preview.EstimatedPhysicalReclaim = selectedPhysicalCapacity - newPhysicalCapacity
			}
		}
	}
	return repackPlan{preview: preview, objects: planObjects}
}

// RepackPreview calculates current slab waste and a safe rewrite plan without
// modifying local metadata or the Sia network.
func (s *Sia) RepackPreview(ctx context.Context, opts s3.RepackOptions) (s3.RepackPreview, error) {
	opts, err := normalizeRepackOptions(opts)
	if err != nil {
		return s3.RepackPreview{}, err
	} else if err := ctx.Err(); err != nil {
		return s3.RepackPreview{}, err
	}

	snapshot, err := s.store.RepackSnapshot()
	if err != nil {
		return s3.RepackPreview{}, fmt.Errorf("failed to load repack snapshot: %w", err)
	}
	return buildRepackPlan(snapshot, opts, s.uploadOptimalSize).preview, nil
}

func (s *Sia) repackIsRunning() bool {
	return s.repackStatus.State == "running"
}

// StartRepack recomputes the preview, then starts one background repack job.
// The request context controls planning only; the job is tied to the Sia
// backend threadgroup so shutting down s3d cancels it safely.
func (s *Sia) StartRepack(ctx context.Context, opts s3.RepackOptions) (s3.RepackStatus, error) {
	s.repackMu.Lock()
	if s.repackIsRunning() {
		status := s.repackStatus
		s.repackMu.Unlock()
		return status, s3.ErrRepackRunning
	}
	s.repackMu.Unlock()

	opts, err := normalizeRepackOptions(opts)
	if err != nil {
		return s3.RepackStatus{}, err
	} else if err := ctx.Err(); err != nil {
		return s3.RepackStatus{}, err
	}

	snapshot, err := s.store.RepackSnapshot()
	if err != nil {
		return s3.RepackStatus{}, fmt.Errorf("failed to load repack snapshot: %w", err)
	}
	plan := buildRepackPlan(snapshot, opts, s.uploadOptimalSize)
	if len(plan.objects) == 0 {
		return s3.RepackStatus{}, s3.ErrNoRepackCandidates
	}

	tgCtx, release, err := s.tg.AddContext(context.Background())
	if err != nil {
		return s3.RepackStatus{}, err
	}
	jobCtx, cancel := context.WithCancel(tgCtx)

	status := s3.RepackStatus{
		State:                    "running",
		Phase:                    "rewriting",
		Options:                  opts,
		StartedAt:                time.Now(),
		ObjectsTotal:             len(plan.objects),
		RewriteBytes:             plan.preview.RewriteBytes,
		EstimatedLogicalReclaim:  plan.preview.EstimatedLogicalReclaim,
		EstimatedPhysicalReclaim: plan.preview.EstimatedPhysicalReclaim,
	}

	s.repackMu.Lock()
	if s.repackIsRunning() {
		s.repackMu.Unlock()
		cancel()
		release()
		return s.repackStatus, s3.ErrRepackRunning
	}
	s.repackStatus = status
	s.repackCancel = cancel
	s.repackMu.Unlock()

	go func() {
		defer release()
		defer cancel()

		err := s.executeRepack(jobCtx, plan)

		s.repackMu.Lock()
		defer s.repackMu.Unlock()
		s.repackCancel = nil
		s.repackStatus.FinishedAt = time.Now()
		switch {
		case errors.Is(err, context.Canceled):
			s.repackStatus.State = "canceled"
		case err != nil:
			s.repackStatus.State = "failed"
			s.repackStatus.Error = err.Error()
		default:
			s.repackStatus.State = "completed"
			s.repackStatus.Phase = "completed"
			s.repackStatus.CurrentObject = ""
			s.repackStatus.CurrentObjectID = ""
			s.repackStatus.CurrentObjectIndex = 0
			s.repackStatus.CurrentObjectSize = 0
			s.repackStatus.ObjectsCompleted = s.repackStatus.ObjectsTotal
			s.repackStatus.RewrittenBytes = s.repackStatus.RewriteBytes
		}
	}()

	return status, nil
}

// CancelRepack requests cancellation of the active job. Cancellation is safe:
// live metadata continues to reference the old objects until every replacement
// is pinned and the final SQLite swap commits.
func (s *Sia) CancelRepack(_ context.Context) (s3.RepackStatus, error) {
	s.repackMu.Lock()
	defer s.repackMu.Unlock()
	if s.repackIsRunning() && s.repackCancel != nil {
		s.repackCancel()
	}
	status := s.repackStatus
	if status.State == "" {
		status.State = "idle"
	}
	return status, nil
}

// RepackStatus returns the current or most recent repack job.
func (s *Sia) RepackStatus(_ context.Context) (s3.RepackStatus, error) {
	s.repackMu.Lock()
	defer s.repackMu.Unlock()
	status := s.repackStatus
	if status.State == "" {
		status.State = "idle"
	}
	return status, nil
}

func (s *Sia) setRepackPhase(phase string) {
	s.repackMu.Lock()
	s.repackStatus.Phase = phase
	s.repackStatus.CurrentObject = ""
	s.repackStatus.CurrentObjectID = ""
	s.repackStatus.CurrentObjectIndex = 0
	s.repackStatus.CurrentObjectSize = 0
	s.repackMu.Unlock()
}

func (s *Sia) setRepackCurrent(phase string, index int, ref objects.RepackObjectRef) {
	label := ref.Label
	if label == "" {
		label = ref.ID.String()
	}
	s.repackMu.Lock()
	s.repackStatus.Phase = phase
	s.repackStatus.CurrentObject = label
	s.repackStatus.CurrentObjectID = ref.ID.String()
	s.repackStatus.CurrentObjectIndex = index
	s.repackStatus.CurrentObjectSize = ref.Size
	s.repackMu.Unlock()
}

func (s *Sia) addRepackProgress(bytes int64, objectsCompleted int) {
	s.repackMu.Lock()
	s.repackStatus.RewrittenBytes += bytes
	s.repackStatus.ObjectsCompleted = objectsCompleted
	s.repackMu.Unlock()
}

func (s *Sia) cleanupRepackPins(ids []types.Hash256) {
	if len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, id := range ids {
		err := s.sdk.DeleteObject(ctx, id)
		if err == nil || isObjectNotFound(err) {
			continue
		}

		s.logger.Error("failed to clean up repack replacement",
			zap.Stringer("objectID", &id),
			zap.Error(err))
		if queueErr := s.store.QueueOrphanedObject(id); queueErr != nil {
			s.logger.Error("failed to queue repack replacement for cleanup",
				zap.Stringer("objectID", &id),
				zap.Error(queueErr))
		} else {
			s.wakeOrphanLoop()
		}
	}
}

func (s *Sia) executeRepack(ctx context.Context, plan repackPlan) error {
	upload, err := s.sdk.UploadPacked(ctx, sdk.WithUploadProgress(s.shardProgress))
	if err != nil {
		return fmt.Errorf("failed to create repack upload: %w", err)
	}
	defer upload.Close()

	active := s.transfer.beginNamedUpload("slab repack", int64(len(plan.objects)), plan.preview.RewriteBytes)
	activeEnded := false
	defer func() {
		if !activeEnded {
			s.transfer.endUpload(active)
		}
	}()

	oldObjects := make([]objects.SiaObject, 0, len(plan.objects))
	for i, ref := range plan.objects {
		s.setRepackCurrent("rewriting", i+1, ref)
		if err := ctx.Err(); err != nil {
			return err
		}

		stored, err := s.store.SiaObject(ref.ID)
		if err != nil {
			return fmt.Errorf("failed to load repack source %v: %w", ref.ID, err)
		}
		obj, err := s.sdk.UnsealObject(stored.Sealed)
		if err != nil {
			return fmt.Errorf("failed to unseal repack source %v: %w", ref.ID, err)
		}
		rc, err := s.sdk.Download(ctx, obj, nil)
		if err != nil {
			return fmt.Errorf("failed to download repack source %v: %w", ref.ID, err)
		}

		n, addErr := upload.Add(s.transfer.trackUpload(rc, active))
		closeErr := rc.Close()
		if addErr != nil {
			return fmt.Errorf("failed to add repack source %v: %w", ref.ID, addErr)
		} else if closeErr != nil {
			return fmt.Errorf("failed to close repack source %v: %w", ref.ID, closeErr)
		} else if n != ref.Size {
			return fmt.Errorf("repack source %v changed size: expected %d, got %d", ref.ID, ref.Size, n)
		}

		oldObjects = append(oldObjects, stored)
		s.addRepackProgress(n, i+1)
	}

	s.setRepackPhase("finalizing")
	active.finalize()
	results, err := upload.Finalize()
	if err != nil {
		return fmt.Errorf("failed to finalize repack upload: %w", err)
	} else if len(results) != len(oldObjects) {
		return fmt.Errorf("repack finalize returned %d objects for %d sources", len(results), len(oldObjects))
	}
	s.transfer.endUpload(active)
	activeEnded = true

	// Pin every replacement before changing any live reference. If a pin fails,
	// remove the already-pinned replacements and leave the old objects intact.
	replacements := make([]objects.RepackReplacement, 0, len(results))
	pinned := make([]types.Hash256, 0, len(results))
	for i, result := range results {
		s.setRepackCurrent("pinning", i+1, plan.objects[i])
		if err := ctx.Err(); err != nil {
			s.cleanupRepackPins(pinned)
			return err
		}
		if err := s.sdk.PinObject(ctx, result); err != nil {
			s.cleanupRepackPins(pinned)
			return fmt.Errorf("failed to pin repack replacement: %w", err)
		}

		sealed, err := s.sdk.SealObject(result)
		if err != nil {
			s.cleanupRepackPins(pinned)
			return fmt.Errorf("failed to seal repack replacement: %w", err)
		}
		pinned = append(pinned, sealed.ID())
		replacements = append(replacements, objects.RepackReplacement{
			OldID: oldObjects[i].ID,
			New:   sealed,
		})
	}

	// One SQLite transaction switches every live reference. Until this commits,
	// every S3 object/version still points at its original, already-pinned data.
	s.setRepackPhase("committing")
	if err := s.store.ReplaceSiaObjects(replacements); err != nil {
		s.cleanupRepackPins(pinned)
		return fmt.Errorf("failed to commit repack metadata: %w", err)
	}

	s.repackMu.Lock()
	s.repackStatus.ObjectsCompleted = len(replacements)
	s.repackMu.Unlock()

	// Snapshot-generation-aware orphan processing decides when the old object
	// IDs can actually be unpinned. A sync may gate immediate cleanup.
	s.setRepackPhase("cleanup")
	s.ProcessOrphans(ctx)
	s.wakeOrphanLoop()
	return nil
}
