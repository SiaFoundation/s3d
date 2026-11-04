package sia

import (
	"sort"
	"sync"

	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

type slabProgress struct {
	successfulShards map[int]string
}

type uploadProgressDiagnostics struct {
	mu    sync.Mutex
	slabs map[int]*slabProgress
}

type partialSlabDiagnostic struct {
	SlabIndex        int `json:"slabIndex"`
	SuccessfulShards int `json:"successfulShards"`
	MissingShards    int `json:"missingShards"`
	UniqueHosts      int `json:"uniqueHosts"`
}

func newUploadProgressDiagnostics() *uploadProgressDiagnostics {
	return &uploadProgressDiagnostics{
		slabs: make(map[int]*slabProgress),
	}
}

func (d *uploadProgressDiagnostics) record(p sdk.ShardProgress) {
	d.mu.Lock()
	defer d.mu.Unlock()

	slab := d.slabs[p.SlabIndex]
	if slab == nil {
		slab = &slabProgress{successfulShards: make(map[int]string)}
		d.slabs[p.SlabIndex] = slab
	}
	slab.successfulShards[p.ShardIndex] = p.HostKey.String()
}

func (d *uploadProgressDiagnostics) logFailure(logger *zap.Logger, expectedSlabs int64, totalShards int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var (
		successfulShardUploads int
		completeSlabs          int
		uniqueHosts            = make(map[string]struct{})
		partial                []partialSlabDiagnostic
	)
	for slabIndex, slab := range d.slabs {
		n := len(slab.successfulShards)
		successfulShardUploads += n

		slabHosts := make(map[string]struct{}, n)
		for _, host := range slab.successfulShards {
			slabHosts[host] = struct{}{}
			uniqueHosts[host] = struct{}{}
		}

		if totalShards > 0 && n == totalShards {
			completeSlabs++
		} else {
			missing := 0
			if totalShards > n {
				missing = totalShards - n
			}
			partial = append(partial, partialSlabDiagnostic{
				SlabIndex:        slabIndex,
				SuccessfulShards: n,
				MissingShards:    missing,
				UniqueHosts:      len(slabHosts),
			})
		}
	}
	sort.Slice(partial, func(i, j int) bool {
		return partial[i].SlabIndex < partial[j].SlabIndex
	})

	logger.Error("failed to finalize upload",
		zap.Error(err),
		zap.Int64("expectedSlabs", expectedSlabs),
		zap.Int("shardsPerSlab", totalShards),
		zap.Int("slabsWithProgress", len(d.slabs)),
		zap.Int("completeSlabs", completeSlabs),
		zap.Int("successfulShardUploads", successfulShardUploads),
		zap.Int("uniqueSuccessfulHosts", len(uniqueHosts)),
		zap.Any("partialSlabs", partial))
}
