package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const (
	// DefaultPackingWastePct is the maximum percentage of wasted space that is tolerated
	// before an object is packed. Objects whose upload would waste more than this percentage are
	// written to disk and batched together with other small objects to fill slabs efficiently.
	DefaultPackingWastePct = 0.1

	packedUploadThreads = 8
	maxSlabsPerPack     = 6
)

// PackedUpload defines the interface for a packed upload.
type PackedUpload interface {
	Add(ctx context.Context, r io.Reader) (int64, error)
	Length() int64
	Remaining() int64
	Finalize(ctx context.Context) ([]sdk.Object, error)
	Close() error
}

type (
	// packedObjects groups objects that will be uploaded together
	// in a single packed upload.
	packedObjects struct {
		slabSize        int64
		packingWastePct float64

		objects   []objects.PackedObject
		totalSize int64
	}
)

func (p *packedObjects) remainingSpace() int64 {
	if p.totalSize == 0 {
		return p.slabSize
	}
	remainder := p.totalSize % p.slabSize
	if remainder == 0 {
		return 0
	}
	return p.slabSize - remainder
}

func (p *packedObjects) wastePct() float64 {
	if p.totalSize == 0 {
		return 1
	}
	remainder := p.totalSize % p.slabSize
	if remainder == 0 {
		return 0
	}
	waste := p.slabSize - remainder
	return float64(waste) / float64(p.totalSize+waste)
}

func (p *packedObjects) slabs() int64 {
	if p.totalSize == 0 {
		return 0
	}
	return (p.totalSize + p.slabSize - 1) / p.slabSize
}

func (p *packedObjects) tryAdd(obj objects.PackedObject) bool {
	newTotal := p.totalSize + obj.Length
	newSlabs := (newTotal + p.slabSize - 1) / p.slabSize

	// don't exceed the maximum number of slabs per pack
	if newSlabs > maxSlabsPerPack {
		return false
	}

	// if we already meet the waste threshold, only accept the object if
	// the resulting group still meets it
	if p.wastePct() < p.packingWastePct {
		remainder := newTotal % p.slabSize
		if remainder != 0 {
			waste := p.slabSize - remainder
			if float64(waste)/float64(newTotal+waste) >= p.packingWastePct {
				return false
			}
		}
	}

	p.objects = append(p.objects, obj)
	p.totalSize += obj.Length
	return true
}

func (s *Sia) newPackedObject(initial objects.PackedObject) packedObjects {
	return packedObjects{
		slabSize:        s.slabSize,
		packingWastePct: s.packingWastePct,
		objects:         []objects.PackedObject{initial},
		totalSize:       initial.Length,
	}
}

func (s *Sia) preparePackedObjects() []packedObjects {
	candidates, err := s.store.ObjectsForPacking()
	if err != nil {
		s.logger.Error("failed to fetch objects for packing", zap.Error(err))
		return nil
	} else if len(candidates) == 0 {
		return nil
	}

	var totalSize int64
	for _, obj := range candidates {
		totalSize += obj.Length
	}
	s.logger.Info("found objects for packing",
		zap.Int("objects", len(candidates)),
		zap.Int64("totalSize", totalSize))

	// place each object in the first group where it fits
	var groups []packedObjects
	for _, obj := range candidates {
		var added bool
		for i := range groups {
			added = groups[i].tryAdd(obj)
			if added {
				break
			}
		}
		if !added {
			groups = append(groups, s.newPackedObject(obj))
		}
	}

	// filter groups that meet the waste threshold
	var ready []packedObjects
	var remaining []objects.PackedObject
	for _, g := range groups {
		if g.wastePct() < s.packingWastePct {
			ready = append(ready, g)
		} else {
			remaining = append(remaining, g.objects...)
		}
	}

	// try and fill gaps with remaining objects
	for _, obj := range remaining {
		for i := range ready {
			if ready[i].tryAdd(obj) {
				break
			}
		}
	}

	for _, g := range ready {
		s.logger.Info("created pack",
			zap.Int("objects", len(g.objects)),
			zap.Int64("size", g.totalSize),
			zap.Int64("slabs", g.slabs()),
			zap.String("waste", fmt.Sprintf("%.2f%%", g.wastePct()*100)))
	}

	return ready
}

func (s *Sia) packingLoop(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.packObjects(ctx)
		}
	}
}

func (s *Sia) packObjects(ctx context.Context) {
	// fetch and prepare objects for packing
	packs := s.preparePackedObjects()
	if len(packs) == 0 {
		s.logger.Debug("packing loop tick")
		return
	}

	var wg sync.WaitGroup
	uploadsCh := make(chan packedObjects, packedUploadThreads)

	// start upload workers
	for range packedUploadThreads {
		wg.Go(func() {
			for p := range uploadsCh {
				s.logger.Info("uploading packed object",
					zap.Int64("size", p.totalSize),
					zap.Int64("slabs", p.slabs()),
					zap.String("waste", fmt.Sprintf("%.2f%%", p.wastePct()*100)),
					zap.Int("n", len(p.objects)),
				)

				err := s.uploadPackedObjects(ctx, p)
				if err != nil {
					s.logger.Error("failed to upload packed object", zap.Error(err))
				}
			}
		})
	}

	// send uploads to workers
	for _, p := range packs {
		uploadsCh <- p
	}
	close(uploadsCh)

	wg.Wait()
}

func (s *Sia) uploadPackedObjects(ctx context.Context, pack packedObjects) error {
	upload, err := s.sdk.UploadPacked()
	if err != nil {
		return fmt.Errorf("failed to create packed upload: %w", err)
	}
	defer upload.Close()

	var objIdx []int
	for i, obj := range pack.objects {
		rc, err := s.openUpload(obj.Bucket, obj.Name, &objects.Object{
			FileName:   &obj.Filename,
			PartsCount: obj.PartsCount,
		}, 0)
		if err != nil {
			s.logger.Error("failed to open upload for packing",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			continue
		}
		n, err := upload.Add(ctx, rc)
		if err != nil {
			s.logger.Error("failed to add object to packed upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			rc.Close()
			continue
		} else if n != obj.Length {
			s.logger.Warn("unexpected number of bytes added to packed upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Int64("expected", obj.Length),
				zap.Int64("got", n))
			rc.Close()
			return fmt.Errorf("packed upload short write for %s/%s: expected %d bytes, got %d", obj.Bucket, obj.Name, obj.Length, n)
		}
		rc.Close()
		objIdx = append(objIdx, i)
	}

	// finalize upload
	results, err := upload.Finalize(ctx)
	if err != nil {
		s.logger.Error("failed to finalize packed upload", zap.Error(err))
		return err
	} else if len(results) != len(objIdx) {
		s.logger.Error("finalize returned unexpected number of results",
			zap.Int("expected", len(objIdx)),
			zap.Int("got", len(results)))
		return fmt.Errorf("unexpected number of results: expected %d, got %d", len(objIdx), len(results))
	}

	// pin object and finalize in store
	for i, obj := range results {
		packObj := pack.objects[objIdx[i]]
		if err := s.sdk.PinObject(ctx, obj); err != nil {
			s.logger.Error("failed to pin packed object",
				zap.String("bucket", packObj.Bucket),
				zap.String("name", packObj.Name),
				zap.Error(err))
			if delErr := s.sdk.DeleteObject(ctx, obj.ID()); delErr != nil {
				s.logger.Error("failed to delete object after pin failure",
					zap.String("bucket", packObj.Bucket),
					zap.String("name", packObj.Name),
					zap.Error(delErr))
			}
			continue
		}

		if err := s.store.MarkObjectUploaded(packObj.Bucket, packObj.Name, packObj.Filename, s.sdk.SealObject(obj)); err != nil {
			if errors.Is(err, objects.ErrObjectModified) {
				s.logger.Warn("object was modified during packing, skipping",
					zap.String("bucket", packObj.Bucket),
					zap.String("name", packObj.Name))
			} else {
				s.logger.Error("failed to finalize packed object in store",
					zap.String("bucket", packObj.Bucket),
					zap.String("name", packObj.Name),
					zap.Error(err))
			}

			// delete pinned object
			if err := s.sdk.DeleteObject(ctx, obj.ID()); err != nil {
				s.logger.Error("failed to delete pinned object after finalize failure",
					zap.String("bucket", packObj.Bucket),
					zap.String("name", packObj.Name),
					zap.Error(err))
			}
			continue
		}

		s.removeUploadQuiet(packObj.Filename)
		s.logger.Debug("packed object uploaded to Sia",
			zap.String("bucket", packObj.Bucket),
			zap.String("name", packObj.Name))
	}

	return nil
}

func (s *Sia) removeUploadQuiet(fileName string) {
	if err := s.removeUpload(fileName); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove file on disk",
			zap.String("filename", fileName),
			zap.Error(err))
	}
}
