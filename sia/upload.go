package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const (
	// DefaultUploadWastePct is the maximum percentage of wasted space that is
	// tolerated before an object is uploaded. Objects whose upload would waste
	// more than this percentage are written to disk and batched together with
	// other small objects to fill slabs efficiently.
	DefaultUploadWastePct = 0.1

	// DefaultMaxGroupSize is the maximum total size of a single upload
	// group. Objects are batched together until this limit is reached.
	DefaultMaxGroupSize = 1 << 30 // 1 GiB

	numUploadThreads = 8
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
	// uploadGroup groups objects that will be uploaded together
	// in a single packed upload.
	uploadGroup struct {
		slabSize       int64
		maxGroupSize   int64
		uploadWastePct float64

		objects   []objects.ObjectForUpload
		totalSize int64
	}
)

func (p *uploadGroup) remainingSpace() int64 {
	if p.totalSize == 0 {
		return p.slabSize
	}
	remainder := p.totalSize % p.slabSize
	if remainder == 0 {
		return 0
	}
	return p.slabSize - remainder
}

func (p *uploadGroup) wastePct() float64 {
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

func (p *uploadGroup) slabs() int64 {
	if p.totalSize == 0 {
		return 0
	}
	return (p.totalSize + p.slabSize - 1) / p.slabSize
}

func (p *uploadGroup) tryAdd(obj objects.ObjectForUpload) bool {
	newTotal := p.totalSize + obj.Length

	// don't exceed the maximum group size
	maxSize := newTotal > p.maxGroupSize

	// once we meet the waste threshold, only accept objects that fit in
	// the remaining space of the last slab or that reduce waste
	if maxSize || p.wastePct() < p.uploadWastePct {
		var newWaste float64
		if remainder := newTotal % p.slabSize; remainder != 0 {
			waste := p.slabSize - remainder
			newWaste = float64(waste) / float64(newTotal+waste)
		}
		reducesWaste := newWaste < p.wastePct()
		fitsLastSlab := obj.Length <= p.remainingSpace()
		if maxSize && !fitsLastSlab {
			// max group size exceeded and object doesn't fit in remaining space
			return false
		} else if !fitsLastSlab && !reducesWaste {
			// neither fits in remaining space nor reduces waste
			return false
		}
	}

	p.objects = append(p.objects, obj)
	p.totalSize += obj.Length
	return true
}

func (s *Sia) newUploadGroup(initial objects.ObjectForUpload) uploadGroup {
	return uploadGroup{
		slabSize:       s.slabSize,
		maxGroupSize:   DefaultMaxGroupSize,
		uploadWastePct: s.uploadWastePct,
		objects:        []objects.ObjectForUpload{initial},
		totalSize:      initial.Length,
	}
}

func (s *Sia) prepareUploads() []uploadGroup {
	candidates, err := s.store.ObjectsForUpload()
	if err != nil {
		s.logger.Error("failed to fetch objects for upload", zap.Error(err))
		return nil
	} else if len(candidates) == 0 {
		return nil
	}

	var totalSize int64
	for _, obj := range candidates {
		totalSize += obj.Length
	}
	s.logger.Info("found objects for upload",
		zap.Int("objects", len(candidates)),
		zap.Int64("totalSize", totalSize))

	// place each object in the first group where it fits
	var groups []uploadGroup
	for _, obj := range candidates {
		var added bool
		for i := range groups {
			added = groups[i].tryAdd(obj)
			if added {
				break
			}
		}
		if !added {
			groups = append(groups, s.newUploadGroup(obj))
		}
	}

	// filter groups that meet the waste threshold
	filtered := groups[:0]
	for _, g := range groups {
		if g.wastePct() < s.uploadWastePct {
			filtered = append(filtered, g)
		}
	}

	for _, g := range filtered {
		s.logger.Info("created upload group",
			zap.Int("objects", len(g.objects)),
			zap.Int64("size", g.totalSize),
			zap.Int64("slabs", g.slabs()),
			zap.String("waste", fmt.Sprintf("%.2f%%", g.wastePct()*100)))
	}

	return filtered
}

func (s *Sia) uploadLoop(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.uploadObjects(ctx)
		}
	}
}

func (s *Sia) uploadObjects(ctx context.Context) { //nolint:revive
	// fetch and prepare objects for upload
	groups := s.prepareUploads()
	if len(groups) == 0 {
		s.logger.Debug("upload loop tick")
		return
	}

	var wg sync.WaitGroup
	uploadsCh := make(chan uploadGroup, numUploadThreads)

	// start upload workers
	for range numUploadThreads {
		wg.Go(func() {
			for g := range uploadsCh {
				s.logger.Info("uploading object group",
					zap.Int64("size", g.totalSize),
					zap.Int64("slabs", g.slabs()),
					zap.String("waste", fmt.Sprintf("%.2f%%", g.wastePct()*100)),
					zap.Int("n", len(g.objects)),
				)

				err := s.uploadObjectGroup(ctx, g)
				if err != nil {
					s.logger.Error("failed to upload object group", zap.Error(err))
				}
			}
		})
	}

	// send uploads to workers
	for _, g := range groups {
		uploadsCh <- g
	}
	close(uploadsCh)

	wg.Wait()
}

func (s *Sia) uploadObjectGroup(ctx context.Context, group uploadGroup) error {
	upload, err := s.sdk.UploadPacked()
	if err != nil {
		return fmt.Errorf("failed to create packed upload: %w", err)
	}
	defer upload.Close()

	var objIdx []int
	for i, obj := range group.objects {
		rc, err := s.openUpload(obj.Bucket, obj.Name, &obj.Filename, obj.Multipart, 0)
		if err != nil {
			s.logger.Error("failed to open upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			continue
		}
		n, err := upload.Add(ctx, rc)
		if err != nil {
			s.logger.Error("failed to add object to upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			rc.Close()
			continue
		} else if n != obj.Length {
			s.logger.Warn("unexpected number of bytes added to upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Int64("expected", obj.Length),
				zap.Int64("got", n))
			rc.Close()
			return fmt.Errorf("upload short write for %s/%s: expected %d bytes, got %d", obj.Bucket, obj.Name, obj.Length, n)
		}
		rc.Close()
		objIdx = append(objIdx, i)
	}

	// finalize upload
	results, err := upload.Finalize(ctx)
	if err != nil {
		s.logger.Error("failed to finalize upload", zap.Error(err))
		return err
	} else if len(results) != len(objIdx) {
		s.logger.Error("finalize returned unexpected number of results",
			zap.Int("expected", len(objIdx)),
			zap.Int("got", len(results)))
		return fmt.Errorf("unexpected number of results: expected %d, got %d", len(objIdx), len(results))
	}

	// pin object and finalize in store
	for i, obj := range results {
		uploadObj := group.objects[objIdx[i]]
		if err := s.sdk.PinObject(ctx, obj); err != nil {
			s.logger.Error("failed to pin object",
				zap.String("bucket", uploadObj.Bucket),
				zap.String("name", uploadObj.Name),
				zap.Error(err))
			if delErr := s.sdk.DeleteObject(ctx, obj.ID()); delErr != nil {
				s.logger.Error("failed to delete object after pin failure",
					zap.String("bucket", uploadObj.Bucket),
					zap.String("name", uploadObj.Name),
					zap.Error(delErr))
			}
			continue
		}

		if err := s.store.MarkObjectUploaded(uploadObj.Bucket, uploadObj.Name, uploadObj.ContentMD5, s.sdk.SealObject(obj)); err != nil {
			if errors.Is(err, objects.ErrObjectNotFound) {
				s.logger.Warn("object was deleted during upload, skipping",
					zap.String("bucket", uploadObj.Bucket),
					zap.String("name", uploadObj.Name))
			} else if errors.Is(err, objects.ErrObjectModified) {
				s.logger.Warn("object was modified during upload, skipping",
					zap.String("bucket", uploadObj.Bucket),
					zap.String("name", uploadObj.Name))
			} else {
				s.logger.Error("failed to finalize object in store",
					zap.String("bucket", uploadObj.Bucket),
					zap.String("name", uploadObj.Name),
					zap.Error(err))
			}

			// delete pinned object
			if err := s.sdk.DeleteObject(ctx, obj.ID()); err != nil {
				s.logger.Error("failed to delete pinned object after finalize failure",
					zap.String("bucket", uploadObj.Bucket),
					zap.String("name", uploadObj.Name),
					zap.Error(err))
			}
			continue
		}

		s.logger.Debug("object uploaded to Sia",
			zap.String("bucket", uploadObj.Bucket),
			zap.String("name", uploadObj.Name))

		if uploadObj.Multipart {
			if err := s.removeMultipartUploadDir(uploadObj.Filename); err != nil {
				s.logger.Error("failed to remove multipart upload directory",
					zap.String("uploadID", uploadObj.Filename),
					zap.Error(err))
			}
		} else {
			if err := s.removeUpload(uploadObj.Filename); err != nil {
				s.logger.Error("failed to remove file on disk",
					zap.String("filename", uploadObj.Filename),
					zap.Error(err))
			}
		}
	}

	return nil
}
