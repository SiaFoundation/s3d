package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// DefaultPackingWastePct is the maximum percentage of wasted space that is tolerated
	// before an object is packed. Objects whose upload would waste more than this percentage are
	// written to disk and batched together with other small objects to fill slabs efficiently.
	DefaultPackingWastePct = 0.1

	packedUploadThreads = 8

	extMultipartPart = "part"
	extPackedObject  = "dat"
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
		objects   []objects.PackedObject
		totalSize int64
	}
)

func (p *packedObjects) remainingSpace(slabSize int64) int64 {
	if p.totalSize == 0 {
		return slabSize
	}
	remainder := p.totalSize % slabSize
	if remainder == 0 {
		return 0
	}
	return slabSize - remainder
}

func (p *packedObjects) wastePct(slabSize int64) float64 {
	if p.totalSize == 0 {
		return 1
	}
	remainder := p.totalSize % slabSize
	if remainder == 0 {
		return 0
	}
	waste := slabSize - remainder
	return float64(waste) / float64(p.totalSize+waste)
}

func (p *packedObjects) tryAdd(obj objects.PackedObject, slabSize int64, packingWastePct float64) bool {
	// if we already meet the waste threshold, only allow small objects that fit in the remaining space
	if p.wastePct(slabSize) < packingWastePct {
		if obj.Length > slabSize {
			return false
		} else if obj.Length > p.remainingSpace(slabSize) {
			return false
		}
	}
	p.objects = append(p.objects, obj)
	p.totalSize += obj.Length
	return true
}

// preparePackedObjects fetches objects for packing and groups
// them using first fit decreasing. Candidates are returned
// from the store in descending size order, and each object is
// placed in the first group where it fits or starts a new one.
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
			added = groups[i].tryAdd(obj, s.slabSize, s.packingWastePct)
			if added {
				break
			}
		}
		if !added {
			groups = append(groups, packedObjects{
				totalSize: obj.Length,
				objects:   []objects.PackedObject{obj},
			})
		}
	}

	// filter groups that meet the waste threshold
	var ready []packedObjects
	var remaining []objects.PackedObject
	for _, g := range groups {
		if g.wastePct(s.slabSize) < s.packingWastePct {
			ready = append(ready, g)
		} else {
			remaining = append(remaining, g.objects...)
		}
	}

	// try and fill gaps with remaining objects
	for _, obj := range remaining {
		for i := range ready {
			if ready[i].tryAdd(obj, s.slabSize, s.packingWastePct) {
				break
			}
		}
	}

	for _, g := range ready {
		s.logger.Info("created pack",
			zap.Int("objects", len(g.objects)),
			zap.Int64("size", g.totalSize),
			zap.Float64("wastePct", g.wastePct(s.slabSize)))
	}

	return ready
}

// needsPacking returns true if uploading size bytes directly
// would waste more than the configured packing threshold.
func (s *Sia) needsPacking(size int64) bool {
	if s.slabSize <= 0 {
		return false
	}
	remainder := size % s.slabSize
	if remainder == 0 {
		return false
	}
	waste := s.slabSize - remainder
	wastePct := float64(waste) / float64(size+waste)
	return wastePct > s.packingWastePct
}

func (s *Sia) packingLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.triggerPackChan:
			s.logger.Debug("packing triggered")
			t.Reset(5 * time.Minute)
		case <-t.C:
		}

		s.packObjects(ctx)
	}
}

func (s *Sia) packObjects(ctx context.Context) {
	// fetch and prepare objects for packing
	packs := s.preparePackedObjects()
	if len(packs) == 0 {
		return
	}

	var wg sync.WaitGroup
	uploadsCh := make(chan packedObjects, packedUploadThreads)

	// start upload workers
	for range packedUploadThreads {
		wg.Go(func() {
			for p := range uploadsCh {
				s.logger.Info("uploading packed object",
					zap.Int("objects", len(p.objects)),
					zap.Int64("size", p.totalSize))

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
		file, err := s.openPackedFile(obj.Filename)
		if err != nil {
			s.logger.Error("failed to open packed file for upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			continue
		}
		n, err := upload.Add(ctx, file)
		if err != nil {
			s.logger.Error("failed to add object to packed upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Error(err))
			file.Close()
			continue
		} else if n != obj.Length {
			s.logger.Warn("unexpected number of bytes added to packed upload",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.String("filename", obj.Filename),
				zap.Int64("expected", obj.Length),
				zap.Int64("got", n))
		}
		file.Close()
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

		if err := s.store.FinalizeObject(packObj.Bucket, packObj.Name, packObj.Filename, obj.ID(), s.sdk.SealObject(obj)); err != nil {
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

		s.tryRemovePackedObject(&packObj.Filename)
		s.logger.Debug("packed object uploaded to Sia",
			zap.String("bucket", packObj.Bucket),
			zap.String("name", packObj.Name))
	}

	return nil
}

func (s *Sia) openPackedFile(filename string) (*os.File, error) {
	return os.Open(filepath.Join(s.packingDir, filename))
}

func (s *Sia) openPackedObject(obj objects.Object) (*os.File, error) {
	// nothing to do if the object is nil or it's not a packed object
	if obj.Filename == nil {
		return nil, nil
	}

	// open the file on disk, if it does not exist, it may have been
	// uploaded in the background, refresh the object and try again
	f, err := s.openPackedFile(*obj.Filename)
	if errors.Is(err, os.ErrNotExist) {
		obj, err = s.store.GetObject(obj.Bucket, obj.Name, nil)
		if err != nil {
			return nil, err
		} else if obj.Filename != nil {
			return nil, fmt.Errorf("file %q not found on disk but object still references it", *obj.Filename)
		}
		return nil, nil
	}

	return f, err
}

func (s *Sia) triggerPacking() {
	select {
	case s.triggerPackChan <- struct{}{}:
	default:
	}
}

func (s *Sia) tryRemovePackedObject(filename *string) {
	if filename == nil {
		return
	}
	if err := os.Remove(filepath.Join(s.packingDir, *filename)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove file on disk",
			zap.String("filename", *filename),
			zap.Error(err))
	}
}

func (s *Sia) writePackedObject(r io.Reader) (filename string, err error) {
	filename = randFilename(extPackedObject)
	filePath := filepath.Join(s.packingDir, filename)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}

	// defer cleanup
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(filePath)
		}
	}()

	// copy and sync
	if _, err = io.Copy(f, r); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	} else if err = f.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	// sync parent directory
	dir, err := os.Open(s.packingDir)
	if err != nil {
		return "", fmt.Errorf("failed to open packing directory: %w", err)
	} else if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return "", fmt.Errorf("failed to sync packing directory: %w", err)
	}

	return filename, nil
}

func randFilename(ext string) string {
	var uuid [8]byte
	frand.Read(uuid[:])
	return fmt.Sprintf("%x.%s", uuid[:], ext)
}
