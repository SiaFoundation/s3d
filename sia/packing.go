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
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// DefaultPackingWastePct is the maximum percentage of wasted space in a
	// slab that is tolerated before an object is packed. Objects whose upload
	// would waste more than this percentage are written to disk and batched
	// together with other small objects to fill slabs efficiently.
	DefaultPackingWastePct = 0.1

	// packedUploadThreads is the maximum number of concurrent uploads of packed objects.
	packedUploadThreads = 4

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

// pendingUpload contains a packed upload and the objects that are being packed
type pendingUpload struct {
	upload  PackedUpload
	objects []objects.PackedObject
}

// canPack returns true if there's enough data for packing
func (s *Sia) canPack(totalSize int64) bool {
	minSize := int64(float64(s.slabSize) * (1 - s.packingWastePct))
	return totalSize >= minSize
}

func (s *Sia) createPendingUploads(ctx context.Context) (_ []pendingUpload, err error) {
	// fetch all candidates
	candidates, err := s.store.ObjectsForPacking()
	if err != nil {
		s.logger.Error("failed to fetch objects for packing", zap.Error(err))
		return nil, err
	}

	// check if there's enough data to pack
	var totalSize int64
	for _, obj := range candidates {
		totalSize += obj.Length
	}
	if !s.canPack(totalSize) {
		s.logger.Debug("not enough data to pack",
			zap.Int("objects", len(candidates)),
			zap.Int64("totalSize", totalSize),
			zap.Float64("wastePct", s.packingWastePct),
			zap.Int64("slabSize", s.slabSize))
		return nil, nil
	}

	s.logger.Info("packing objects",
		zap.Int("candidates", len(candidates)),
		zap.Int64("totalSize", totalSize))

	// close pending uploads on error
	var pending []pendingUpload
	defer func() {
		if err != nil {
			for _, pu := range pending {
				pu.upload.Close()
			}
		}
	}()

	// loop over candidates, try to add them to the pending uploads
	for _, obj := range candidates {
		if obj.Length > s.slabSize {
			s.logger.Warn("skipping object that exceeds slab size",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.Int64("size", obj.Length),
				zap.Int64("slabSize", s.slabSize))
			continue
		}

		// find a pending upload with room for the object
		var added bool
		for i := range pending {
			if obj.Length <= pending[i].upload.Remaining() {
				err := s.addToUpload(ctx, &pending[i], obj)
				if err != nil {
					return nil, fmt.Errorf("failed to add object: %w", err)
				}
				added = true
				break
			}
		}
		if added {
			continue
		}

		// create a new upload
		upload, err := s.sdk.UploadPacked()
		if err != nil {
			return nil, fmt.Errorf("failed to create packed upload: %w", err)
		}

		pu := pendingUpload{upload: upload}
		err = s.addToUpload(ctx, &pu, obj)
		if err != nil {
			return nil, fmt.Errorf("failed to add object: %w", err)
		}
		pending = append(pending, pu)
	}

	// discard pending uploads that don't meet the threshold
	ready := pending[:0]
	for _, pu := range pending {
		if !s.canPack(pu.upload.Length()) {
			s.logger.Debug("discarding underfilled upload",
				zap.Int("objects", len(pu.objects)),
				zap.Int64("uploadSize", pu.upload.Length()),
				zap.Int64("slabSize", s.slabSize))
			pu.upload.Close()
			continue
		}
		ready = append(ready, pu)
	}

	return ready, nil
}

// needsPacking returns true if the size meets the packing threshold
func (s *Sia) needsPacking(size int64) bool {
	if s.slabSize <= 0 {
		return false
	}

	remainder := size % s.slabSize
	if remainder == 0 {
		return false
	}

	waste := s.slabSize - remainder
	return float64(waste)/float64(s.slabSize) >= s.packingWastePct
}

func (s *Sia) packingLoop(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.triggerPackChan:
			s.logger.Debug("packing triggered")
			t.Reset(time.Minute)
		case <-t.C:
		}

		s.packObjects(ctx)
	}
}

func (s *Sia) packObjects(ctx context.Context) {
	// create pending uploads
	uploads, err := s.createPendingUploads(ctx)
	if err != nil {
		s.logger.Error("failed to create pending uploads", zap.Error(err))
		return
	} else if len(uploads) == 0 {
		s.logger.Debug("no pending uploads created, skipping packing")
		return
	}

	var wg sync.WaitGroup
	uploadsCh := make(chan pendingUpload, packedUploadThreads)

	// start workers to finalize uploads
	for range packedUploadThreads {
		wg.Go(func() {
			for pu := range uploadsCh {
				s.logger.Info("uploading packed object",
					zap.Int("objects", len(pu.objects)),
					zap.Int64("size", pu.upload.Length()))

				err := s.upload(ctx, pu)
				if err != nil {
					s.logger.Error("failed to upload packed object", zap.Error(err))
				}
			}
		})
	}

	// send uploads to workers
	for _, pu := range uploads {
		uploadsCh <- pu
	}
	close(uploadsCh)
	wg.Wait()
}

func (s *Sia) addToUpload(ctx context.Context, pu *pendingUpload, obj objects.PackedObject) error {
	f, err := os.Open(filepath.Join(s.packingDir, obj.Filename))
	if err != nil {
		s.logger.Warn("failed to open file for packing",
			zap.String("filename", obj.Filename),
			zap.Error(err))
		return nil
	}
	defer f.Close()

	if _, err := pu.upload.Add(ctx, f); err != nil {
		return fmt.Errorf("failed to add object to packed upload: %w", err)
	}
	pu.objects = append(pu.objects, obj)
	return nil
}

func (s *Sia) upload(ctx context.Context, pu pendingUpload) error {
	defer pu.upload.Close()

	// finalize upload
	results, err := pu.upload.Finalize(ctx)
	if err != nil {
		s.logger.Error("failed to finalize packed upload", zap.Error(err))
		return err
	} else if len(results) != len(pu.objects) {
		s.logger.Error("finalize returned unexpected number of results",
			zap.Int("expected", len(pu.objects)),
			zap.Int("got", len(results)))
		return fmt.Errorf("unexpected number of results: expected %d, got %d", len(pu.objects), len(results))
	}

	// pin object and finalize in store
	for i, obj := range pu.objects {
		siaObj := results[i]
		if err := s.sdk.PinObject(ctx, siaObj); err != nil {
			s.logger.Error("failed to pin packed object",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.Error(err))
			if delErr := s.sdk.DeleteObject(ctx, siaObj.ID()); delErr != nil {
				s.logger.Error("failed to delete object after pin failure",
					zap.String("bucket", obj.Bucket),
					zap.String("name", obj.Name),
					zap.Error(delErr))
			}
			continue
		}

		if err := s.store.FinalizeObject(obj.Bucket, obj.Name, obj.Filename, siaObj.ID(), s.sdk.SealObject(siaObj)); err != nil {
			if errors.Is(err, objects.ErrObjectModified) {
				s.logger.Warn("object was modified during packing, skipping",
					zap.String("bucket", obj.Bucket),
					zap.String("name", obj.Name))
			} else {
				s.logger.Error("failed to finalize packed object in store",
					zap.String("bucket", obj.Bucket),
					zap.String("name", obj.Name),
					zap.Error(err))
			}

			// delete pinned object
			if err := s.sdk.DeleteObject(ctx, siaObj.ID()); err != nil {
				s.logger.Error("failed to delete pinned object after finalize failure",
					zap.String("bucket", obj.Bucket),
					zap.String("name", obj.Name),
					zap.Error(err))
			}
			continue
		}

		s.tryRemove(&obj.Filename)
		s.logger.Debug("packed object uploaded to Sia",
			zap.String("bucket", obj.Bucket),
			zap.String("name", obj.Name))
	}

	return nil
}

func (s *Sia) triggerPacking() {
	select {
	case s.triggerPackChan <- struct{}{}:
	default:
	}
}

func (s *Sia) tryRemove(filename *string) {
	if filename == nil {
		return
	}
	if err := os.Remove(filepath.Join(s.packingDir, *filename)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove file on disk",
			zap.String("filename", *filename),
			zap.Error(err))
	}
}

func (s *Sia) writeToDisk(r io.Reader) (filename string, err error) {
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
