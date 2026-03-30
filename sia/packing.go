package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// DefaultPackingThreshold is the slab size at default redundancy (40 MiB).
	// objects smaller than this are stored on disk until enough data has
	// accumulated to fill a slab.
	DefaultPackingThreshold = 40 << 20

	// DefaultPackingLeewayPct allows some leeway when packing objects to avoid
	// waiting too long for the perfect combination of objects to fill a slab.
	DefaultPackingLeewayPct = 0.1 // allow 10% leeway when packing

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

func (s *Sia) passesPackingThreshold(size int64) bool {
	return size >= int64(float64(s.packingThreshold)*(1-s.packingLeewayPct))
}

func (s *Sia) needsPacking(size int64) bool {
	return s.packingThreshold > 0 && size < s.packingThreshold
}

func (s *Sia) packLoop(ctx context.Context) {
	s.logger.Info("pack loop started")

	for ctx.Err() == nil {
		// fetch objects for packing
		objs, err := s.store.ObjectsForPacking()
		if err != nil {
			s.logger.Error("failed to fetch objects for packing", zap.Error(err))
			return
		}

		// ensure we have enough data to pack
		var totalSize int64
		for _, obj := range objs {
			totalSize += obj.Length
		}
		if !s.passesPackingThreshold(totalSize) {
			s.logger.Debug("not enough data to pack",
				zap.Int("objects", len(objs)),
				zap.Int64("totalSize", totalSize),
				zap.Float64("leewayPct", s.packingLeewayPct),
				zap.Int64("threshold", s.packingThreshold))
			break
		}

		// pack a slab
		s.logger.Info("packing slab",
			zap.Int("candidates", len(objs)),
			zap.Int64("totalSize", totalSize))

		if packed, err := s.packSlab(ctx, objs); err != nil {
			s.logger.Error("failed to pack slab", zap.Error(err))
			return
		} else if !packed {
			s.logger.Debug("no objects were packed into the slab")
			break
		}
	}
}

func (s *Sia) packSlab(ctx context.Context, candidates []objects.PackedObject) (bool, error) {
	// initiate upload
	upload, err := s.sdk.UploadPacked()
	if err != nil {
		return false, fmt.Errorf("failed to create packed upload: %w", err)
	}
	defer upload.Close()

	// loop through candidates and add them to the upload
	var packed []objects.PackedObject
	for _, obj := range candidates {
		if obj.Length > upload.Remaining() {
			continue
		}

		f, err := os.Open(filepath.Join(s.packingDir, obj.Filename))
		if err != nil {
			s.logger.Warn("failed to open file for packing",
				zap.String("filename", obj.Filename),
				zap.Error(err))
			continue
		}
		_, err = upload.Add(ctx, f)
		f.Close()
		if err != nil {
			return false, fmt.Errorf("failed to add object to packed upload: %w", err)
		}
		packed = append(packed, obj)
	}

	// skip finalizing if the upload doesn't match the packing threshold
	if len(packed) == 0 {
		s.logger.Debug("no objects could be packed into the slab")
		return false, nil
	} else if !s.passesPackingThreshold(upload.Length()) {
		s.logger.Debug("packing skipped, upload does not exceed leeway threshold",
			zap.Int("objects", len(packed)),
			zap.Int64("uploadSize", upload.Length()),
			zap.Float64("leewayPct", s.packingLeewayPct),
			zap.Int64("threshold", s.packingThreshold))
		return false, nil
	}

	s.logger.Info("finalizing packed upload",
		zap.Int("objects", len(packed)),
		zap.Int64("size", upload.Length()))

	// finalize the upload
	results, err := upload.Finalize(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to finalize packed upload: %w", err)
	} else if len(results) != len(packed) {
		return false, fmt.Errorf("finalize returned %d results for %d objects", len(results), len(packed))
	}

	// pin the objects and finalize them in the store
	for i, obj := range packed {
		siaObj := results[i]
		if err := s.sdk.PinObject(ctx, siaObj); err != nil {
			s.logger.Error("failed to pin packed object",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.Error(err))
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

	return true, nil
}

func (s *Sia) tryPack(filename *string) {
	if filename == nil {
		return
	}

	s.packingMu.Lock()
	if s.packingRunning {
		s.packingMu.Unlock()
		return
	}
	s.packingRunning = true
	s.packingMu.Unlock()

	ctx, cancel, err := s.tg.AddContext(context.Background())
	if err != nil {
		s.packingMu.Lock()
		s.packingRunning = false
		s.packingMu.Unlock()
		return
	}

	go func() {
		defer cancel()
		defer func() {
			s.packingMu.Lock()
			s.packingRunning = false
			s.packingMu.Unlock()
		}()
		s.packLoop(ctx)
	}()
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

func randFilename(ext string) string {
	var uuid [8]byte
	frand.Read(uuid[:])
	return fmt.Sprintf("%x.%s", uuid[:], ext)
}
