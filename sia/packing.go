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
	defer f.Close()
	defer func() {
		if err != nil {
			os.Remove(filePath)
		}
	}()

	if _, err = io.Copy(f, r); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	if err = f.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	// sync parent directory
	dir, err := os.Open(s.packingDir)
	if err != nil {
		return "", fmt.Errorf("failed to open packing directory: %w", err)
	}
	if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return "", fmt.Errorf("failed to sync packing directory: %w", err)
	}

	return filename, nil
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

		// check whether we haven enough to fill a slab
		var totalSize int64
		for _, obj := range objs {
			totalSize += obj.Length
		}
		if totalSize < s.packingThreshold {
			s.logger.Debug("not enough data to pack",
				zap.Int("objects", len(objs)),
				zap.Int64("totalSize", totalSize),
				zap.Int64("threshold", s.packingThreshold))
			break
		}

		// pack a slab
		s.logger.Info("packing slab",
			zap.Int("candidates", len(objs)),
			zap.Int64("totalSize", totalSize))

		if err := s.packSlab(ctx, objs); err != nil {
			s.logger.Error("failed to pack slab", zap.Error(err))
			return
		}
	}

	// cleanup orphaned files
	entries, err := os.ReadDir(s.packingDir)
	if err != nil {
		s.logger.Error("failed to read packing directory", zap.Error(err))
		return
	} else if len(entries) == 0 {
		return
	}

	objs, err := s.store.ObjectsForPacking()
	if err != nil {
		s.logger.Error("failed to fetch objects for orphan cleanup", zap.Error(err))
		return
	}

	referenced := make(map[string]struct{}, len(objs))
	for _, obj := range objs {
		referenced[obj.Filename] = struct{}{}
	}

	for _, entry := range entries {
		if _, ok := referenced[entry.Name()]; !ok {
			path := filepath.Join(s.packingDir, entry.Name())
			if err := os.Remove(path); err != nil {
				s.logger.Error("failed to remove orphaned file",
					zap.String("path", path),
					zap.Error(err))
			} else {
				s.logger.Info("removed orphaned packing file",
					zap.String("filename", entry.Name()))
			}
		}
	}
}

// packSlab expects candidates to be sorted by size descending for greedy
// best-fit packing.
func (s *Sia) packSlab(ctx context.Context, candidates []objects.PackedObject) error {
	// initiate upload
	upload, err := s.sdk.UploadPacked()
	if err != nil {
		return fmt.Errorf("failed to create packed upload: %w", err)
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
			return fmt.Errorf("failed to add object to packed upload: %w", err)
		}
		packed = append(packed, obj)

		if upload.Remaining() == 0 {
			break
		}
	}

	if len(packed) == 0 {
		return errors.New("no objects could be packed into the slab")
	}

	s.logger.Info("finalizing packed upload",
		zap.Int("objects", len(packed)),
		zap.Int64("slabUsage", upload.Length()))

	// finalize the upload
	results, err := upload.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("failed to finalize packed upload: %w", err)
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
			s.logger.Error("failed to finalize packed object in store",
				zap.String("bucket", obj.Bucket),
				zap.String("name", obj.Name),
				zap.Error(err))
			continue
		}

		s.tryRemove(&obj.Filename)
		s.logger.Debug("packed object uploaded to Sia",
			zap.String("bucket", obj.Bucket),
			zap.String("name", obj.Name))
	}

	return nil
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
