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

	packedUploadThreads  = 4
	packedUploadMaxSlabs = 4

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
		slabSize  int64
		totalSize int64
		objects   []objects.PackedObject
	}

	// pendingUpload contains a packed upload and the objects
	// that are being packed.
	pendingUpload struct {
		upload  PackedUpload
		objects []objects.PackedObject
	}
)

func (p *packedObjects) wastePct() float64 {
	if p.totalSize <= 0 {
		return 1
	}
	remainder := p.totalSize % p.slabSize
	if remainder == 0 {
		return 0
	}
	waste := p.slabSize - remainder
	return float64(waste) / float64(p.totalSize+waste)
}

func (p *packedObjects) remainingSpace() int64 {
	remainder := p.totalSize % p.slabSize
	if remainder == 0 {
		return p.slabSize
	}
	return p.slabSize - remainder
}

func (p *packedObjects) tryAdd(obj objects.PackedObject) bool {
	if p.totalSize+obj.Length > p.slabSize*packedUploadMaxSlabs {
		return false
	} else if obj.Length > p.remainingSpace() && obj.Length <= p.slabSize {
		return false
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
			added = groups[i].tryAdd(obj)
			if added {
				break
			}
		}
		if !added {
			groups = append(groups, packedObjects{
				slabSize:  s.slabSize,
				totalSize: obj.Length,
				objects:   []objects.PackedObject{obj},
			})
		}
	}

	// filter groups that meet the waste threshold
	var ready []packedObjects
	var remaining []objects.PackedObject
	for _, g := range groups {
		if s.meetsUploadThreshold(g.totalSize) {
			ready = append(ready, g)
		} else {
			remaining = append(remaining, g.objects...)
		}
	}

	// try and fill gaps with remaining objects
	for _, obj := range remaining {
		for i := range ready {
			if !s.meetsUploadThreshold(ready[i].totalSize + obj.Length) {
				continue
			}
			if ready[i].tryAdd(obj) {
				break
			}
		}
	}

	for _, g := range ready {
		s.logger.Info("created pack",
			zap.Int("objects", len(g.objects)),
			zap.Int64("size", g.totalSize),
			zap.Float64("wastePct", g.wastePct()))
	}

	return ready
}

// preparePendingUploads creates packed uploads for the given
// packs, reading files from disk into each upload. Packs
// whose files have been deleted concurrently are discarded.
func (s *Sia) preparePendingUploads(ctx context.Context, packs []packedObjects) (_ []pendingUpload, err error) {
	var uploads []pendingUpload

	// close uploads on error
	defer func() {
		if err != nil {
			for _, pu := range uploads {
				pu.upload.Close()
			}
		}
	}()

	// loop over packed objects and create uploads
	for _, pack := range packs {
		upload, err := s.sdk.UploadPacked()
		if err != nil {
			return nil, fmt.Errorf("failed to create packed upload: %w", err)
		}

		pu := pendingUpload{upload: upload}
		for _, obj := range pack.objects {
			if err := s.addToUpload(ctx, &pu, obj); err != nil {
				s.logger.Error("failed to add object to upload, aborting pack", zap.Error(err))
				pu.upload.Close()
				pu = pendingUpload{}
				break
			}
		}
		if pu.upload == nil {
			continue
		}

		// verify the upload still meets the threshold
		if !s.meetsUploadThreshold(pu.upload.Length()) {
			s.logger.Debug("discarding underfilled upload",
				zap.Int("objects", len(pu.objects)),
				zap.Int64("uploadSize", pu.upload.Length()),
				zap.Int64("slabSize", s.slabSize))
			pu.upload.Close()
			continue
		}

		uploads = append(uploads, pu)
	}

	return uploads, nil
}

// needsPacking returns true if uploading size bytes directly
// would waste more than the configured packing threshold.
func (s *Sia) needsPacking(size int64) bool {
	if s.slabSize <= 0 {
		return false
	}
	return !s.meetsUploadThreshold(size)
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
	packs := s.preparePackedObjects()
	if len(packs) == 0 {
		return
	}

	uploads, err := s.preparePendingUploads(ctx, packs)
	if err != nil {
		s.logger.Error("failed to create pending uploads", zap.Error(err))
		return
	} else if len(uploads) == 0 {
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

		s.tryRemovePackedObject(&obj.Filename)
		s.logger.Debug("packed object uploaded to Sia",
			zap.String("bucket", obj.Bucket),
			zap.String("name", obj.Name))
	}

	return nil
}

// meetsUploadThreshold returns true if size bytes can be
// uploaded without exceeding the configured waste threshold.
func (s *Sia) meetsUploadThreshold(size int64) bool {
	if size <= 0 {
		return false
	}
	remainder := size % s.slabSize
	if remainder == 0 {
		return true
	}

	waste := s.slabSize - remainder
	allocated := size + waste
	return float64(waste)/float64(allocated) < s.packingWastePct
}

func (s *Sia) triggerPacking() {
	select {
	case s.triggerPackChan <- struct{}{}:
	default:
	}
}

func (s *Sia) openPackedObject(obj objects.Object) (*os.File, error) {
	// nothing to do if the object is nil or it's not a packed object
	if obj.Filename == nil {
		return nil, nil
	}

	// open the file on disk, if it does not exist, it may have been
	// uploaded in the background, refresh the object and try again
	f, err := os.Open(filepath.Join(s.packingDir, *obj.Filename))
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
