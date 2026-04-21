package sia_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type uploadedObject struct {
	data []byte
	meta sdk.Object
}

type MemorySDK struct {
	mu       sync.Mutex
	appKey   types.PrivateKey
	objects  map[types.Hash256]uploadedObject
	slabSize int64

	objectCallCount int
	fail            bool // when true, Object() will return an error
}

func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		slabSize: 40 << 20,
		appKey:   types.GeneratePrivateKey(),
		objects:  make(map[types.Hash256]uploadedObject),
	}
}

func (s *MemorySDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, id)
	return nil
}

func (s *MemorySDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	s.mu.Lock()
	uploaded, exists := s.objects[obj.ID()]
	s.mu.Unlock()
	if !exists {
		return errors.New("download failed - object not found")
	}
	data := uploaded.data
	if rnge != nil {
		if rnge.Start+rnge.Length > int64(len(data)) {
			return fmt.Errorf("download failed - range %d-%d exceeds object size %d", rnge.Start, rnge.Start+rnge.Length, len(data))
		}
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}
	_, err := w.Write(data)
	return err
}

// TODO: Right now, all objects have the same ID. We'll need to expose something from
// the SDK to be able to mock objects with different IDs.
func (s *MemorySDK) Object(ctx context.Context, objectID types.Hash256) (sdk.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objectCallCount++
	if s.fail {
		return sdk.Object{}, errors.New("indexer error")
	}
	obj, exists := s.objects[objectID]
	if !exists {
		return sdk.Object{}, errors.New("object not found")
	}
	return obj.meta, nil
}

func (s *MemorySDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return sdk.Object{}, err
	}
	obj := sdk.NewEmptyObject()
	s.mu.Lock()
	s.objects[obj.ID()] = uploadedObject{
		data: data,
		meta: obj,
	}
	s.mu.Unlock()
	return obj, nil
}

func (s *MemorySDK) SlabSize() (int64, error) {
	return s.slabSize, nil
}

func (s *MemorySDK) UploadPacked() (sia.PackedUpload, error) {
	return &memoryPackedUpload{sdk: s}, nil
}

func (s *MemorySDK) PinObject(ctx context.Context, obj sdk.Object) error {
	return nil
}

type memoryPackedUpload struct {
	sdk     *MemorySDK
	objects []uploadedObject
	length  int64
}

func (u *memoryPackedUpload) Add(ctx context.Context, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	obj := sdk.NewEmptyObject()
	u.objects = append(u.objects, uploadedObject{
		data: data,
		meta: obj,
	})
	u.length += int64(len(data))
	return int64(len(data)), nil
}

func (u *memoryPackedUpload) Length() int64    { return u.length }
func (u *memoryPackedUpload) Remaining() int64 { return u.sdk.slabSize - (u.length % u.sdk.slabSize) }

func (u *memoryPackedUpload) Finalize(ctx context.Context) ([]sdk.Object, error) {
	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()

	var results []sdk.Object
	for _, obj := range u.objects {
		u.sdk.objects[obj.meta.ID()] = obj
		results = append(results, obj.meta)
	}
	u.objects = nil
	return results, nil
}

func (u *memoryPackedUpload) Close() error { return nil }

func (s *MemorySDK) SealObject(obj sdk.Object) slabs.SealedObject {
	return obj.Seal(s.appKey).SealedObject
}

func (s *MemorySDK) UnsealObject(sealed slabs.SealedObject) (sdk.Object, error) {
	obj, exists := s.objects[sealed.ID()]
	if !exists {
		return sdk.Object{}, errors.New("object not found")
	}
	return obj.meta, nil
}

func NewTester(t testing.TB, opts ...testutil.TesterOption) *testutil.S3Tester {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	// use in-memory SDK
	sdk := NewMemorySDK()

	// use SQLite store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	return NewCustomTester(t, dir, store, sdk, log, opts...)
}

func NewCustomTester(t testing.TB, dir string, store sia.Store, sdk sia.SDK, log *zap.Logger, opts ...testutil.TesterOption) *testutil.S3Tester {
	backend, err := sia.New(context.Background(), sdk, store, dir,
		sia.WithPackingWaste(0),
		sia.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })

	var mergedOpts []testutil.TesterOption
	mergedOpts = append(mergedOpts, testutil.WithBackend(backend))
	mergedOpts = append(mergedOpts, opts...)
	return testutil.NewTester(t, mergedOpts...)
}
