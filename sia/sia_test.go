package sia_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"unsafe"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type uploadedObject struct {
	data []byte
	meta sdk.Object
}

type MemorySDK struct {
	mu       sync.Mutex
	appKey   types.PrivateKey
	objects  map[types.Hash256]uploadedObject
	events   []sdk.ObjectEvent
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

// SetEvents replaces the events returned by ObjectEvents.
func (s *MemorySDK) SetEvents(events []sdk.ObjectEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = events
}

func (s *MemorySDK) ObjectEvents(_ context.Context, cursor sdk.ObjectsCursor, limit int) ([]sdk.ObjectEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sorted := slices.Clone(s.events)
	slices.SortFunc(sorted, func(a, b sdk.ObjectEvent) int {
		if c := a.UpdatedAt.Compare(b.UpdatedAt); c != 0 {
			return c
		}
		return bytes.Compare(a.Key[:], b.Key[:])
	})

	var filtered []sdk.ObjectEvent
	for _, ev := range sorted {
		after := ev.UpdatedAt.After(cursor.After) ||
			(ev.UpdatedAt.Equal(cursor.After) && bytes.Compare(ev.Key[:], cursor.Key[:]) > 0)
		if !after {
			continue
		}
		filtered = append(filtered, ev)
		if len(filtered) >= limit {
			break
		}
	}
	return filtered, nil
}

// Upload stores an object in memory. It is not part of the SDK interface but
// used by tests to simulate the background upload to Sia.
func (s *MemorySDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return sdk.Object{}, err
	}
	obj := newTestObject()
	s.mu.Lock()
	s.objects[obj.ID()] = uploadedObject{
		data: data,
		meta: obj,
	}
	s.mu.Unlock()
	return obj, nil
}

// SetSlabSize overrides the slab size for testing.
func (s *MemorySDK) SetSlabSize(size int64) { s.slabSize = size }

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
	obj := newTestObject()
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

func (s *MemorySDK) SealObject(obj sdk.Object) sdk.SealedObject {
	return obj.Seal(s.appKey)
}

func (s *MemorySDK) UnsealObject(sealed sdk.SealedObject) (sdk.Object, error) {
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
	backend, err := sia.New(t.Context(), sdk, store, dir,
		sia.WithUploadDisabled(),
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

func newTestObject() sdk.Object {
	obj := sdk.NewEmptyObject()
	ss := []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: 1}}
	v := reflect.ValueOf(&obj).Elem()
	f := v.FieldByName("slabs")
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(ss))
	return obj
}
