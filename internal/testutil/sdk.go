package testutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"unsafe"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"lukechampine.com/frand"
)

type (
	// MemorySDK is an in-memory implementation of the sia.SDK interface for
	// testing the Sia backend without requiring a full indexer.
	MemorySDK struct {
		mu       sync.Mutex
		appKey   types.PrivateKey
		objects  map[types.Hash256]uploadedObject
		events   []sdk.ObjectEvent
		slabSize int64

		pruneSlabsCalls int

		pinErr      error // when non-nil, PinObject returns this error
		pinAttempts int   // number of PinObject calls observed
	}

	uploadedObject struct {
		data []byte
		meta sdk.Object
	}

	memoryPackedUpload struct {
		sdk *MemorySDK

		// guarded by sdk.mu
		objects []uploadedObject
		length  int64
	}
)

// NewMemorySDK creates a new in-memory SDK for testing.
func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		slabSize: 40 << 20,
		appKey:   types.GeneratePrivateKey(),
		objects:  make(map[types.Hash256]uploadedObject),
	}
}

// DeleteObject deletes the object with the given key.
func (s *MemorySDK) DeleteObject(_ context.Context, id types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, id)
	return nil
}

// Download downloads an object.
func (s *MemorySDK) Download(obj sdk.Object, rnge *s3.ObjectRange) (io.ReadCloser, error) {
	s.mu.Lock()
	uploaded, exists := s.objects[obj.ID()]
	s.mu.Unlock()
	if !exists {
		return nil, errors.New("download failed: object not found")
	}
	data := uploaded.data
	if rnge != nil {
		if rnge.Start+rnge.Length > int64(len(data)) {
			return nil, fmt.Errorf("download failed: range %d-%d exceeds object size %d", rnge.Start, rnge.Start+rnge.Length, len(data))
		}
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// SetEvents replaces the events returned by ObjectEvents.
func (s *MemorySDK) SetEvents(events []sdk.ObjectEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = events
}

// ObjectEvents returns object events starting from the given cursor, up to the
// given limit.
func (s *MemorySDK) ObjectEvents(_ context.Context, cursor slabs.Cursor, limit int) ([]sdk.ObjectEvent, error) {
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

// PruneSlabs prunes slabs not associated with an object from the indexer.
func (s *MemorySDK) PruneSlabs(_ context.Context, opts ...api.URLQueryParameterOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneSlabsCalls++
	return nil
}

// PruneSlabsCalls returns the number of times PruneSlabs has been invoked.
func (s *MemorySDK) PruneSlabsCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruneSlabsCalls
}

// ObjectCount returns the number of objects stored in the SDK.
func (s *MemorySDK) ObjectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

// Upload stores an object in memory. It is not part of the SDK interface but
// used by tests to simulate the background upload to Sia.
func (s *MemorySDK) Upload(_ context.Context, r io.Reader) (sdk.Object, error) {
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
func (s *MemorySDK) SetSlabSize(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slabSize = size
}

// OptimalDataSize returns the optimal data size.
func (s *MemorySDK) OptimalDataSize() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.slabSize, nil
}

// UploadPacked creates a new packed upload.
func (s *MemorySDK) UploadPacked() (sia.PackedUpload, error) {
	return &memoryPackedUpload{sdk: s}, nil
}

// PinObject pins the given object.
func (s *MemorySDK) PinObject(_ context.Context, obj sdk.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinAttempts++
	return s.pinErr
}

// SetPinError configures the error returned by future PinObject calls; pass
// nil to restore the default no-op behavior.
func (s *MemorySDK) SetPinError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinErr = err
}

// PinAttempts returns the number of times PinObject has been called.
func (s *MemorySDK) PinAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pinAttempts
}

// SealObject seals the object using the app key.
func (s *MemorySDK) SealObject(obj sdk.Object) sdk.SealedObject {
	return obj.Seal(s.appKey)
}

// UnsealObject unseals an object using the app key.
func (s *MemorySDK) UnsealObject(sealed sdk.SealedObject) (sdk.Object, error) {
	return sealed.Open(s.appKey)
}

func (u *memoryPackedUpload) Add(_ context.Context, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	obj := newTestObject()
	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()
	u.objects = append(u.objects, uploadedObject{
		data: data,
		meta: obj,
	})
	u.length += int64(len(data))
	return int64(len(data)), nil
}

func (u *memoryPackedUpload) Length() int64 {
	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()
	return u.length
}

func (u *memoryPackedUpload) Remaining() int64 {
	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()
	return u.sdk.slabSize - (u.length % u.sdk.slabSize)
}

func (u *memoryPackedUpload) Finalize(_ context.Context) ([]sdk.Object, error) {
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

func newTestObject() sdk.Object {
	obj := sdk.NewEmptyObject()
	ss := []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: 1}}
	v := reflect.ValueOf(&obj).Elem()
	f := v.FieldByName("slabs")
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(ss))
	return obj
}
