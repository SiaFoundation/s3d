package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"lukechampine.com/frand"
)

type (
	// MemorySDK is an in-memory implementation of the sia.SDK interface for
	// testing the Sia backend without requiring a full indexer.
	MemorySDK struct {
		mu          sync.Mutex
		appKey      types.PrivateKey
		objects     map[types.Hash256]uploadedObject
		staged      map[types.Hash256]uploadedObject
		events      []sdk.ObjectEvent
		eventsErr   error // when set, ObjectEvents returns this error
		objectErr   error // when set, Object returns this error
		slabSize    int64
		failUploads bool

		accountErr       error
		pruneSlabsCalls  int
		remainingStorage uint64

		pinErr      error // when non-nil, PinObject returns this error
		pinAttempts int   // number of PinObject calls observed
		pinPublish  bool  // when set, PinObject appends the pinned object's event

		objectEventCalls int // number of ObjectEvents calls observed
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
		slabSize:         40 << 20,
		appKey:           types.GeneratePrivateKey(),
		objects:          make(map[types.Hash256]uploadedObject),
		staged:           make(map[types.Hash256]uploadedObject),
		remainingStorage: math.MaxUint64,
	}
}

// Account returns the account info, including the remaining storage.
func (s *MemorySDK) Account(_ context.Context) (app.AccountResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accountErr != nil {
		return app.AccountResponse{}, s.accountErr
	}
	return app.AccountResponse{
		RemainingStorage: s.remainingStorage,
	}, nil
}

// SetAccountErr makes Account return the given error until cleared.
func (s *MemorySDK) SetAccountErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accountErr = err
}

// SetRemainingStorage overrides the remaining storage returned by Account.
func (s *MemorySDK) SetRemainingStorage(remaining uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remainingStorage = remaining
}

// DeleteObject deletes the object with the given key. Deleting an unknown
// object reports the indexer's not found sentinel.
func (s *MemorySDK) DeleteObject(_ context.Context, id types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[id]; !ok {
		return slabs.ErrObjectNotFound
	}
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

// SetEventsError sets the error returned by ObjectEvents.
func (s *MemorySDK) SetEventsError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventsErr = err
}

// ObjectEvents returns object events starting from the given cursor, up to the
// given limit.
func (s *MemorySDK) ObjectEvents(_ context.Context, cursor slabs.Cursor, limit int) ([]sdk.ObjectEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objectEventCalls++
	if s.eventsErr != nil {
		return nil, s.eventsErr
	}

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

// Object returns the object with the given id, mirroring the indexer's point
// read of its committed store.
func (s *MemorySDK) Object(_ context.Context, id types.Hash256) (sdk.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objectErr != nil {
		return sdk.Object{}, s.objectErr
	}
	o, ok := s.objects[id]
	if !ok {
		return sdk.Object{}, fmt.Errorf("failed to get object: %w", slabs.ErrObjectNotFound)
	}
	return o.meta, nil
}

// SetObjectError makes Object return the given error until cleared.
func (s *MemorySDK) SetObjectError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objectErr = err
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

// Pinned reports whether the object with the given id is still stored in the SDK.
func (s *MemorySDK) Pinned(id types.Hash256) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[id]
	return ok
}

// Upload stages the object's data in memory keyed by its ID and records its
// metadata. Like the production SDK the object is only stored once it is
// pinned. It implements the sia.SDK interface.
func (s *MemorySDK) Upload(_ context.Context, obj *sdk.Object, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// give the object a slab so its ID is content-derived and unique
	setSlabs(obj, []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: uint32(len(data))}})
	s.staged[obj.ID()] = uploadedObject{data: data, meta: *obj}
	return nil
}

// AddObject uploads data as a fresh object, stores it as if pinned, and
// returns it. It is a test convenience for seeding objects, not part of the
// SDK interface.
func (s *MemorySDK) AddObject(ctx context.Context, r io.Reader) (sdk.Object, error) {
	obj := sdk.NewEmptyObject()
	if err := s.Upload(ctx, &obj, r); err != nil {
		return sdk.Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[obj.ID()] = s.staged[obj.ID()]
	delete(s.staged, obj.ID())
	return obj, nil
}

// ObjectData returns the uploaded data for an object.
func (s *MemorySDK) ObjectData(id types.Hash256) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[id]
	if !ok {
		return nil, false
	}
	return o.data, true
}

// ObjectMetadata returns the metadata recorded for an uploaded object.
func (s *MemorySDK) ObjectMetadata(id types.Hash256) (json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[id]
	if !ok {
		return nil, false
	}
	return o.meta.Metadata(), true
}

// ObjectEventCalls returns how many times the event stream was enumerated.
// Fetching a snapshot by id must not enumerate at all.
func (s *MemorySDK) ObjectEventCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.objectEventCalls
}

// StoredObject returns the stored object for an id, carrying the slabs and
// metadata a real object event would. It is a test accessor, distinct from the
// SDK's Object method.
func (s *MemorySDK) StoredObject(id types.Hash256) (sdk.Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[id]
	return o.meta, ok
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

// SetFailUploads controls whether UploadPacked returns an error.
func (s *MemorySDK) SetFailUploads(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failUploads = fail
}

// UploadPacked creates a new packed upload.
func (s *MemorySDK) UploadPacked() (sia.PackedUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failUploads {
		return nil, errors.New("upload failed")
	}
	return &memoryPackedUpload{sdk: s}, nil
}

// PinObject pins the given object, storing a staged upload.
func (s *MemorySDK) PinObject(_ context.Context, obj sdk.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinAttempts++
	if s.pinErr != nil {
		return s.pinErr
	}
	if o, ok := s.staged[obj.ID()]; ok {
		s.objects[obj.ID()] = o
		delete(s.staged, obj.ID())
		if s.pinPublish {
			meta := o.meta
			s.events = append(s.events, sdk.ObjectEvent{Key: obj.ID(), UpdatedAt: time.Now(), Object: &meta})
		}
	}
	return nil
}

// SetPublishOnPin makes PinObject append the pinned object's event to the event
// stream, mirroring an indexer that publishes events after the pin commits.
func (s *MemorySDK) SetPublishOnPin(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinPublish = v
}

// SetPinError configures the error returned by future PinObject calls. Pass
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
	setSlabs(&obj, []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: 1}})
	return obj
}

func setSlabs(obj *sdk.Object, ss []slabs.SlabSlice) {
	v := reflect.ValueOf(obj).Elem()
	f := v.FieldByName("slabs")
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(ss))
}
