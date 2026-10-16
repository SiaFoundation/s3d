package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"lukechampine.com/frand"
)

type (
	// MemorySDK is an in-memory implementation of the sia.SDK interface for
	// testing the Sia backend without requiring a full indexer.
	//
	// An Object cannot be built from Go, and every empty one hashes to the
	// same ID, so an object carries no identity a fake can use. This type
	// therefore mints blank objects and remembers which one each stands for,
	// keyed on the pointer. The sealed form is a plain
	// [sdk.SealedObject] made up here rather than real cryptography; nothing
	// in these tests verifies a signature.
	MemorySDK struct {
		mu          sync.Mutex
		objects     map[types.Hash256]*uploadedObject
		staged      map[types.Hash256]*uploadedObject
		handles     map[*sdk.Object]types.Hash256
		events      []sdk.ObjectEvent
		eventsErr   error // when set, ObjectEvents returns this error
		slabSize    int64
		failUploads bool

		accountErr       error
		pruneSlabsCalls  int
		remainingStorage uint64

		pinErr      error                 // when non-nil, PinObject returns this error
		pinAttempts int                   // number of PinObject calls observed
		pinHook     func(obj *sdk.Object) // when non-nil, PinObject runs this after a successful pin
	}

	uploadedObject struct {
		data []byte
		// slabs fix the object's ID; sealing keeps them and re-randomizes
		// everything else, the way a real seal produces a fresh ciphertext for
		// the same object.
		slabs    []slabs.SlabSlice
		metadata []byte
	}

	memoryPackedUpload struct {
		sdk *MemorySDK

		// guarded by sdk.mu
		objects []*uploadedObject
		length  int64
	}
)

// errObjectNotFound stands in for the indexer not knowing an object.
var errObjectNotFound = errors.New("object not found")

// NewMemorySDK creates a new in-memory SDK for testing.
func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		slabSize:         40 << 20,
		objects:          make(map[types.Hash256]*uploadedObject),
		staged:           make(map[types.Hash256]*uploadedObject),
		handles:          make(map[*sdk.Object]types.Hash256),
		remainingStorage: math.MaxUint64,
	}
}

// newObject returns an object with a unique ID and no data.
func newObject() *uploadedObject {
	return &uploadedObject{
		slabs: []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: 1}},
	}
}

// id returns the object's ID, which the sealed form shares.
func (o *uploadedObject) id() types.Hash256 {
	return slabs.ObjectID(o.slabs)
}

// seal returns the object's sealed form. The encrypted fields are random, so
// two seals of the same object differ while sharing an ID, as they do when the
// real SDK re-encrypts under a fresh nonce.
func (o *uploadedObject) seal() sdk.SealedObject {
	now := time.Now().UTC()
	return sdk.SealedObject{
		EncryptedDataKey:  frand.Bytes(32),
		Slabs:             o.slabs,
		EncryptedMetadata: o.metadata,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

// handle returns a new SDK object standing for the uploaded one, carrying its
// metadata the way one from the real SDK does.
//
// s.mu must be held.
func (s *MemorySDK) handle(obj *uploadedObject) *sdk.Object {
	h := sdk.NewEmptyObject().WithMetadata(obj.metadata)
	s.handles[h] = obj.id()
	return h
}

// lookup resolves an issued object to the uploaded one it stands for.
//
// s.mu must be held.
func (s *MemorySDK) lookup(obj *sdk.Object) (*uploadedObject, error) {
	id, ok := s.handles[obj]
	if !ok {
		return nil, fmt.Errorf("%w: not issued by this SDK", errObjectNotFound)
	}
	uploaded, ok := s.objects[id]
	if !ok {
		// an object that has been uploaded but not yet pinned is still
		// readable, the way the real SDK can seal one before it is pinned
		if uploaded, ok = s.staged[id]; !ok {
			return nil, fmt.Errorf("%w: %v", errObjectNotFound, id)
		}
	}
	return uploaded, nil
}

// Account returns the account info, including the remaining storage.
func (s *MemorySDK) Account(_ context.Context) (sdk.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accountErr != nil {
		return sdk.Account{}, s.accountErr
	}
	return sdk.Account{
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

// DeleteObject deletes the object with the given key.
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
func (s *MemorySDK) Download(_ context.Context, obj *sdk.Object, rnge *s3.ObjectRange) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uploaded, err := s.lookup(obj)
	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
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

	if s.eventsErr != nil {
		return nil, s.eventsErr
	}

	sorted := slices.Clone(s.events)
	slices.SortFunc(sorted, func(a, b sdk.ObjectEvent) int {
		if c := a.UpdatedAt.Compare(b.UpdatedAt); c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})

	var filtered []sdk.ObjectEvent
	for _, ev := range sorted {
		after := ev.UpdatedAt.After(cursor.After) ||
			(ev.UpdatedAt.Equal(cursor.After) && bytes.Compare(ev.ID[:], cursor.Key[:]) > 0)
		if !after {
			continue
		}
		// Every object this SDK issues has to be registered, since a blank
		// one carries no identity of its own.
		if ev.Object != nil {
			s.handles[ev.Object] = ev.ID
		}
		filtered = append(filtered, ev)
		if len(filtered) >= limit {
			break
		}
	}
	return filtered, nil
}

// PruneSlabs prunes slabs not associated with an object from the indexer.
func (s *MemorySDK) PruneSlabs(_ context.Context, _ time.Time) error {
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

// Upload stores an object in memory, staged rather than pinned, the way the
// real SDK leaves an uploaded object until PinObject.
func (s *MemorySDK) Upload(_ context.Context, obj *sdk.Object, r io.Reader, _ ...sdk.UploadOption) (*sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	uploaded := newObject()
	uploaded.data = data
	if obj != nil {
		uploaded.metadata = obj.Metadata()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.staged[uploaded.id()] = uploaded
	return s.handle(uploaded), nil
}

// AddObject uploads data as a fresh object, stores it as if pinned, and
// returns it. It is a test convenience for seeding objects, not part of the
// SDK interface.
func (s *MemorySDK) AddObject(ctx context.Context, r io.Reader) (*sdk.Object, error) {
	h, err := s.Upload(ctx, sdk.NewEmptyObject(), r)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.handles[h]
	if uploaded, ok := s.staged[id]; ok {
		s.objects[id] = uploaded
		delete(s.staged, id)
	}
	return h, nil
}

// ObjectID reports the id of an object this SDK issued, for a test that needs
// to look one up later. It is a test convenience, not part of the SDK
// interface: blank objects all hash to the same id, so the mapping kept here
// is the only identity one has.
func (s *MemorySDK) ObjectID(obj *sdk.Object) types.Hash256 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handles[obj]
}

// Pinned reports whether the object with the given id is still stored in the
// SDK.
func (s *MemorySDK) Pinned(id types.Hash256) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[id]
	return ok
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
	return o.metadata, true
}

// SetSlabSize overrides the slab size for testing.
func (s *MemorySDK) SetSlabSize(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slabSize = size
}

// OptimalDataSize returns the optimal data size.
func (s *MemorySDK) OptimalDataSize(_ context.Context) (int64, error) {
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
func (s *MemorySDK) UploadPacked(_ context.Context, _ ...sdk.UploadOption) (sia.PackedUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failUploads {
		return nil, errors.New("upload failed")
	}
	return &memoryPackedUpload{sdk: s}, nil
}

// PinObject pins the given object.
func (s *MemorySDK) PinObject(_ context.Context, obj *sdk.Object) error {
	s.mu.Lock()
	s.pinAttempts++
	if err := s.pinErr; err != nil {
		s.mu.Unlock()
		return err
	}
	id, ok := s.handles[obj]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("pin failed: %w: not issued by this SDK", errObjectNotFound)
	}
	if uploaded, ok := s.staged[id]; ok {
		s.objects[id] = uploaded
		delete(s.staged, id)
	}
	hook := s.pinHook
	s.mu.Unlock()

	// the hook runs unlocked so it can call back into the SDK
	if hook != nil {
		hook(obj)
	}
	return nil
}

// SetPinHook configures a callback PinObject runs after every successful pin.
// Pass nil to remove it.
func (s *MemorySDK) SetPinHook(fn func(obj *sdk.Object)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinHook = fn
}

// SetEventsError configures the error returned by future ObjectEvents calls.
// Pass nil to restore the default behavior.
func (s *MemorySDK) SetEventsError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventsErr = err
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

// SealObject seals the uploaded object this one stands for, picking up whatever
// metadata the caller set on it.
func (s *MemorySDK) SealObject(obj *sdk.Object) (sdk.SealedObject, error) {
	metadata := obj.Metadata()

	s.mu.Lock()
	defer s.mu.Unlock()
	uploaded, err := s.lookup(obj)
	if err != nil {
		return sdk.SealedObject{}, fmt.Errorf("seal failed: %w", err)
	}
	uploaded.metadata = metadata
	return uploaded.seal(), nil
}

// UnsealObject returns the object the sealed form stands for.
func (s *MemorySDK) UnsealObject(sealed sdk.SealedObject) (*sdk.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := sealed.ID()
	uploaded, ok := s.objects[id]
	if !ok {
		// the real ObjectFromSealed runs locally and does not check pin
		// state, so a staged object unseals the same as a pinned one
		if uploaded, ok = s.staged[id]; !ok {
			return nil, fmt.Errorf("unseal failed: %w: %v", errObjectNotFound, id)
		}
	}
	return s.handle(uploaded), nil
}

func (u *memoryPackedUpload) Add(r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	obj := newObject()
	obj.data = data

	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()
	u.objects = append(u.objects, obj)
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

func (u *memoryPackedUpload) Finalize() ([]*sdk.Object, error) {
	u.sdk.mu.Lock()
	defer u.sdk.mu.Unlock()

	var results []*sdk.Object
	for _, obj := range u.objects {
		u.sdk.objects[obj.id()] = obj
		results = append(results, u.sdk.handle(obj))
	}
	u.objects = nil
	return results, nil
}

func (u *memoryPackedUpload) Close() error { return nil }
