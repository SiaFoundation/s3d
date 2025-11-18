package sia_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type uploadedObject struct {
	meta sdk.Object
	data []byte
}

type MemorySDK struct {
	appKey  types.PrivateKey
	objects map[types.Hash256]uploadedObject
	pinned  map[types.Hash256]struct{}
}

func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		appKey:  types.GeneratePrivateKey(),
		objects: make(map[types.Hash256]uploadedObject),
		pinned:  make(map[types.Hash256]struct{}),
	}
}

func (s *MemorySDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, opts ...sdk.DownloadOption) error {
	uploaded, exists := s.objects[obj.ID()]
	if !exists {
		return errors.New("download failed - object not found")
	}
	_, err := w.Write(uploaded.data)
	return err
}

func (s *MemorySDK) SealObject(obj sdk.Object) slabs.SealedObject {
	return obj.Seal(s.appKey)
}

func (s *MemorySDK) PinObject(ctx context.Context, obj sdk.Object) error {
	s.pinned[obj.ID()] = struct{}{}
	return nil
}

func (s *MemorySDK) Upload(ctx context.Context, r io.Reader, opts ...sdk.UploadOption) (sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return sdk.Object{}, err
	}
	obj := s.newObject(len(data))
	s.objects[obj.ID()] = uploadedObject{
		data: data,
		meta: obj,
	}
	return obj, nil
}

func (s *MemorySDK) newObject(size int) sdk.Object {
	// single slice that adds up to size
	slices := []slabs.SlabSlice{
		{
			SlabID: frand.Entropy256(),
			Offset: 0,
			Length: uint32(size),
		},
	}
	return sdk.NewObject(slices, []byte{})
}

func NewTester(t testing.TB, opts ...testutil.TesterOption) *testutil.S3Tester {
	log := zaptest.NewLogger(t)

	// use in-memory SDK
	sdk := NewMemorySDK()

	// use SQLite store
	store, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	backend, err := sia.New(context.Background(), sdk, store, testutil.AccessKeyID, testutil.SecretAccessKey,
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	var mergedOpts []testutil.TesterOption
	mergedOpts = append(mergedOpts, testutil.WithBackend(backend))
	mergedOpts = append(mergedOpts, opts...)
	return testutil.NewTester(t, mergedOpts...)
}
