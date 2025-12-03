package sia_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap/zaptest"
)

type uploadedObject struct {
	meta sdk.Object
	data []byte
}

type MemorySDK struct {
	appKey  types.PrivateKey
	objects map[types.Hash256]uploadedObject
}

func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		appKey:  types.GeneratePrivateKey(),
		objects: make(map[types.Hash256]uploadedObject),
	}
}

func (s *MemorySDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	uploaded, exists := s.objects[obj.ID()]
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

func (s *MemorySDK) Object(ctx context.Context, objectKey types.Hash256) (sdk.Object, error) {
	uploaded, exists := s.objects[objectKey]
	if !exists {
		return sdk.Object{}, errors.New("object not found")
	}
	return uploaded.meta, nil
}

// TODO: Right now, all objects have the same ID. We'll need to expose something from
// the SDK to be able to mock objects with different IDs.
func (s *MemorySDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return sdk.Object{}, err
	}
	obj := sdk.Object{}
	s.objects[obj.ID()] = uploadedObject{
		data: data,
		meta: sdk.Object{},
	}
	return obj, nil
}

func NewTester(t testing.TB, opts ...testutil.TesterOption) *testutil.S3Tester {
	return NewCustomTester(t, t.TempDir(), opts...)
}

func NewCustomTester(t testing.TB, dir string, opts ...testutil.TesterOption) *testutil.S3Tester {
	log := zaptest.NewLogger(t)

	// use in-memory SDK
	sdk := NewMemorySDK()

	// use SQLite store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	backend, err := sia.New(context.Background(), sdk, store, dir, testutil.AccessKeyID, testutil.SecretAccessKey,
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	var mergedOpts []testutil.TesterOption
	mergedOpts = append(mergedOpts, testutil.WithBackend(backend))
	mergedOpts = append(mergedOpts, opts...)
	return testutil.NewTester(t, mergedOpts...)
}
