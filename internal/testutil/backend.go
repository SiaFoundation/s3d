package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
)

// UploadedObject is an object stored in the in-memory SDK.
type UploadedObject struct {
	Data []byte
	Meta sdk.Object
}

// MemorySDK is an in-memory [sia.SDK] for testing.
type MemorySDK struct {
	AppKey  types.PrivateKey
	Objects map[types.Hash256]UploadedObject

	ObjectCallCount int
	Fail            bool // when true, Object() returns an error
}

// NewMemorySDK creates a new MemorySDK.
func NewMemorySDK() *MemorySDK {
	return &MemorySDK{
		AppKey:  types.GeneratePrivateKey(),
		Objects: make(map[types.Hash256]UploadedObject),
	}
}

func (s *MemorySDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	delete(s.Objects, id)
	return nil
}

func (s *MemorySDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	uploaded, exists := s.Objects[obj.ID()]
	if !exists {
		return errors.New("download failed - object not found")
	}
	data := uploaded.Data
	if rnge != nil {
		if rnge.Start+rnge.Length > int64(len(data)) {
			return fmt.Errorf("download failed - range %d-%d exceeds object size %d", rnge.Start, rnge.Start+rnge.Length, len(data))
		}
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}
	_, err := w.Write(data)
	return err
}

func (s *MemorySDK) Object(ctx context.Context, objectID types.Hash256) (sdk.Object, error) {
	s.ObjectCallCount++
	if s.Fail {
		return sdk.Object{}, errors.New("indexer error")
	}
	obj, exists := s.Objects[objectID]
	if !exists {
		return sdk.Object{}, errors.New("object not found")
	}
	return obj.Meta, nil
}

func (s *MemorySDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return sdk.Object{}, err
	}
	obj := sdk.Object{}
	s.Objects[obj.ID()] = UploadedObject{
		Data: data,
		Meta: obj,
	}
	return obj, nil
}

func (s *MemorySDK) SealObject(obj sdk.Object) slabs.SealedObject {
	return obj.Seal(s.AppKey).SealedObject
}

func (s *MemorySDK) UnsealObject(sealed slabs.SealedObject) (sdk.Object, error) {
	obj, exists := s.Objects[sealed.ID()]
	if !exists {
		return sdk.Object{}, errors.New("object not found")
	}
	return obj.Meta, nil
}

func NewBackend(tb testing.TB) *sia.Sia {
	tb.Helper()
	dir := tb.TempDir()
	log := zaptest.NewLogger(tb)
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { store.Close() })

	backend, err := sia.New(tb.Context(), NewMemorySDK(), store, dir,
		sia.WithKeyPair(AccessKeyID, SecretAccessKey),
		sia.WithLogger(log))
	if err != nil {
		tb.Fatal(err)
	}
	return backend
}
