package sia

import (
	"context"
	"io"
	"slices"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
)

type (
	// IndexdSDK is a wrapper around the indexd SDK to implement the SDK interface.
	IndexdSDK struct {
		inner *sdk.SDK

		dlOpts []sdk.DownloadOption
		ulOpts []sdk.UploadOption
	}

	// SDKOption is a configuration option for the IndexdSDK.
	SDKOption func(*IndexdSDK)
)

// WithDownloadOptions sets the download options for the IndexdSDK.
func WithDownloadOptions(opts ...sdk.DownloadOption) SDKOption {
	return func(s *IndexdSDK) {
		s.dlOpts = opts
	}
}

// WithUploadOptions sets the upload options for the IndexdSDK.
func WithUploadOptions(opts ...sdk.UploadOption) SDKOption {
	return func(s *IndexdSDK) {
		s.ulOpts = opts
	}
}

// NewSDK wraps an indexd SDK for use in s3d.
func NewSDK(sdk *sdk.SDK, opts ...SDKOption) *IndexdSDK {
	indexd := &IndexdSDK{
		inner: sdk,
	}
	for _, opt := range opts {
		opt(indexd)
	}
	return indexd
}

// Download downloads an object from indexd.
func (s *IndexdSDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	opts := slices.Clone(s.dlOpts)
	if rnge != nil {
		opts = append(opts, sdk.WithDownloadRange(uint64(rnge.Start), uint64(rnge.Length)))
	}
	return s.inner.Download(ctx, w, obj, opts...)
}

// SlabSize returns the slab size by creating a temporary packed upload and
// reading its capacity.
func (s *IndexdSDK) SlabSize() (int64, error) {
	pu, err := s.inner.UploadPacked(s.ulOpts...)
	if err != nil {
		return 0, err
	}
	defer pu.Close()
	return pu.SlabSize(), nil
}

// UploadPacked creates a new packed upload.
func (s *IndexdSDK) UploadPacked() (PackedUpload, error) {
	return s.inner.UploadPacked(s.ulOpts...)
}

// PinObject pins the given object in the indexer.
func (s *IndexdSDK) PinObject(ctx context.Context, obj sdk.Object) error {
	return s.inner.PinObject(ctx, obj)
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *IndexdSDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	return s.inner.DeleteObject(ctx, id)
}

// Object retrieves the object with the given key.
func (s *IndexdSDK) Object(ctx context.Context, objectKey types.Hash256) (sdk.Object, error) {
	return s.inner.Object(ctx, objectKey)
}

// SealObject seals the object using the app key.
func (s *IndexdSDK) SealObject(obj sdk.Object) slabs.SealedObject {
	return obj.Seal(s.inner.AppKey()).SealedObject
}

// UnsealObject unseals a sealed object using the app key.
func (s *IndexdSDK) UnsealObject(sealed slabs.SealedObject) (sdk.Object, error) {
	return (&sdk.SealedObject{SealedObject: sealed}).Open(s.inner.AppKey())
}
