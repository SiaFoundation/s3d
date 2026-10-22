package sia

import (
	"context"
	"io"
	"slices"
	"time"

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

// Account retrieves account information for the connected app key.
func (s *IndexdSDK) Account(ctx context.Context) (sdk.Account, error) {
	return s.inner.Account(ctx)
}

// Download downloads an object from indexd.
func (s *IndexdSDK) Download(ctx context.Context, obj *sdk.Object, rnge *s3.ObjectRange) (io.ReadCloser, error) {
	opts := slices.Clone(s.dlOpts)
	if rnge != nil {
		opts = append(opts, sdk.WithDownloadRange(uint64(rnge.Start), uint64(rnge.Length)))
	}
	return s.inner.Download(ctx, obj, opts...)
}

// OptimalDataSize returns the optimal data size.
func (s *IndexdSDK) OptimalDataSize(ctx context.Context) (int64, error) {
	pu, err := s.inner.UploadPacked(ctx, s.ulOpts...)
	if err != nil {
		return 0, err
	}
	defer pu.Close()
	return pu.OptimalDataSize(), nil
}

// Upload streams r into a new object carrying obj's metadata and returns it.
// Pin it with PinObject to persist it.
//
// obj supplies the metadata only. The SDK returns a different object once the
// transfer finishes, since the slabs are not known until then.
func (s *IndexdSDK) Upload(ctx context.Context, obj *sdk.Object, r io.Reader, opts ...sdk.UploadOption) (*sdk.Object, error) {
	return s.inner.Upload(ctx, obj, r, append(slices.Clone(s.ulOpts), opts...)...)
}

// UploadPacked creates a new packed upload.
func (s *IndexdSDK) UploadPacked(ctx context.Context, opts ...sdk.UploadOption) (PackedUpload, error) {
	return s.inner.UploadPacked(ctx, append(slices.Clone(s.ulOpts), opts...)...)
}

// PinObject pins the given object in the indexer.
func (s *IndexdSDK) PinObject(ctx context.Context, obj *sdk.Object) error {
	return s.inner.PinObject(ctx, obj)
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *IndexdSDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	return s.inner.DeleteObject(ctx, id)
}

// Object retrieves a single object from the indexer by id.
func (s *IndexdSDK) Object(ctx context.Context, id types.Hash256) (*sdk.Object, error) {
	return s.inner.Object(ctx, id)
}

// ObjectEvents returns object events from the indexer, starting from the
// given cursor, up to the given limit.
func (s *IndexdSDK) ObjectEvents(ctx context.Context, cursor slabs.Cursor, limit int) ([]sdk.ObjectEvent, error) {
	after := sdk.EventCursor{After: cursor.After, AfterID: cursor.Key}
	return s.inner.ObjectEvents(ctx, after, limit)
}

// PruneSlabs releases the slabs no remaining object references, which frees
// the pinned storage a deleted object was using. Only slabs pinned prior to
// before are released.
//
// A zero before leaves the cutoff to the indexer.
func (s *IndexdSDK) PruneSlabs(ctx context.Context, before time.Time) error {
	return s.inner.PruneSlabs(ctx, before)
}

// SealObject seals the object using the app key.
func (s *IndexdSDK) SealObject(obj *sdk.Object) (sdk.SealedObject, error) {
	return s.inner.SealObject(obj)
}

// UnsealObject unseals an object using the app key.
func (s *IndexdSDK) UnsealObject(sealed sdk.SealedObject) (*sdk.Object, error) {
	return s.inner.ObjectFromSealed(sealed)
}
