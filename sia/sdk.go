package sia

import (
	"context"
	"io"
	"slices"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
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

// Upload uploads an object to indexd and saves it.
func (s *IndexdSDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	obj := sdk.NewEmptyObject()
	if err := s.inner.Upload(ctx, &obj, r, s.ulOpts...); err != nil {
		return sdk.Object{}, err
	}
	if err := s.inner.PinObject(ctx, obj); err != nil {
		return sdk.Object{}, err
	}
	return obj, nil
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *IndexdSDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	return s.inner.DeleteObject(ctx, id)
}

// ObjectEvents returns object events from the indexer, starting from the
// given cursor, up to the given limit.
func (s *IndexdSDK) ObjectEvents(ctx context.Context, cursor sdk.ObjectsCursor, limit int) ([]sdk.ObjectEvent, error) {
	return s.inner.ObjectEvents(ctx, cursor, limit)
}

// SealObject seals the object using the app key.
func (s *IndexdSDK) SealObject(obj sdk.Object) objects.SiaObject {
	sealed := obj.Seal(s.inner.AppKey())
	return objects.SiaObject{
		ID:     sealed.ID(),
		Sealed: sealed.SealedObject,
	}
}

// UnsealObject unseals an object using the app key.
func (s *IndexdSDK) UnsealObject(siaObject objects.SiaObject) (sdk.Object, error) {
	return (&sdk.SealedObject{SealedObject: siaObject.Sealed}).Open(s.inner.AppKey())
}
