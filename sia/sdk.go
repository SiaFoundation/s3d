package sia

import (
	"context"
	"io"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
)

// IndexdSDK is a wrapper around the indexd SDK to implement the SDK interface.
type IndexdSDK struct {
	inner *sdk.SDK

	perDownloadInflight int
	perUploadInflight   int
}

func NewSDK(baseURL string, appKey types.PrivateKey, opts ...sdk.Option) (*IndexdSDK, error) {
	sdk, err := sdk.NewSDK(baseURL, appKey, opts...)
	if err != nil {
		return nil, err
	}
	return &IndexdSDK{
		inner: sdk,
	}, nil
}

func (s *IndexdSDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	return s.inner.Download(ctx, w, obj, sdk.WithDownloadInflight(s.perDownloadInflight))
}

func (s *IndexdSDK) PinObject(ctx context.Context, obj sdk.Object) error {
	return s.inner.PinObject(ctx, obj)
}

func (s *IndexdSDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	return s.inner.Upload(ctx, r, sdk.WithSkipPinObject(),
		sdk.WithUploadInflight(s.perUploadInflight))
}

func (s *IndexdSDK) OpenSealedObject(so slabs.SealedObject) (sdk.Object, error) {
	return s.OpenSealedObject(so)
}

func (s *IndexdSDK) SealObject(obj sdk.Object) slabs.SealedObject {
	return s.inner.SealObject(obj)
}
