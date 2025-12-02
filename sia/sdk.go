package sia

import (
	"context"
	"io"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
)

// IndexdSDK is a wrapper around the indexd SDK to implement the SDK interface.
type IndexdSDK struct {
	inner *sdk.SDK

	perDownloadInflight int
	perUploadInflight   int
}

// NewSDK creates a new IndexdSDK instance using the Go Indexd SDK.
func NewSDK(baseURL string, appKey types.PrivateKey, opts ...sdk.Option) (*IndexdSDK, error) {
	sdk, err := sdk.NewSDK(baseURL, appKey, opts...)
	if err != nil {
		return nil, err
	}
	return &IndexdSDK{
		inner: sdk,
	}, nil
}

// Download downloads an object from indexd.
func (s *IndexdSDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, rnge *s3.ObjectRange) error {
	// TODO: support range downloads once the SDK supports them
	return s.inner.Download(ctx, w, obj, sdk.WithDownloadInflight(s.perDownloadInflight))
}

// Upload uploads an object to indexd without pinning it.
func (s *IndexdSDK) Upload(ctx context.Context, r io.Reader) (sdk.Object, error) {
	return s.inner.Upload(ctx, r,
		sdk.WithUploadInflight(s.perUploadInflight))
}

// Object retrieves the object with the given key.
func (s *IndexdSDK) Object(ctx context.Context, objectKey types.Hash256) (sdk.Object, error) {
	return s.inner.Object(ctx, objectKey)
}
