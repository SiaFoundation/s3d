package sia

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.sia.tech/indexd/sdk"
)

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket, object string) (*s3.DeleteObjectResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// GetObject retrieves the object with the given key from the specified
// bucket for the user identified by the given access key. The provided
// range is either nil if no range was requested, or contains the requested,
// byte range.
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return nil, s3errs.ErrNotImplemented
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return nil, s3errs.ErrNotImplemented
}

// ListObjects lists objects in the specified bucket for the user identified
// by the given access key. The backend should use the prefix to limit the
// contents of the bucket and sort the results into the Contents and
// CommonPrefixes fields of the returned ObjectsListResult.
func (s *Sia) ListObjects(ctx context.Context, accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// PutObject puts an object with the given key into the specified bucket.
func (s *Sia) PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts s3.PutObjectOptions) (*s3.PutObjectResult, error) {
	// check if bucket exists
	if err := s.HeadBucket(ctx, accessKeyID, bucket); err != nil {
		return nil, err
	}

	// TODO: check if object exists

	// compute md5 checksum for the etag
	md5Hash := md5.New()
	r = io.TeeReader(r, md5Hash)

	// check if we need to compute any other checksums
	var sha256Hash hash.Hash
	if opts.ContentSHA256 != nil {
		sha256Hash = sha256.New()
		r = io.TeeReader(r, sha256Hash)
	}

	// upload the data
	obj, err := s.sdk.Upload(ctx, r, sdk.WithSkipPinObject())
	if err != nil {
		return nil, fmt.Errorf("failed to upload object: %w", err)
	}

	// check content length
	var contentLength int64
	for _, slab := range obj.Slabs() {
		contentLength += int64(slab.Length)
	}
	if opts.ContentLength != contentLength {
		return nil, s3errs.ErrIncompleteBody
	}

	// verify checksums
	contentMD5 := md5.Sum(nil)
	if opts.ContentSHA256 != nil && !bytes.Equal(sha256Hash.Sum(nil), opts.ContentSHA256[:]) {
		return nil, s3errs.ErrBadDigest
	} else if opts.ContentMD5 != nil && contentMD5 != *opts.ContentMD5 {
		return nil, s3errs.ErrBadDigest
	}

	// update metadata and pin object
	objMeta := &objectMeta{
		contentMD5: contentMD5,
		meta:       opts.Meta,
	}
	encodedMeta, err := objMeta.encode()
	if err != nil {
		return nil, err
	}
	obj.UpdateMetadata(encodedMeta)

	if err := s.sdk.PinObject(ctx, obj); err != nil {
		return nil, fmt.Errorf("failed to pin object: %w", err)
	}

	return nil, s3errs.ErrNotImplemented
}
