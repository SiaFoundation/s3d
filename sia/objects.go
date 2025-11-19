package sia

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket, object string) (*s3.DeleteObjectResult, error) {
	err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}
	return &s3.DeleteObjectResult{
		IsDeleteMarker: false,
		VersionID:      "",
	}, nil
}

// GetObject retrieves the object with the given key from the specified
// bucket for the user identified by the given access key. The provided
// range is either nil if no range was requested, or contains the requested,
// byte range.
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, false)
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, true)
}

func (s *Sia) headOrGetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, head bool) (*s3.Object, error) {
	so, err := s.store.GetObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}

	obj, err := s.sdk.OpenSealedObject(so)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	var meta objectMeta
	if err := meta.decode(obj.Metadata()); err != nil {
		return nil, fmt.Errorf("failed to decode object metadata: %w", err)
	}

	size := int64(obj.Size())
	rnge, err := requestedRange.Range(size)
	if err != nil {
		return nil, err
	}

	resp := &s3.Object{
		Body:         nil,
		ContentMD5:   meta.contentMD5,
		LastModified: obj.UpdatedAt(),
		Metadata:     meta.meta,
		Range:        rnge,
		Size:         size,
	}

	// if this is a head request, we are done
	if head {
		return resp, nil
	}

	// otherwise, we download the body
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err := s.sdk.Download(ctx, pw, obj, rnge)
		if err != nil {
			s.logger.Error("download failed", zap.Error(err), zap.String("bucket", bucket), zap.String("object", object))
		}
	}()

	resp.Body = pr
	return resp, nil
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
	// quick check if the object already exists
	if _, err := s.store.GetObject(&accessKeyID, bucket, object); !errors.Is(err, s3errs.ErrNoSuchKey) {
		return nil, err
	}

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
	obj, err := s.sdk.Upload(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("failed to upload object: %w", err)
	}

	// check content length
	if opts.ContentLength != int64(obj.Size()) {
		return nil, s3errs.ErrIncompleteBody
	}

	// verify checksums
	var contentMD5 [16]byte
	sum := md5Hash.Sum(nil)
	copy(contentMD5[:], sum)
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

	// store the object in the database
	if err := s.store.PutObject(accessKeyID, bucket, object, s.sdk.SealObject(obj)); err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}
