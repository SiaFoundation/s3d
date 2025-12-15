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
	"maps"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	obj, err := s.store.GetObject(&accessKeyID, srcBucket, srcObject, nil)
	if err != nil {
		return nil, err
	}

	if replace {
		obj.Meta = meta
	} else {
		maps.Copy(obj.Meta, meta)
	}

	if err := s.store.PutObject(accessKeyID, dstBucket, dstObject, obj.ID, obj.Meta, obj.ContentMD5, obj.Length); err != nil {
		return nil, err
	}
	return &s3.CopyObjectResult{
		ContentMD5:   obj.ContentMD5,
		LastModified: obj.LastModified,
		VersionID:    "", // versioning isn't supported
	}, nil
}

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket string, object s3.ObjectID) (*s3.DeleteObjectResult, error) {
	err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}

	// TODO: unpin the object from the indexer if no other references exist

	return &s3.DeleteObjectResult{
		IsDeleteMarker: false,
		VersionID:      "",
	}, nil
}

// DeleteObjects deletes multiple objects from the specified bucket for the
// user identified by the given access key.
func (s *Sia) DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []s3.ObjectID) (*s3.ObjectsDeleteResult, error) {
	var result s3.ObjectsDeleteResult

	for _, obj := range objects {
		_, err := s.DeleteObject(ctx, accessKeyID, bucket, obj)
		if errors.Is(err, s3errs.ErrNoSuchBucket) {
			return nil, err
		}

		if err != nil && !errors.Is(err, s3errs.ErrNoSuchKey) {
			result.Error = append(result.Error, s3.ErrorResult{
				Key:     obj.Key,
				Code:    s3errs.ErrorCode(err),
				Message: err.Error(),
			})
		} else {
			result.Deleted = append(result.Deleted, s3.ObjectID{
				Key:       obj.Key,
				VersionID: "",
			})
		}
	}
	return &result, nil
}

// GetObject retrieves the object with the given key from the specified
// bucket for the user identified by the given access key. The provided
// range is either nil if no range was requested, or contains the requested,
// byte range.
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, partNumber, false)
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, partNumber, true)
}

func (s *Sia) headOrGetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, partNumber *int32, head bool) (*s3.Object, error) {
	if accessKeyID == nil {
		// anonymous access is not supported yet
		return nil, s3errs.ErrAccessDenied
	}

	obj, err := s.store.GetObject(accessKeyID, bucket, object, partNumber)
	if err != nil {
		return nil, err
	}

	var resp *s3.Object
	if partNumber != nil {
		resp = &s3.Object{
			Body:         nil,
			ContentMD5:   obj.ContentMD5,
			LastModified: obj.LastModified,
			Metadata:     obj.Meta,
			Range:        &s3.ObjectRange{Start: obj.Offset, Length: obj.Length},
			Size:         obj.Length,
			PartsCount:   aws.Int32(obj.PartsCount),
		}
	} else {
		rnge, err := requestedRange.Range(obj.Length)
		if err != nil {
			return nil, err
		}
		resp = &s3.Object{
			Body:         nil,
			ContentMD5:   obj.ContentMD5,
			LastModified: obj.LastModified,
			Metadata:     obj.Meta,
			Range:        rnge,
			Size:         obj.Length,
			PartsCount:   nil,
		}
	}

	// if this is a head request, we are done
	if head {
		return resp, nil
	}

	// TODO: once the indexer returns the full metadata we can cache it locally
	// and avoid fetching it on-demand.
	pinnedObj, err := s.sdk.Object(ctx, obj.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch object from indexer: %w", err)
	}

	// otherwise, we download the body
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err = s.sdk.Download(ctx, pw, pinnedObj, resp.Range)
		if err != nil {
			s.logger.Error("download failed", zap.Error(err), zap.String("bucket", bucket), zap.String("object", object))
			pw.CloseWithError(err)
			return
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
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
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

	// count size of uploaded data
	lr := &lenReader{
		inner: r,
	}

	// upload the data
	obj, err := s.sdk.Upload(ctx, lr)
	if err != nil {
		return nil, fmt.Errorf("failed to upload object: %w", err)
	}

	// check content length
	if opts.ContentLength != lr.N {
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

	// store the object in the database
	if err := s.store.PutObject(accessKeyID, bucket, object, obj.ID(), opts.Meta, contentMD5, lr.N); err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}

// lenReader is an io.Reader that counts the number of bytes read.
type lenReader struct {
	N     int64
	inner io.Reader
}

// Read counts the number of bytes read from the inner reader.
func (r *lenReader) Read(d []byte) (int, error) {
	n, err := r.inner.Read(d)
	r.N += int64(n)
	return n, err
}
