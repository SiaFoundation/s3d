package sia

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
)

const metadataCacheLifetime = 24 * time.Hour

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	obj, err := s.store.GetObject(&accessKeyID, srcBucket, srcObject)
	if err != nil {
		return nil, err
	}

	if replace {
		obj.Meta = meta
	} else {
		maps.Copy(obj.Meta, meta)
	}
	obj.UpdatedAt = time.Now()

	if err := s.store.PutObject(accessKeyID, dstBucket, dstObject, obj); err != nil {
		return nil, err
	}
	return &s3.CopyObjectResult{
		ContentMD5:   obj.ContentMD5,
		LastModified: obj.UpdatedAt,
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
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest, _ *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, false)
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *s3.ObjectRangeRequest, _ *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, rnge, true)
}

func (s *Sia) headOrGetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, head bool) (*s3.Object, error) {
	if accessKeyID == nil {
		// anonymous access is not supported yet
		return nil, s3errs.ErrAccessDenied
	}

	obj, err := s.store.GetObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}

	rnge, err := requestedRange.Range(obj.Size)
	if err != nil {
		return nil, err
	}

	resp := &s3.Object{
		Body:         nil,
		ContentMD5:   obj.ContentMD5,
		LastModified: obj.UpdatedAt,
		Metadata:     obj.Meta,
		Range:        rnge,
		Size:         obj.Size,
	}

	// if this is a head request, we are done
	if head {
		return resp, nil
	}

	// handle empty objects without downloading from Sia
	if obj.Size == 0 {
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		return resp, nil
	}

	var cached bool
	var pinnedObj sdk.Object
	if len(obj.Object) > 0 {
		// try to load from cache
		if err := json.Unmarshal(obj.Object, &pinnedObj); err != nil {
			s.logger.Warn("failed to unmarshal cached object metadata", zap.Error(err))
		} else {
			cached = true
		}
	}

	// fetch from indexer if cache is missing or expired
	if !cached || time.Since(obj.ObjectRetrieved) >= metadataCacheLifetime {
		fetched, err := s.sdk.Object(ctx, obj.ID)
		if err != nil {
			if cached {
				s.logger.Warn("failed to fetch object from indexer, using stale metadata", zap.Error(err))
			} else {
				return nil, fmt.Errorf("failed to fetch object from indexer: %w", err)
			}
		} else {
			pinnedObj = fetched
			// update cache
			if data, err := json.Marshal(pinnedObj); err == nil {
				obj.Object = data
				obj.ObjectRetrieved = time.Now()
				if err := s.store.PutObject(*accessKeyID, bucket, object, obj); err != nil {
					s.logger.Warn("failed to update object metadata cache", zap.Error(err))
				}
			}
		}
	}

	// otherwise, we download the body
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err = s.sdk.Download(ctx, pw, pinnedObj, rnge)
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
	if accessKeyID == nil {
		// anonymous access is not supported yet
		return nil, s3errs.ErrAccessDenied
	}

	// quick check if the bucket exists
	if err := s.store.HeadBucket(*accessKeyID, bucket); err != nil {
		return nil, err
	}

	return s.store.ListObjects(accessKeyID, bucket, prefix, page)
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

	// handle empty object case
	var objectID types.Hash256
	var encoded []byte
	if opts.ContentLength == 0 {
		// drain reader
		if _, err := io.Copy(io.Discard, lr); err != nil {
			return nil, fmt.Errorf("failed to read object data: %w", err)
		}
	} else {
		// upload the data
		obj, err := s.sdk.Upload(ctx, lr)
		if err != nil {
			return nil, fmt.Errorf("failed to upload object: %w", err)
		}
		objectID = obj.ID()
		if encoded, err = json.Marshal(obj); err != nil {
			return nil, fmt.Errorf("failed to marshal object metadata: %w", err)
		}
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
	if err := s.store.PutObject(accessKeyID, bucket, object, &objects.Object{
		ID:              objectID,
		ContentMD5:      contentMD5,
		Meta:            opts.Meta,
		Size:            lr.N,
		UpdatedAt:       time.Now(),
		Object:          encoded,
		ObjectRetrieved: time.Now(),
	}); err != nil {
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
