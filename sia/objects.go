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
	"os"
	"path/filepath"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

const metadataCacheLifetime = 24 * time.Hour

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	// if the source is on disk, copy the file first so we can pass the
	// new filename to the store
	var dstFilename *string
	srcObj, err := s.store.GetObject(&accessKeyID, srcBucket, srcObject, nil)
	if err != nil {
		return nil, err
	}
	if srcObj.Filename != nil {
		src, err := os.Open(filepath.Join(s.packingDir, *srcObj.Filename))
		if errors.Is(err, os.ErrNotExist) {
			// the background packer may have uploaded and removed the file
			// between our db read and file open, re-fetch from the store
			srcObj, err = s.store.GetObject(&accessKeyID, srcBucket, srcObject, nil)
			if err != nil {
				return nil, err
			} else if srcObj.Filename != nil {
				return nil, fmt.Errorf("file %q not found on disk but object still references it", *srcObj.Filename)
			}
			// filename is now nil, fall through to store.CopyObject which
			// will copy the object on Sia
		} else if err != nil {
			return nil, fmt.Errorf("failed to open source file on disk: %w", err)
		} else {
			defer src.Close()

			fn, err := s.writeToDisk(src)
			if err != nil {
				return nil, fmt.Errorf("failed to copy file on disk: %w", err)
			}
			dstFilename = &fn
		}
	}

	obj, prevFilename, err := s.store.CopyObject(srcBucket, srcObject, dstBucket, dstObject, meta, replace, dstFilename)
	if err != nil {
		s.tryRemove(dstFilename)
		return nil, err
	}

	s.tryRemove(prevFilename)
	s.tryPack(dstFilename)

	return &s3.CopyObjectResult{
		ContentMD5:   obj.ContentMD5,
		LastModified: obj.LastModified,
		VersionID:    "", // versioning isn't supported
	}, nil
}

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket string, object s3.ObjectID) (*s3.DeleteObjectResult, error) {
	filename, err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}

	s.tryRemove(filename)

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

	// handle empty objects without downloading from Sia
	if obj.Length == 0 {
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		return resp, nil
	}

	// serve from disk if the object is stored locally
	if obj.Filename != nil {
		f, err := os.Open(filepath.Join(s.packingDir, *obj.Filename))
		if errors.Is(err, os.ErrNotExist) {
			// the background packer may have uploaded and removed the file
			// between our db read and file open, re-fetch from the store
			obj, err = s.store.GetObject(accessKeyID, bucket, object, partNumber)
			if err != nil {
				return nil, err
			} else if obj.Filename != nil {
				return nil, fmt.Errorf("file %q not found on disk but object still references it", *obj.Filename)
			}
			// filename is now nil, fall through to the Sia download path
		} else if err != nil {
			return nil, fmt.Errorf("failed to open file on disk: %w", err)
		} else {
			if resp.Range != nil {
				if _, err := f.Seek(resp.Range.Start, io.SeekStart); err != nil {
					f.Close()
					return nil, fmt.Errorf("failed to seek file on disk: %w", err)
				}
				resp.Body = LimitReadCloser(f, resp.Range.Length)
			} else {
				resp.Body = f
			}
			return resp, nil
		}
	}

	// refresh cached object metadata if needed
	siaObj, err := s.refreshSiaObject(ctx, *accessKeyID, bucket, object, obj)
	if err != nil {
		return nil, err
	}

	// otherwise, we download the body
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err = s.sdk.Download(ctx, pw, siaObj, resp.Range)
		if err != nil {
			s.logger.Error("download failed", zap.Error(err), zap.String("bucket", bucket), zap.String("object", object))
			pw.CloseWithError(err)
			return
		}
	}()

	resp.Body = pr
	return resp, nil
}

// refreshSiaObject refreshes the object's cached Sia object if it is missing
// or stale. Returns the unsealed sdk.Object for use in downloads.
func (s *Sia) refreshSiaObject(ctx context.Context, accessKeyID, bucket, objectKey string, obj *objects.Object) (siaObj sdk.Object, err error) {
	cached := !obj.CachedAt.IsZero()

	// if cache is fresh, unseal and return
	cachedUntil := obj.CachedAt.Add(metadataCacheLifetime)
	if time.Now().Before(cachedUntil) {
		siaObj, err = s.sdk.UnsealObject(obj.SiaObject)
		if err != nil {
			s.logger.Warn("failed to unseal cached object, will fetch from indexer", zap.Error(err))
			cached = false
		} else {
			return siaObj, nil
		}
	}

	// fetch from indexer
	fetched, err := s.sdk.Object(ctx, obj.ID)
	if err != nil {
		if cached {
			s.logger.Warn("failed to fetch object from indexer, using stale metadata", zap.Error(err))
			// try to unseal the stale cached object
			return s.sdk.UnsealObject(obj.SiaObject)
		}
		return sdk.Object{}, fmt.Errorf("failed to fetch object from indexer: %w", err)
	}

	// seal the fetched object for storage
	obj.SiaObject = s.sdk.SealObject(fetched)
	obj.CachedAt = time.Now()
	if _, err := s.store.PutObject(bucket, objectKey, obj, false); err != nil {
		s.logger.Warn("failed to update object metadata cache", zap.Error(err))
	}
	return fetched, nil
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
	var siaObject slabs.SealedObject
	var cachedAt time.Time
	var filename *string
	if opts.ContentLength == 0 {
		// drain reader
		if _, err := io.Copy(io.Discard, lr); err != nil {
			return nil, fmt.Errorf("failed to read object data: %w", err)
		}
	} else if s.needsPacking(opts.ContentLength) {
		// small object, write to disk and upload to Sia later
		fn, err := s.writeToDisk(lr)
		if err != nil {
			return nil, fmt.Errorf("failed to write object to disk: %w", err)
		}
		filename = &fn
	} else {
		// large object — upload directly
		obj, err := s.sdk.Upload(ctx, lr)
		if err != nil {
			return nil, fmt.Errorf("failed to upload object: %w", err)
		}
		err = s.sdk.PinObject(ctx, obj)
		if err != nil {
			return nil, fmt.Errorf("failed to pin object in indexer: %w", err)
		}
		objectID = obj.ID()
		siaObject = s.sdk.SealObject(obj)
		cachedAt = time.Now()
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
	prevFilename, err := s.store.PutObject(bucket, object, &objects.Object{
		ID:         objectID,
		ContentMD5: contentMD5,
		Meta:       opts.Meta,
		Length:     lr.N,
		SiaObject:  siaObject,
		CachedAt:   cachedAt,
		Filename:   filename,
	}, true)
	if err != nil {
		s.tryRemove(filename)
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}

	// trigger packing if needed
	s.tryRemove(prevFilename)
	s.tryPack(filename)

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}

// limitReadCloser wraps an io.LimitReader with a Closer that closes the
// underlying ReadCloser.
type limitReadCloser struct {
	io.Reader
	io.Closer
}

// LimitReadCloser returns an io.ReadCloser that reads at most n bytes from rc
// and then closes rc when Close is called.
func LimitReadCloser(rc io.ReadCloser, n int64) io.ReadCloser {
	return &limitReadCloser{
		Reader: io.LimitReader(rc, n),
		Closer: rc,
	}
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
