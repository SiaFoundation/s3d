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
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const metadataCacheLifetime = 24 * time.Hour

type (
	lockedUpload struct {
		deleted  bool
		refCount int
	}

	lockedUploadReader struct {
		io.Reader
		c      io.Closer
		unlock func()
	}
)

func (lr *lockedUploadReader) Close() error {
	err := lr.c.Close()
	lr.unlock()
	return err
}

func (s *Sia) uploadDir() string {
	return filepath.Join(s.directory, UploadsDirectory)
}

func (s *Sia) lockUpload(filename string) func() {
	s.lockedUploadsMu.Lock()
	defer s.lockedUploadsMu.Unlock()

	lu, ok := s.lockedUploads[filename]
	if !ok {
		lu = &lockedUpload{}
		s.lockedUploads[filename] = lu
	}
	lu.refCount++

	return func() {
		s.lockedUploadsMu.Lock()
		defer s.lockedUploadsMu.Unlock()

		lu.refCount--
		if lu.refCount == 0 {
			delete(s.lockedUploads, filename)
			if lu.deleted {
				if err := os.RemoveAll(filepath.Join(s.uploadDir(), filename)); err != nil {
					s.logger.Error("failed to remove upload upon unlock",
						zap.String("filename", filename),
						zap.Error(err))
				}
			}
		}
		return
	}
}

func (s *Sia) openUpload(bucket, name string, filename *string, multipart bool, r *s3.ObjectRange) (io.ReadCloser, error) {
	if filename == nil {
		return nil, os.ErrNotExist
	}
	unlock := s.lockUpload(*filename)
	defer unlock()

	var offset int64
	if r != nil {
		offset = r.Start
	}

	var reader io.Reader
	var closer io.Closer
	if multipart {
		parts, err := s.store.ObjectParts(bucket, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get object parts: %w", err)
		}
		uploadDir := filepath.Join(s.uploadDir(), *filename)
		r, err := objects.NewReader(uploadDir, parts, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to create multipart reader: %w", err)
		}
		reader, closer = r, r
	} else {
		f, err := os.Open(filepath.Join(s.uploadDir(), *filename))
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				f.Close()
				return nil, err
			}
		}
		reader, closer = f, f
	}

	if r != nil {
		reader = io.LimitReader(reader, r.Length)
	}
	return &lockedUploadReader{Reader: reader, c: closer, unlock: unlock}, nil
}

func (s *Sia) removeUpload(fileName string) error {
	s.lockedUploadsMu.Lock()
	defer s.lockedUploadsMu.Unlock()

	lu, ok := s.lockedUploads[fileName]
	if ok {
		lu.deleted = true
		return nil
	}
	return os.RemoveAll(filepath.Join(s.uploadDir(), fileName))
}

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	obj, err := s.store.CopyObject(srcBucket, srcObject, dstBucket, dstObject, meta, replace)
	if err != nil {
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
	fileName, err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	} else if fileName != nil {
		// object hasn't been uploaded yet, so we clean up the file
		if err := s.removeUpload(*fileName); err != nil {
			s.logger.Warn("failed to remove pending upload file", zap.String("bucket", bucket), zap.String("object", object.Key), zap.Error(err))
		}
	}

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

	// read from disk if the object hasn't been uploaded yet
	if obj.FileName != nil {
		rc, err := s.openUpload(bucket, object, obj.FileName, obj.IsMultipart(), resp.Range)
		if errors.Is(err, fs.ErrNotExist) {
			// possible race, check if the object was uploaded to Sia in the
			// meantime
			obj, err = s.store.GetObject(accessKeyID, bucket, object, partNumber)
			if err != nil {
				return nil, err
			} else if obj.SiaObject == nil {
				return nil, fmt.Errorf("pending upload file disappeared and object is not on Sia")
			}
		} else if err != nil {
			return nil, fmt.Errorf("failed to open pending upload file: %w", err)
		} else {
			resp.Body = rc
			return resp, nil
		}
	}

	// object is on Sia, refresh cached object metadata if needed
	siaObj, err := s.refreshSiaObject(ctx, obj)
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
func (s *Sia) refreshSiaObject(ctx context.Context, obj *objects.Object) (siaObj sdk.Object, err error) {
	if obj.ID == nil || obj.SiaObject == nil {
		return sdk.Object{}, fmt.Errorf("object hasn't been uploaded yet") // should never happen
	}
	cached := !obj.CachedAt.IsZero()

	// if cache is fresh, unseal and return
	cachedUntil := obj.CachedAt.Add(metadataCacheLifetime)
	if time.Now().Before(cachedUntil) {
		siaObj, err = s.sdk.UnsealObject(*obj.SiaObject)
		if err != nil {
			s.logger.Warn("failed to unseal cached object, will fetch from indexer", zap.Error(err))
			cached = false
		} else {
			return siaObj, nil
		}
	}

	// fetch from indexer
	fetched, err := s.sdk.Object(ctx, *obj.ID)
	if err != nil {
		if cached {
			s.logger.Warn("failed to fetch object from indexer, using stale metadata", zap.Error(err))
			// try to unseal the stale cached object
			return s.sdk.UnsealObject(*obj.SiaObject)
		}
		return sdk.Object{}, fmt.Errorf("failed to fetch object from indexer: %w", err)
	}

	// seal the fetched object for storage
	sealed := s.sdk.SealObject(fetched)
	if err := s.store.UpdateSiaObject(sealed, time.Now()); err != nil {
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
func (s *Sia) PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts s3.PutObjectOptions) (_ *s3.PutObjectResult, err error) {
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

	// handle empty object case
	var objPath string
	var fileName *string
	var size int64
	if opts.ContentLength == 0 {
		// drain reader
		if _, err := io.Copy(io.Discard, r); err != nil {
			return nil, fmt.Errorf("failed to read object data: %w", err)
		}
	} else {
		// save the object
		randFileName := randObjectName()
		objPath = filepath.Join(s.uploadDir(), randFileName)
		fileName = &randFileName
		f, err := os.Create(objPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary file: %w", err)
		}
		size, err = io.Copy(f, io.LimitReader(r, opts.ContentLength))
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to store object: %w", err)
		} else if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to sync object to disk: %w", err)
		} else if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close object file: %w", err)
		}
	}

	defer func() {
		if err != nil && objPath != "" {
			_ = os.Remove(objPath)
		}
	}()

	// check content length
	if opts.ContentLength != size {
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
	if err := s.store.PutObject(accessKeyID, bucket, object, contentMD5, opts.Meta, size, fileName, true); err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}
