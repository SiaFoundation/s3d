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

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

// addDiskUsage reserves size bytes against the disk usage limit, blocking
// until enough space is available. If allowExcess returns true the
// reservation bypasses the limit; it is re-evaluated each time
// releaseDiskUsage is called.
func (s *Sia) addDiskUsage(ctx context.Context, size int64, allowExcess func() (bool, error)) error {
	if size <= 0 || s.diskUsageLimit == 0 {
		return nil
	}
	for {
		s.diskUsageMu.Lock()
		wake := s.diskUsageWake
		if s.diskUsage < s.diskUsageLimit {
			s.diskUsage += uint64(size)
			s.diskUsageMu.Unlock()
			return nil
		}
		s.diskUsageMu.Unlock()

		if allowExcess != nil {
			allow, err := allowExcess()
			if err != nil {
				return err
			} else if allow {
				s.diskUsageMu.Lock()
				s.diskUsage += uint64(size)
				s.diskUsageMu.Unlock()
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

// releaseDiskUsage releases size bytes previously reserved by addDiskUsage.
// Passing 0 wakes blocked waiters without releasing any space.
func (s *Sia) releaseDiskUsage(size int64) {
	if s.diskUsageLimit == 0 {
		return
	}

	s.diskUsageMu.Lock()
	defer s.diskUsageMu.Unlock()
	if size > 0 {
		if uint64(size) > s.diskUsage {
			s.logger.Warn("disk usage release exceeds tracked amount; resetting to 0",
				zap.Int64("size", size),
				zap.Uint64("diskUsage", s.diskUsage))
			s.diskUsage = 0
		} else {
			s.diskUsage -= uint64(size)
		}
	}
	close(s.diskUsageWake)
	s.diskUsageWake = make(chan struct{})
}

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

func (s *Sia) lockUpload(path string) func() {
	s.lockedUploadsMu.Lock()
	defer s.lockedUploadsMu.Unlock()

	lu, ok := s.lockedUploads[path]
	if !ok {
		lu = &lockedUpload{}
		s.lockedUploads[path] = lu
	}
	lu.refCount++

	return func() {
		s.lockedUploadsMu.Lock()
		defer s.lockedUploadsMu.Unlock()

		lu.refCount--
		if lu.refCount <= 0 {
			_, locked := s.lockedUploads[path]
			if !locked {
				panic(fmt.Sprintf("unlock called for path %s that is not locked", path))
			}
			delete(s.lockedUploads, path)
			if lu.deleted {
				if err := os.RemoveAll(path); err != nil {
					s.logger.Error("failed to remove upload upon unlock",
						zap.String("path", path),
						zap.Error(err))
				}
			}
		}
	}
}

func (s *Sia) openUpload(bucket, name string, filename *string, multipart bool, r *s3.ObjectRange) (_ io.ReadCloser, err error) {
	if filename == nil {
		return nil, os.ErrNotExist
	}
	uploadPath := filepath.Join(s.uploadDir(), *filename)
	unlock := s.lockUpload(uploadPath)
	defer func() {
		if err != nil {
			unlock()
		}
	}()

	var offset int64
	if r != nil {
		offset = r.Start
	}

	var reader io.Reader
	var closer io.Closer
	if multipart {
		parts, err := s.store.ObjectPartsByName(bucket, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get object parts: %w", err)
		}
		r, err := objects.NewReader(uploadPath, parts, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to create multipart reader: %w", err)
		}
		reader, closer = r, r
	} else {
		f, err := os.Open(uploadPath)
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

func (s *Sia) removeUpload(path string) error {
	s.lockedUploadsMu.Lock()
	if lu, ok := s.lockedUploads[path]; ok {
		lu.deleted = true
		s.lockedUploadsMu.Unlock()
		return nil
	}
	s.lockedUploadsMu.Unlock()

	return os.RemoveAll(path)
}

func (s *Sia) cleanupOrphan(path string, size int64) {
	if err := s.removeUpload(path); err != nil {
		s.logger.Warn("failed to remove orphaned upload file",
			zap.String("path", path),
			zap.Error(err))
	}
	s.releaseDiskUsage(size)
}

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. The provided metadata map contains any
// metadata that should be merged into the copied object except for the
// x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	obj, orphanFile, orphanSize, err := s.store.CopyObject(accessKeyID, srcBucket, srcObject, dstBucket, dstObject, meta, replace)
	if err != nil {
		return nil, err
	}
	if orphanFile != "" {
		s.cleanupOrphan(filepath.Join(s.uploadDir(), orphanFile), orphanSize)
	}

	return &s3.CopyObjectResult{
		ContentMD5:   obj.ContentMD5,
		LastModified: obj.LastModified,
		VersionID:    "", // versioning isn't supported
		PartsCount:   obj.PartsCount,
	}, nil
}

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket string, object s3.ObjectID) (*s3.DeleteObjectResult, error) {
	orphanFile, orphanSize, err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}
	if orphanFile != "" {
		s.cleanupOrphan(filepath.Join(s.uploadDir(), orphanFile), orphanSize)
	}

	return &s3.DeleteObjectResult{
		IsDeleteMarker: false,
		VersionID:      "",
	}, nil
}

// DeleteObjects deletes multiple objects from the specified bucket for the
// user identified by the given access key.
func (s *Sia) DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []s3.ObjectID) (*s3.ObjectsDeleteResult, error) {
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	var result s3.ObjectsDeleteResult

	for _, obj := range objects {
		orphanFile, orphanSize, err := s.store.DeleteObject(accessKeyID, bucket, obj)
		if err == nil && orphanFile != "" {
			s.cleanupOrphan(filepath.Join(s.uploadDir(), orphanFile), orphanSize)
		}

		if err != nil && !errors.Is(err, s3errs.ErrNoSuchKey) {
			result.Error = append(result.Error, s3.ErrorResult{
				Key:     obj.Key,
				Code:    s3errs.ErrorCode(err),
				Message: err.Error(),
			})
		} else {
			result.Deleted = append(result.Deleted, s3.ObjectID{
				Key: obj.Key,
				// VersionID is now *string; the follow-up branch (versioning-3)
				// rewrites this method.
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
		return nil, s3errs.ErrAccessDenied
	}

	obj, err := s.store.GetObject(*accessKeyID, bucket, object, partNumber)
	if err != nil {
		return nil, err
	}

	var resp *s3.Object
	if partNumber != nil {
		partsCount := max(obj.PartsCount, 1)
		resp = &s3.Object{
			Body:         nil,
			ContentMD5:   obj.ContentMD5,
			LastModified: obj.LastModified,
			Metadata:     obj.Meta,
			Range:        &s3.ObjectRange{Start: obj.Offset, Length: obj.Length},
			Size:         obj.Size,
			PartsCount:   aws.Int32(partsCount),
		}
	} else {
		rnge, err := requestedRange.Range(obj.Length)
		if err != nil {
			return nil, err
		}
		var partsCount *int32
		if obj.PartsCount > 0 {
			partsCount = aws.Int32(obj.PartsCount)
		}
		resp = &s3.Object{
			Body:         nil,
			ContentMD5:   obj.ContentMD5,
			LastModified: obj.LastModified,
			Metadata:     obj.Meta,
			Range:        rnge,
			Size:         obj.Length,
			PartsCount:   partsCount,
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
			// the upload loop moved the file to Sia between our GetObject
			// and file open, re-fetch to get the updated metadata and retry
			obj, err = s.store.GetObject(*accessKeyID, bucket, object, partNumber)
			if err != nil {
				return nil, err
			} else if obj.FileName != nil {
				rc, err = s.openUpload(bucket, object, obj.FileName, obj.IsMultipart(), resp.Range)
			}
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("failed to open pending upload file: %w", err)
		} else if rc != nil {
			resp.Body = rc
			return resp, nil
		}
	}

	if obj.SiaObject == nil {
		return nil, fmt.Errorf("object cannot neither be found on disk or on Sia")
	}
	siaObj, err := s.sdk.UnsealObject(obj.SiaObject.Sealed)
	if err != nil {
		return nil, fmt.Errorf("failed to unseal object: %w", err)
	}

	// otherwise, we download the body
	body, err := s.sdk.Download(siaObj, resp.Range)
	if err != nil {
		return nil, fmt.Errorf("failed to download object: %w", err)
	}

	resp.Body = body
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

	result, err := s.store.ListObjects(*accessKeyID, bucket, prefix, page)
	if err != nil {
		return nil, err
	}

	// populate owner info if requested
	if page.FetchOwner != nil && *page.FetchOwner {
		owner, err := s.UserInfo(ctx, *accessKeyID)
		if err != nil {
			return nil, err
		}
		for i := range result.Contents {
			result.Contents[i].Owner = owner
		}
	}
	return result, nil
}

// PutObject puts an object with the given key into the specified bucket.
func (s *Sia) PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts s3.PutObjectOptions) (_ *s3.PutObjectResult, err error) {
	// fail fast if the bucket is inaccessible before streaming the body to disk
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	if err := s.addDiskUsage(ctx, opts.ContentLength, nil); err != nil {
		return nil, err
	}
	var objPath string
	defer func() {
		if err != nil {
			s.cleanupOrphan(objPath, opts.ContentLength)
		}
	}()

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
	orphanFile, orphanSize, err := s.store.PutObject(accessKeyID, bucket, object, contentMD5, opts.Meta, size, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}
	if orphanFile != "" {
		s.cleanupOrphan(filepath.Join(s.uploadDir(), orphanFile), orphanSize)
	}

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}
