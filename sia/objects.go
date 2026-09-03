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
	"go.uber.org/zap"
)

// addDiskUsage reserves size bytes against the disk usage limit, blocking
// until enough space is available. If no space frees up within the disk
// usage timeout it fails with ErrSlowDown so clients can back off and
// retry instead of pinning a handler indefinitely. If allowExcess returns
// true the reservation bypasses the limit; it is re-evaluated each time
// releaseDiskUsage is called.
func (s *Sia) addDiskUsage(ctx context.Context, size int64, allowExcess func() (bool, error)) error {
	if size <= 0 || s.diskUsageLimit == 0 {
		return nil
	}
	timeout := time.NewTimer(s.diskUsageTimeout)
	defer timeout.Stop()
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
		case <-timeout.C:
			return s3errs.ErrSlowDown
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

func (lr *lockedUploadReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, lr.Reader)
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

func (s *Sia) openUpload(bucket, name, versionID string, filename *string, multipart bool, r *s3.ObjectRange) (_ io.ReadCloser, err error) {
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
		parts, err := s.store.ObjectPartsByName(bucket, name, versionID)
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
		if mr, ok := reader.(*objects.MultipartReader); ok {
			reader = objects.LimitReader(mr, r.Length)
		} else {
			reader = io.LimitReader(reader, r.Length)
		}
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

// cleanupOrphanFile removes the orphaned upload file and releases its disk
// usage. A zero OrphanedFile is a no-op.
func (s *Sia) cleanupOrphanFile(o objects.OrphanedFile) {
	if o.Filename == "" {
		return
	}
	s.cleanupOrphan(filepath.Join(s.uploadDir(), o.Filename), o.Size)
}

// CopyObject copies an object from the source bucket and object key to the
// destination bucket and object key. opts.Meta contains any metadata that
// should be merged into the copied object except for the x-amz-acl header.
func (s *Sia) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject string, srcVersion s3.VersionRequest, dstBucket, dstObject string, opts s3.CopyObjectOptions) (*s3.CopyObjectResult, error) {
	result, orphan, err := s.store.CopyObject(accessKeyID, srcBucket, srcObject, srcVersion, dstBucket, dstObject, opts)
	if err != nil {
		return nil, err
	}
	s.cleanupOrphanFile(orphan)
	return result, nil
}

// DeleteObject deletes the object with the given key from the specified
// bucket for the user identified by the given access key.
func (s *Sia) DeleteObject(ctx context.Context, accessKeyID, bucket string, object s3.ObjectID) (*s3.DeleteObjectResult, error) {
	versionID, isDeleteMarker, orphan, err := s.store.DeleteObject(accessKeyID, bucket, object)
	if err != nil {
		return nil, err
	}
	s.cleanupOrphanFile(orphan)

	return &s3.DeleteObjectResult{
		IsDeleteMarker: isDeleteMarker,
		VersionID:      versionID,
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
		versionID, isDeleteMarker, orphan, err := s.store.DeleteObject(accessKeyID, bucket, obj)
		if err == nil {
			s.cleanupOrphanFile(orphan)
		}

		if err != nil && !errors.Is(err, s3errs.ErrNoSuchKey) {
			result.Error = append(result.Error, s3.ErrorResult{
				Key:     obj.Key,
				Code:    s3errs.ErrorCode(err),
				Message: err.Error(),
			})
			continue
		}

		deleted := s3.DeletedObject{Key: obj.Key}
		if obj.VersionID != nil {
			deleted.VersionID = versionID
		}
		if isDeleteMarker {
			deleted.DeleteMarker = true
			deleted.DeleteMarkerVersionID = versionID
		}
		result.Deleted = append(result.Deleted, deleted)
	}
	return &result, nil
}

// GetObject retrieves the object with the given key from the specified
// bucket for the user identified by the given access key. The provided
// range is either nil if no range was requested, or contains the requested,
// byte range. version selects a specific version, or the current version when
// unspecified.
func (s *Sia) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, version s3.VersionRequest, rnge *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, version, rnge, partNumber, false)
}

// HeadObject is like GetObject but only retrieves the metadata of the
// object and returns an empty body.
func (s *Sia) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, version s3.VersionRequest, rnge *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return s.headOrGetObject(ctx, accessKeyID, bucket, object, version, rnge, partNumber, true)
}

func (s *Sia) headOrGetObject(ctx context.Context, accessKeyID *string, bucket, object string, version s3.VersionRequest, requestedRange *s3.ObjectRangeRequest, partNumber *int32, head bool) (*s3.Object, error) {
	// decided once so the retry below stays authorized as the read the client
	// asked for, not as a versioned one
	action := s3.ReadAction(version)

	obj, err := s.store.GetObject(accessKeyID, bucket, object, version, partNumber, action)
	if err != nil {
		return nil, err
	}

	resp := &s3.Object{
		ContentMD5:     obj.ContentMD5,
		LastModified:   obj.LastModified,
		Metadata:       obj.Meta,
		VersionID:      obj.VersionID,
		Versioned:      obj.Versioned,
		IsDeleteMarker: obj.IsDeleteMarker,
	}
	switch {
	case obj.IsDeleteMarker:
		// a delete marker carries no data; skip range/part validation
		resp.Size = obj.Length
		return resp, nil
	case partNumber != nil:
		resp.Range = &s3.ObjectRange{Start: obj.Offset, Length: obj.Length}
		resp.Size = obj.Size
		// nil for a non-multipart object, so its ETag gets no part suffix
		if obj.PartsCount > 0 {
			resp.PartsCount = aws.Int32(obj.PartsCount)
		}
	default:
		rnge, err := requestedRange.Range(obj.Length)
		if err != nil {
			return nil, err
		}
		resp.Range = rnge
		resp.Size = obj.Length
		if obj.PartsCount > 0 {
			resp.PartsCount = aws.Int32(obj.PartsCount)
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
		rc, err := s.openUpload(bucket, object, obj.VersionID, obj.FileName, obj.IsMultipart(), resp.Range)
		if errors.Is(err, fs.ErrNotExist) {
			// the upload loop moved the file to Sia between our GetObject
			// and file open, re-fetch the same version to get the updated
			// metadata and retry
			obj, err = s.store.GetObject(accessKeyID, bucket, object, s3.SpecificVersion(obj.VersionID), partNumber, action)
			if err != nil {
				return nil, err
			} else if obj.FileName != nil {
				rc, err = s.openUpload(bucket, object, obj.VersionID, obj.FileName, obj.IsMultipart(), resp.Range)
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
	// the store rejects an anonymous list unless the policy grants s3:ListBucket
	result, err := s.store.ListObjects(accessKeyID, bucket, prefix, page)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListObjectVersions lists all versions (including delete markers) of the
// objects in the specified bucket for the user identified by the given access
// key.
func (s *Sia) ListObjectVersions(ctx context.Context, accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectVersionsPage) (*s3.ObjectVersionsListResult, error) {
	// the store rejects an anonymous list unless the policy grants
	// s3:ListBucketVersions
	result, err := s.store.ListObjectVersions(accessKeyID, bucket, prefix, page)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// checkWritePreconditions evaluates the preconditions against the current
// version of the object a write would replace. It runs outside the write's
// transaction, so it is a fail-fast check only; the store re-checks atomically.
func (s *Sia) checkWritePreconditions(accessKeyID, bucket, object string, p s3.ObjectPreconditions) error {
	if !p.HasWritePreconditions() {
		return nil
	}
	obj, err := s.store.GetObject(&accessKeyID, bucket, object, s3.NoVersion(), nil, s3.ActionGetObject)
	if errors.Is(err, s3errs.ErrNoSuchKey) {
		return p.CheckWrite(nil)
	} else if err != nil {
		return err
	}
	attrs := obj.Attrs()
	return p.CheckWrite(&attrs)
}

// PutObject puts an object with the given key into the specified bucket.
func (s *Sia) PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts s3.PutObjectOptions) (_ *s3.PutObjectResult, err error) {
	// fail fast if the bucket is inaccessible before streaming the body to disk
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// likewise for a precondition that already cannot hold, so a doomed write
	// does not stream its whole body first
	if err := s.checkWritePreconditions(accessKeyID, bucket, object, opts.Preconditions); err != nil {
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
	versionID, orphan, err := s.store.PutObject(accessKeyID, bucket, object, objects.PutOptions{
		ContentMD5:    contentMD5,
		Meta:          opts.Meta,
		Length:        size,
		FileName:      fileName,
		Preconditions: opts.Preconditions,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}
	s.cleanupOrphanFile(orphan)

	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
		VersionID:  versionID,
	}, nil
}
