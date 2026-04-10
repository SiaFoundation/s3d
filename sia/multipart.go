package sia

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// CreateMultipartUpload creates a new multipart upload.
func (s *Sia) CreateMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, opts s3.CreateMultipartUploadOptions) (*s3.CreateMultipartUploadResult, error) {
	// check bucket access
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// create multipart upload directory
	uploadID := s3.NewUploadID()
	if err := os.Mkdir(filepath.Join(s.multipartDir, uploadID.String()), 0700); err != nil {
		return nil, fmt.Errorf("failed to create upload directory: %w", err)
	}

	// create multipart upload in the database
	err := s.store.CreateMultipartUpload(bucket, object, uploadID, opts.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return &s3.CreateMultipartUploadResult{UploadID: uploadID}, nil
}

// ListMultipartUploads lists in-progress multipart uploads.
func (s *Sia) ListMultipartUploads(ctx context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions, page s3.ListMultipartUploadsPage) (*s3.ListMultipartUploadsResult, error) {
	// assert auth
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	prefix := s3.Prefix{
		HasPrefix:    opts.Prefix != "",
		Prefix:       opts.Prefix,
		HasDelimiter: opts.Delimiter != "",
		Delimiter:    opts.Delimiter,
	}

	return s.store.ListMultipartUploads(bucket, prefix, page)
}

// AbortMultipartUpload aborts a multipart upload.
func (s *Sia) AbortMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, uploadID s3.UploadID) error {
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return err
	}

	// abort the multipart upload in the database
	if err := s.store.AbortMultipartUpload(bucket, object, uploadID); err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	// remove multipart upload directory
	uploadDir := filepath.Join(s.multipartDir, uploadID.String())
	if err := os.RemoveAll(uploadDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove multipart upload directory",
			zap.String("path", uploadDir),
			zap.Error(err))
	}

	return nil
}

// UploadPart uploads a single multipart part.
func (s *Sia) UploadPart(ctx context.Context, accessKeyID, bucket, object string, uploadID s3.UploadID, r io.Reader, opts s3.UploadPartOptions) (_ *s3.UploadPartResult, err error) {
	// check if the multipart upload exists
	if err := s.store.HasMultipartUpload(bucket, object, uploadID); err != nil {
		return nil, err
	}

	// create part directory
	partDir := filepath.Join(s.multipartDir, uploadID.String(), fmt.Sprintf("%d", opts.PartNumber))
	if err := os.Mkdir(partDir, 0700); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	// create part file
	partPath := filepath.Join(partDir, randFilename(extMultipartPart))
	partFile, err := os.Create(partPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create part file: %w", err)
	}
	defer partFile.Close()

	// defer cleanup on error
	defer func() {
		if err != nil {
			if removeErr := os.Remove(partPath); removeErr != nil {
				s.logger.Error("failed to remove part file after upload failure",
					zap.String("path", partPath),
					zap.Error(removeErr))
			}
		}
	}()

	// prepare writers
	md5Hash := md5.New()
	writers := []io.Writer{partFile, md5Hash}

	var sha256Hash hash.Hash
	if opts.ContentSHA256 != nil {
		sha256Hash = sha256.New()
		writers = append(writers, sha256Hash)
	}

	// copy data and validate size
	contentLength, err := io.Copy(io.MultiWriter(writers...), io.LimitReader(r, s3.MaxUploadPartSize+1))
	if err != nil {
		return nil, err
	} else if contentLength != opts.ContentLength {
		return nil, s3errs.ErrIncompleteBody
	} else if contentLength > s3.MaxUploadPartSize {
		return nil, s3errs.ErrEntityTooLarge
	}

	// validate hash digests
	var contentMD5 [16]byte
	copy(contentMD5[:], md5Hash.Sum(nil))
	if opts.ContentMD5 != nil && *opts.ContentMD5 != contentMD5 {
		return nil, s3errs.ErrBadDigest
	}

	var contentSHA256 *[32]byte
	if opts.ContentSHA256 != nil {
		contentSHA256 = new([32]byte)
		copy(contentSHA256[:], sha256Hash.Sum(nil))
		if *opts.ContentSHA256 != *contentSHA256 {
			return nil, s3errs.ErrBadDigest
		}
	}

	// sync part file
	if err := partFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync part file: %w", err)
	}

	// sync parent directory
	if dir, err := os.Open(partDir); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil {
		return nil, fmt.Errorf("failed to open part directory: %w", err)
	} else if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, fmt.Errorf("failed to sync part directory: %w", err)
	}

	// add multipart part to the database
	previous, err := s.store.AddMultipartPart(bucket, object, uploadID, filepath.Base(partPath), opts.PartNumber, contentMD5, contentLength)
	if err != nil {
		return nil, fmt.Errorf("failed to add part: %w", err)
	} else if previous != "" {
		prevPath := filepath.Join(partDir, previous)
		if err := os.Remove(prevPath); err != nil {
			s.logger.Error("failed to remove old part file",
				zap.String("path", prevPath),
				zap.Error(err))
		}
	}

	return &s3.UploadPartResult{ContentMD5: contentMD5}, nil
}

// UploadPartCopy uploads a part by copying data from an existing object.
func (s *Sia) UploadPartCopy(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, uploadID s3.UploadID, opts s3.UploadPartCopyOptions) (*s3.UploadPartCopyResult, error) {
	// check if the multipart upload exists
	if err := s.store.HasMultipartUpload(dstBucket, dstObject, uploadID); err != nil {
		return nil, err
	}

	// fetch source object metadata
	obj, err := s.store.GetObject(srcBucket, srcObject, nil)
	if err != nil {
		return nil, err
	}

	// validate range
	if opts.Range.Length <= 0 || opts.Range.Start < 0 || opts.Range.Start >= obj.Length || opts.Range.Length > obj.Length-opts.Range.Start {
		return nil, s3errs.ErrInvalidRange
	} else if opts.Range.Length > s3.MaxUploadPartSize {
		return nil, s3errs.ErrEntityTooLarge
	}

	// create part directory
	partDir := filepath.Join(s.multipartDir, uploadID.String(), fmt.Sprintf("%d", opts.PartNumber))
	if err := os.Mkdir(partDir, 0700); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	// create part file
	partPath := filepath.Join(partDir, randFilename(extMultipartPart))
	partFile, err := os.Create(partPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create part file: %w", err)
	}
	defer partFile.Close()

	// defer cleanup on error
	defer func() {
		if err != nil {
			if removeErr := os.Remove(partPath); removeErr != nil {
				s.logger.Error("failed to remove part file after upload failure",
					zap.String("path", partPath),
					zap.Error(removeErr))
			}
		}
	}()

	// write the requested range to the part file
	md5Hash := md5.New()
	writer := &lenWriter{
		w: io.MultiWriter(partFile, md5Hash),
	}

	file, err := s.openPackedObject(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to open source object: %w", err)
	} else if file != nil {
		// try write from disk if it's a packed object
		defer file.Close()
		if _, err := file.Seek(opts.Range.Start, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek source file on disk: %w", err)
		}
		if _, err := io.CopyN(writer, file, opts.Range.Length); err != nil {
			return nil, fmt.Errorf("failed to copy from source file on disk: %w", err)
		}
	} else if obj.ID != nil {
		// download from Sia if it's not a packed object or empty object
		pinnedObj, err := s.sdk.Object(ctx, *obj.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch object from indexer: %w", err)
		}
		if err := s.sdk.Download(ctx, writer, pinnedObj, &opts.Range); err != nil {
			s.logger.Error("download failed",
				zap.Error(err),
				zap.String("bucket", srcBucket),
				zap.String("object", srcObject))
			return nil, err
		}
	}

	contentLength := writer.n
	if contentLength != opts.Range.Length {
		return nil, s3errs.ErrInvalidRange
	}

	var contentMD5 [16]byte
	copy(contentMD5[:], md5Hash.Sum(nil))

	// sync part file
	if err := partFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync part file: %w", err)
	}

	// sync parent directory
	if dir, err := os.Open(partDir); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil {
		return nil, fmt.Errorf("failed to open part directory: %w", err)
	} else if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, fmt.Errorf("failed to sync part directory: %w", err)
	}

	// add multipart part to the database
	previous, err := s.store.AddMultipartPart(dstBucket, dstObject, uploadID, filepath.Base(partPath), opts.PartNumber, contentMD5, contentLength)
	if err != nil {
		return nil, fmt.Errorf("failed to add part: %w", err)
	} else if previous != "" {
		prevPath := filepath.Join(partDir, previous)
		if err := os.Remove(prevPath); err != nil {
			s.logger.Error("failed to remove old part file",
				zap.String("path", prevPath),
				zap.Error(err))
		}
	}

	return &s3.UploadPartCopyResult{
		ContentMD5:   contentMD5,
		LastModified: obj.LastModified,
	}, nil
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Sia) ListParts(ctx context.Context, accessKeyID, bucket, object string, uploadID s3.UploadID, page s3.ListPartsPage) (*s3.ListPartsResult, error) {
	return s.store.ListParts(bucket, object, uploadID, page.PartNumberMarker, page.MaxParts)
}

// CompleteMultipartUpload completes a multipart upload.
func (s *Sia) CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, uploadID s3.UploadID, parts []s3.CompleteMultipartPart) (*s3.CompleteMultipartUploadResult, error) {
	// get multipart upload
	uploaded, err := s.store.MultipartParts(bucket, object, uploadID)
	if err != nil {
		return nil, err
	}

	// build map of uploaded parts
	lookup := make(map[int]objects.Part)
	for _, part := range uploaded {
		lookup[part.PartNumber] = part
	}

	// validate parts
	completed := make([]objects.Part, len(parts))
	for i, p := range parts {
		part, ok := lookup[p.PartNumber]
		if !ok {
			return nil, s3errs.ErrInvalidPart
		}
		if s3.ParseETag(p.ETag) != part.ContentMD5 {
			return nil, s3errs.ErrInvalidPart
		}
		lastPart := i == len(parts)-1
		if !lastPart && part.Size < s3.MinUploadPartSize {
			return nil, s3errs.ErrEntityTooSmall
		}
		completed[i] = part
	}

	// validate part numbers are in ascending order
	for i := 1; i < len(parts); i++ {
		if parts[i].PartNumber <= parts[i-1].PartNumber {
			return nil, s3errs.ErrInvalidPartOrder
		}
	}

	// assert the upload directory exists
	uploadDir := filepath.Join(s.multipartDir, uploadID.String())
	if _, err := os.Stat(uploadDir); err != nil {
		return nil, fmt.Errorf("failed to stat upload directory: %w", err)
	}

	// create reader for the completed object
	r, err := objects.NewReader(uploadDir, completed)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart reader: %w", err)
	}
	defer r.Close()

	// compute final content MD5
	hash := md5.New()
	for _, part := range completed {
		hash.Write(part.ContentMD5[:])
	}
	var contentMD5 [16]byte
	copy(contentMD5[:], hash.Sum(nil))

	// compute final content length
	var contentLength int64
	for _, part := range completed {
		contentLength += int64(part.Size)
	}

	var objectID *types.Hash256
	var packedFilename *string
	if s.needsPacking(contentLength) {
		// uploading directly to Sia would be too wasteful, write to disk
		// for the packer to pick up instead
		filename, err := s.writePackedObject(r)
		if err != nil {
			return nil, fmt.Errorf("failed to write completed object to disk: %w", err)
		}
		packedFilename = &filename
	} else {
		// upload directly and pin the object
		obj, err := s.sdk.Upload(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("failed to upload object to Sia: %w", err)
		}
		err = s.sdk.PinObject(ctx, obj)
		if err != nil {
			if delErr := s.sdk.DeleteObject(ctx, obj.ID()); delErr != nil {
				s.logger.Error("failed to delete object after pin failure",
					zap.String("objectID", obj.ID().String()),
					zap.NamedError("pinFailure", err),
					zap.Error(delErr))
			}
			return nil, fmt.Errorf("failed to pin object in indexer: %w", err)
		}
		oID := obj.ID()
		objectID = &oID
	}

	// complete the multipart upload in the database
	prevFilename, err := s.store.CompleteMultipartUpload(bucket, object, uploadID, objectID, packedFilename, contentMD5, contentLength)
	if err != nil {
		s.tryRemovePackedObject(packedFilename)
		return nil, fmt.Errorf("failed to complete multipart upload in store: %w", err)
	}

	// trigger packing if needed
	s.tryRemovePackedObject(prevFilename)
	if packedFilename != nil {
		s.triggerPacking()
	}

	// calculate ETag
	etag := s3.FormatETag(contentMD5[:], len(completed))

	// remove multipart upload directory
	if err := os.RemoveAll(uploadDir); err != nil {
		s.logger.Error("failed to remove multipart upload directory after completion",
			zap.String("path", uploadDir),
			zap.Error(err))
	}

	return &s3.CompleteMultipartUploadResult{
		ETag:       etag,
		ContentMD5: contentMD5,
	}, nil
}

// lenWriter counts bytes written while forwarding to the wrapped writer.
type lenWriter struct {
	w io.Writer
	n int64
}

// Write forwards the data and increments the byte count.
func (w *lenWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}
