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
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func randPartName() string {
	var uuid [8]byte
	frand.Read(uuid[:])
	return fmt.Sprintf("%x.part", uuid[:])
}

// CreateMultipartUpload creates a new multipart upload.
func (s *Sia) CreateMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, opts s3.CreateMultipartUploadOptions) (*s3.CreateMultipartUploadResult, error) {
	// check bucket access
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// create multipart upload directory
	uploadID := s3.NewUploadID()
	if err := os.Mkdir(filepath.Join(s.directory, uploadID.String()), 0700); err != nil {
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
func (s *Sia) AbortMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string) error {
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return err
	}

	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return s3errs.ErrNoSuchUpload
	}

	// abort the multipart upload in the database
	if err := s.store.AbortMultipartUpload(bucket, object, uid); err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	// remove multipart upload directory
	uploadDir := filepath.Join(s.directory, uploadID)
	if err := os.RemoveAll(uploadDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove multipart upload directory",
			zap.String("path", uploadDir),
			zap.Error(err))
	}

	return nil
}

// UploadPart uploads a single multipart part.
func (s *Sia) UploadPart(ctx context.Context, accessKeyID, bucket, object, uploadID string, r io.Reader, opts s3.UploadPartOptions) (_ *s3.UploadPartResult, err error) {
	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return nil, s3errs.ErrNoSuchUpload
	}

	// check if the multipart upload exists
	if err := s.store.HasMultipartUpload(bucket, object, uid); err != nil {
		return nil, err
	}

	// create part directory
	partDir := filepath.Join(s.directory, uploadID, fmt.Sprintf("%d", opts.PartNumber))
	if err := os.Mkdir(partDir, 0700); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	// create part file
	partPath := filepath.Join(partDir, randPartName())
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
	previous, err := s.store.AddMultipartPart(bucket, object, uid, filepath.Base(partPath), opts.PartNumber, contentMD5, contentSHA256, contentLength)
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
func (s *Sia) UploadPartCopy(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject, uploadID string, opts s3.UploadPartCopyOptions) (*s3.UploadPartCopyResult, error) {
	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return nil, s3errs.ErrNoSuchUpload
	}

	// check if the multipart upload exists
	if err := s.store.HasMultipartUpload(dstBucket, dstObject, uid); err != nil {
		return nil, err
	}

	// fetch source object metadata
	obj, err := s.store.GetObject(&accessKeyID, srcBucket, srcObject)
	if err != nil {
		return nil, err
	}

	// validate range
	if opts.Range.Length <= 0 || opts.Range.Start < 0 || opts.Range.Start >= obj.Size || opts.Range.Length > obj.Size-opts.Range.Start {
		return nil, s3errs.ErrInvalidRange
	} else if opts.Range.Length > s3.MaxUploadPartSize {
		return nil, s3errs.ErrEntityTooLarge
	}

	// fetch pinned object from the indexer
	pinnedObj, err := s.sdk.Object(ctx, obj.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch object from indexer: %w", err)
	}

	// create part directory
	partDir := filepath.Join(s.directory, uploadID, fmt.Sprintf("%d", opts.PartNumber))
	if err := os.Mkdir(partDir, 0700); errors.Is(err, os.ErrNotExist) {
		return nil, s3errs.ErrNoSuchUpload
	} else if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	// create part file
	partPath := filepath.Join(partDir, randPartName())
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

	// prepare writer and download the requested range
	md5Hash := md5.New()
	writer := &lenWriter{
		w: io.MultiWriter(partFile, md5Hash),
	}
	if err := s.sdk.Download(ctx, writer, pinnedObj, &opts.Range); err != nil {
		s.logger.Error("download failed",
			zap.Error(err),
			zap.String("bucket", srcBucket),
			zap.String("object", srcObject))
		return nil, err
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
	previous, err := s.store.AddMultipartPart(dstBucket, dstObject, uid, filepath.Base(partPath), opts.PartNumber, contentMD5, nil, contentLength)
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
		LastModified: obj.UpdatedAt,
	}, nil
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Sia) ListParts(ctx context.Context, accessKeyID, bucket, object, uploadID string, page s3.ListPartsPage) (*s3.ListPartsResult, error) {
	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return nil, s3errs.ErrNoSuchUpload
	}

	return s.store.ListParts(bucket, object, uid, page.PartNumberMarker, page.MaxParts)
}

// CompleteMultipartUpload completes a multipart upload.
func (s *Sia) CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string, parts []s3.CompletedPart) (*s3.CompleteMultipartUploadResult, error) {
	return nil, s3errs.ErrNotImplemented
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
