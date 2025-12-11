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
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// create multipart upload in the database
	uploadID, err := s.store.CreateMultipartUpload(bucket, object, opts.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	// create multipart upload directory
	if err := os.Mkdir(filepath.Join(s.directory, uploadID), 0700); err != nil {
		if abortErr := s.store.AbortMultipartUpload(bucket, object, uploadID); abortErr != nil {
			s.logger.Error("failed to abort multipart upload after directory creation failure",
				zap.String("bucket", bucket),
				zap.String("object", object),
				zap.String("uploadID", uploadID),
				zap.Error(abortErr))
		}
		return nil, fmt.Errorf("failed to create upload directory: %w", err)
	}

	return &s3.CreateMultipartUploadResult{
		UploadID: uploadID,
	}, nil
}

// ListMultipartUploads lists in-progress multipart uploads.
func (s *Sia) ListMultipartUploads(ctx context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions) (*s3.ListMultipartUploadsResult, error) {
	// assert auth
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	return s.store.ListMultipartUploads(bucket, opts.Prefix, opts.Delimiter, opts.KeyMarker, opts.UploadIDMarker, opts.MaxUploads)
}

// AbortMultipartUpload aborts a multipart upload.
func (s *Sia) AbortMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string) error {
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return err
	}

	// remove multipart upload directory
	uploadDir := filepath.Join(s.directory, uploadID)
	if err := os.RemoveAll(uploadDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("failed to remove multipart upload directory",
			zap.String("path", uploadDir),
			zap.Error(err))
	}

	// abort the multipart upload in the database
	if err := s.store.AbortMultipartUpload(bucket, object, uploadID); err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	return nil
}

// UploadPart uploads a single multipart part.
func (s *Sia) UploadPart(ctx context.Context, accessKeyID, bucket, object, uploadID string, r io.Reader, opts s3.UploadPartOptions) (_ *s3.UploadPartResult, err error) {
	// check auth and bucket existence
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// check if the multipart upload exists
	if err := s.store.HasMultipartUpload(bucket, object, uploadID); err != nil {
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
	previous, err := s.store.AddMultipartPart(bucket, object, uploadID, filepath.Base(partPath), opts.PartNumber, contentMD5, contentSHA256, contentLength)
	if err != nil {
		if err := os.Remove(partPath); err != nil {
			s.logger.Error("failed to remove part file",
				zap.String("path", partPath),
				zap.Error(err))
		}
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
	return nil, s3errs.ErrNotImplemented
}

// ListParts lists uploaded parts for a multipart upload.
func (s *Sia) ListParts(ctx context.Context, accessKeyID, bucket, object, uploadID string, page s3.ListPartsPage) (*s3.ListPartsResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// CompleteMultipartUpload completes a multipart upload.
func (s *Sia) CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string, parts []s3.CompletedPart) (*s3.CompleteMultipartUploadResult, error) {
	return nil, s3errs.ErrNotImplemented
}
