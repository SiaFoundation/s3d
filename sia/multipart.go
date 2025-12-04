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
	"lukechampine.com/frand"
)

func randUUID() (uuid [8]byte) {
	frand.Read(uuid[:])
	return
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

	return &s3.CreateMultipartUploadResult{
		UploadID: uploadID,
	}, nil
}

// ListMultipartUploads lists in-progress multipart uploads.
func (s *Sia) ListMultipartUploads(ctx context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions) (*s3.ListMultipartUploadsResult, error) {
	return nil, s3errs.ErrNotImplemented
}

// AbortMultipartUpload aborts a multipart upload.
func (s *Sia) AbortMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string) error {
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return err
	}

	// abort the multipart upload in the database
	if err := s.store.AbortMultipartUpload(bucket, object, uploadID); err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	return nil
}

// UploadPart uploads a single multipart part.
func (s *Sia) UploadPart(ctx context.Context, accessKeyID, bucket, object, uploadID string, r io.Reader, opts s3.UploadPartOptions) (_ *s3.UploadPartResult, err error) {
	// quick check if the bucket exists
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// verify multipart upload exists
	if err := s.store.HasMultipartUpload(bucket, object, uploadID); err != nil {
		return nil, err
	}

	// add part metadata to the database
	if err := s.store.AddMultipartPart(uploadID, opts.PartNumber); err != nil {
		return nil, fmt.Errorf("failed to add multipart part: %w", err)
	}

	// create part directory
	partDir := filepath.Join(s.directory, uploadID)
	if err := os.MkdirAll(partDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	// prepare part file locations, we upload to a temporary file first and then
	// rename the file once the upload is complete, it's possible parts with the
	// same part number are uploaded concurrently
	partPathTmp := filepath.Join(partDir, fmt.Sprintf("%d-%x.part.tmp", opts.PartNumber, randUUID()))
	partPathFinal := filepath.Join(partDir, fmt.Sprintf("%d.part", opts.PartNumber))

	// create temporary part file
	partFileTmp, err := os.Create(partPathTmp)
	if err != nil {
		return nil, fmt.Errorf("failed to create part file: %w", err)
	}
	defer partFileTmp.Close()

	// defer a best effort cleanup on error
	defer func() {
		if err != nil {
			_ = os.Remove(partPathTmp)
			_ = os.Remove(partPathFinal)
		}
	}()

	// prepare writers
	md5Hash := md5.New()
	writers := []io.Writer{partFileTmp, md5Hash}

	var sha256Hash hash.Hash
	if opts.ContentSHA256 != nil {
		sha256Hash = sha256.New()
		writers = append(writers, sha256Hash)
	}

	// copy data and validate size
	buf := make([]byte, 64*1024) // 64 KiB buffer for efficient copying
	contentLength, err := io.CopyBuffer(io.MultiWriter(writers...), io.LimitReader(r, s3.MaxUploadPartSize+1), buf)
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

	// TODO: If a part already exists, we could preserve it by moving the current
	// file aside, write the new upload, and roll back on DB failure. If the DB
	// update succeeds, delete the old file from its temporary location.

	// sync and rename part file
	if err := errors.Join(partFileTmp.Sync(), os.Rename(partPathTmp, partPathFinal)); err != nil {
		return nil, fmt.Errorf("failed to sync and rename part file: %w", err)
	}

	// sync parent directory
	dir, err := os.Open(partDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open multipart tmp dir: %w", err)
	} else if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, fmt.Errorf("failed to sync multipart part file and dir: %w", err)
	}

	// finalize part in the database
	if err := s.store.FinishMultipartPart(uploadID, opts.PartNumber, contentMD5, contentSHA256, contentLength); err != nil {
		return nil, fmt.Errorf("failed to finish multipart part: %w", err)
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
