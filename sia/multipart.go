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
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/multipart"
	"github.com/SiaFoundation/s3d/sia/objects"
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
func (s *Sia) ListMultipartUploads(ctx context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions) (*s3.ListMultipartUploadsResult, error) {
	return nil, s3errs.ErrNotImplemented
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
	// check bucket access
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return nil, s3errs.ErrInvalidArgument
	}

	return s.store.ListParts(accessKeyID, bucket, object, uid, page.PartNumberMarker, page.MaxParts)
}

// CompleteMultipartUpload completes a multipart upload.
//
// NOTE: the given parts slice is expected to have passed a validation step
// already, asserting the part numbers and ETags are correct, the backend still
// validates that only the last part can be smaller than MinUploadPartSize.
func (s *Sia) CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string, parts []s3.CompletedPart) (*s3.CompleteMultipartUploadResult, error) {
	// check bucket access
	if err := s.store.HeadBucket(accessKeyID, bucket); err != nil {
		return nil, err
	}

	// parse upload ID
	uid, err := s3.UploadIDFromString(uploadID)
	if err != nil {
		return nil, s3errs.ErrInvalidArgument
	}

	// get multipart upload
	upload, err := s.store.MultipartUpload(bucket, object, uid)
	if err != nil {
		return nil, err
	}

	// update parts based on the provided completed parts
	if len(parts) > len(upload.Parts) {
		return nil, s3errs.ErrInvalidArgument
	} else if len(parts) != len(upload.Parts) {
		upload.Parts = upload.Parts[:len(parts)]
	}

	// validate parts
	for i, part := range upload.Parts {
		if part.PartNumber != parts[i].PartNumber {
			return nil, s3errs.ErrInvalidPart
		}
		if part.MD5 != parts[i].ETag {
			return nil, s3errs.ErrBadDigest
		}
		lastPart := i == len(parts)-1
		if !lastPart && part.Size < s3.MinUploadPartSize {
			return nil, s3errs.ErrEntityTooSmall
		}
	}

	// assert the upload directory exists
	uploadDir := filepath.Join(s.directory, uploadID)
	if _, err := os.Stat(uploadDir); err != nil {
		return nil, fmt.Errorf("failed to stat upload directory: %w", err)
	}

	// assert all part files exist
	for _, part := range upload.Parts {
		partPath := filepath.Join(uploadDir, fmt.Sprintf("%d", part.PartNumber), part.Filename)
		if _, err := os.Stat(partPath); err != nil {
			return nil, fmt.Errorf("failed to stat part file %d: %w", part.PartNumber, err)
		}
	}

	// upload the combined object to Sia
	r := multipart.NewReader(upload, uploadDir)
	obj, err := s.sdk.Upload(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("failed to upload object to Sia: %w", err)
	}
	contentMD5 := r.MD5Sum()

	// store the object in the database
	if err := s.store.PutObject(accessKeyID, bucket, object, &objects.Object{
		ID:         obj.ID(),
		ContentMD5: contentMD5,
		Meta:       upload.Meta,
		Size:       r.Size(),
		UpdatedAt:  time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("failed to store object metadata: %w", err)
	}

	// complete the multipart upload in the database
	if err := s.store.CompleteMultipartUpload(bucket, object, uid, upload.Parts); err != nil {
		return nil, fmt.Errorf("failed to complete multipart upload in store: %w", err)
	}

	// remove multipart upload directory
	if err := os.RemoveAll(uploadDir); err != nil {
		s.logger.Error("failed to remove multipart upload directory after completion",
			zap.String("path", uploadDir),
			zap.Error(err))
	}

	return &s3.CompleteMultipartUploadResult{
		ETag:       s3.FormatETag(contentMD5[:]),
		ContentMD5: contentMD5,
	}, nil
}
