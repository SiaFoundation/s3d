package multipart

import (
	"fmt"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

type (
	// Upload represents a collection of parts that make up a
	// multipart upload.
	Upload struct {
		Meta  map[string]string
		Parts []Part
	}

	// Part represents a single part of a multipart upload.
	Part struct {
		PartNumber int
		Filename   string
		Size       int64
		MD5        [16]byte
	}
)

// Validate returns an error if the multipart upload parts don't match the given
// parts. The given parts are expexted to be sorted by part number and contain
// no duplicates.
//
//	are invalid. A valid multipart upload must have contiguous part
//
// numbers starting from 1, each part must be within the size limits, and each
// part must have a non-zero size, MD5 checksum, and filename.
func (u Upload) Validate(parts []s3.CompletedPart) error {
	// assert there is at least one part and not too many parts
	if len(u.Parts) == 0 {
		return fmt.Errorf("no parts provided")
	} else if len(u.Parts) > s3.MaxUploadPartNumber {
		return fmt.Errorf("too many parts: %d (maximum is %d)", len(u.Parts), s3.MaxUploadPartNumber)
	} else if len(u.Parts) != len(parts) {
		return fmt.Errorf("mismatched number of parts: %d in upload, %d provided", len(u.Parts), len(parts))
	}

	for i, part := range u.Parts {
		// assert part numbers are contiguous
		expectedPartNumber := i + 1
		if part.PartNumber != expectedPartNumber {
			return fmt.Errorf("missing part number %d", expectedPartNumber)
		}

		// assert part size limits
		if i < len(u.Parts)-1 {
			if part.Size < s3.MinUploadPartSize {
				return fmt.Errorf("part %d is too small: %d bytes (minimum is %d bytes); %w", part.PartNumber, part.Size, s3.MinUploadPartSize, s3errs.ErrEntityTooSmall)
			}
		}
		if part.Size > s3.MaxUploadPartSize {
			return fmt.Errorf("part %d is too large: %d bytes (maximum is %d bytes); %w", part.PartNumber, part.Size, s3.MaxUploadPartSize, s3errs.ErrEntityTooLarge)
		} else if part.Size == 0 {
			return fmt.Errorf("part %d has zero size; %w", part.PartNumber, s3errs.ErrInvalidPart)
		}

		// assert MD5 matches provided parts and is non-zero
		if part.MD5 != parts[i].ETag {
			return fmt.Errorf("part %d has mismatched MD5 checksum; %w", part.PartNumber, s3errs.ErrInvalidPart)
		} else if part.MD5 == [16]byte{} {
			return fmt.Errorf("part %d has no MD5 checksum; %w", part.PartNumber, s3errs.ErrInvalidPart)
		}

		// assert filename is set
		if part.Filename == "" {
			return fmt.Errorf("part %d has no filename", part.PartNumber)
		}
	}
	return nil
}
