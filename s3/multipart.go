package s3

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/SiaFoundation/s3d/s3/auth"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// CreateMultipartUploadOptions contains options for initiating a multipart
// upload.
type CreateMultipartUploadOptions struct {
	Meta map[string]string
}

// CreateMultipartUploadResult returns an upload ID for a newly created
// multipart upload. This ID is used to identify the multipart upload in
// subsequent requests.
type CreateMultipartUploadResult struct {
	UploadID string
}

// UploadPartOptions contains options for uploading an individual part in a
// multipart upload.
type UploadPartOptions struct {
	PartNumber    int
	ContentLength int64
	ContentMD5    *[16]byte
	ContentSHA256 *[32]byte
}

// UploadPartResult contains metadata about an uploaded part, such as the
// computed MD5 checksum.
type UploadPartResult struct {
	ContentMD5 [16]byte
}

// CompletedPart represents a single part referenced during a multipart
// completion request.
type CompletedPart struct {
	PartNumber int
	ETag       [16]byte
}

// CompleteMultipartUploadResult contains metadata about the completed object,
// such as the final ETag.
type CompleteMultipartUploadResult struct {
	ETag       string
	ContentMD5 [16]byte
}

// routeMultipartUpload operates on routes that contain '?uploadId=<id>' in the
// query string.
func (s *s3) routeMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object, uploadID string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	switch r.Method {
	case http.MethodPut:
		return s.addUploadPart(w, r, validatedKey, bucket, object, uploadID, r.URL.Query().Get("partNumber"))
	case http.MethodPost:
		return s.completeMultipartUpload(w, r, validatedKey, bucket, object, uploadID)
	case http.MethodDelete:
		return s.abortMultipartUpload(w, r, validatedKey, bucket, object, uploadID)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// routeMultipartUploadBase operates on routes that contain '?uploads' in the
// query string. These routes may or may not have a value for bucket or object;
// this is validated and handled in the target handler functions.
func (s *s3) routeMultipartUploadBase(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string) error {
	if r.Method != http.MethodPost {
		return s3errs.ErrMethodNotAllowed
	}

	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	return s.createMultipartUpload(w, r, validatedKey, bucket, object)
}

func (s *s3) abortMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object, uploadID string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("uploadID", uploadID),
	)
	log.Debug("abort multipart upload")

	if err := s.backend.AbortMultipartUpload(r.Context(), accessKeyID, bucket, object, uploadID); err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *s3) createMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
	)
	log.Debug("create multipart upload")

	// check key length
	if len(object) > KeySizeLimit {
		return s3errs.ErrKeyTooLongError
	}

	// extract metadata headers
	meta, err := metadataHeaders(r.Header, MetadataSizeLimit)
	if err != nil {
		return err
	}

	result, err := s.backend.CreateMultipartUpload(r.Context(), accessKeyID, bucket, object, CreateMultipartUploadOptions{
		Meta: meta,
	})
	if err != nil {
		return err
	}

	return writeXMLResponse(w, InitiateMultipartUploadResponse{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      object,
		UploadID: result.UploadID,
	})
}

func (s *s3) addUploadPart(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object, uploadID, partNumberStr string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("uploadID", uploadID),
		zap.String("partNumber", partNumberStr),
	)
	log.Debug("upload multipart part")

	partNumber, err := parsePartNumber(partNumberStr)
	if err != nil {
		return err
	}

	// content length is mandatory
	if r.ContentLength < 0 {
		return s3errs.ErrMissingContentLength
	} else if r.ContentLength > MaxUploadPartSize {
		return s3errs.ErrEntityTooLarge
	}

	// extract Content-MD5 header
	var contentMD5 *[16]byte
	if md5Header := r.Header.Get("Content-Md5"); md5Header != "" {
		contentMD5 = new([16]byte)
		if n, err := base64.StdEncoding.Decode(contentMD5[:], []byte(md5Header)); err != nil || n != len(contentMD5) {
			return s3errs.ErrInvalidDigest
		}
	}

	// extract SHA256 checksum from "X-Amz-Content-Sha256" header if present
	contentSHA256, err := auth.Sha256HashFromRequest(r)
	if err != nil {
		return err
	}

	res, err := s.backend.UploadPart(r.Context(), accessKeyID, bucket, object, uploadID, r.Body, UploadPartOptions{
		PartNumber:    partNumber,
		ContentLength: r.ContentLength,
		ContentMD5:    contentMD5,
		ContentSHA256: contentSHA256,
	})
	if err != nil {
		return err
	}

	w.Header().Set("ETag", FormatETag(res.ContentMD5[:]))
	return nil
}

func (s *s3) completeMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object, uploadID string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("uploadID", uploadID),
	)
	log.Debug("complete multipart upload")

	var req CompleteMultipartUploadRequest
	if err := decodeXMLBody(r.Body, &req); err != nil {
		return err
	}

	completedParts, err := parseCompletedParts(req.Parts)
	if err != nil {
		return err
	}

	res, err := s.backend.CompleteMultipartUpload(r.Context(), accessKeyID, bucket, object, uploadID, completedParts)
	if err != nil {
		return err
	}

	protocol := "http"
	if r.TLS != nil {
		protocol = "https"
	}

	var location string
	if len(s.hostBucketBases) > 0 {
		location = fmt.Sprintf("%s://%s/%s", protocol, r.Host, object)
	} else {
		location = fmt.Sprintf("%s://%s/%s/%s", protocol, r.Host, bucket, object)
	}

	w.Header().Set("ETag", res.ETag)
	return writeXMLResponse(w, CompleteMultipartUploadResponse{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: location,
		Bucket:   bucket,
		Key:      object,
		ETag:     res.ETag,
	})
}

func parsePartNumber(partStr string) (int, error) {
	if partStr == "" {
		return 0, s3errs.ErrInvalidRequest
	}
	partNumber, err := strconv.Atoi(partStr)
	if err != nil {
		return 0, s3errs.ErrInvalidArgument
	}
	if partNumber < 1 || partNumber > MaxUploadPartNumber {
		return 0, s3errs.ErrInvalidArgument
	}
	return partNumber, nil
}

func parseCompletedParts(parts []CompleteMultipartPartXML) ([]CompletedPart, error) {
	if len(parts) == 0 {
		return nil, s3errs.ErrInvalidRequest
	}

	completed := make([]CompletedPart, 0, len(parts))
	prev := 0
	for i, part := range parts {
		if part.PartNumber < 1 || part.PartNumber > MaxUploadPartNumber {
			return nil, s3errs.ErrInvalidArgument
		}
		if i > 0 && part.PartNumber <= prev {
			return nil, s3errs.ErrInvalidPartOrder
		}

		etag, err := parseCompletedPartETag(part.ETag)
		if err != nil {
			return nil, err
		}

		completed = append(completed, CompletedPart{
			PartNumber: part.PartNumber,
			ETag:       etag,
		})
		prev = part.PartNumber
	}

	return completed, nil
}

func parseCompletedPartETag(etagStr string) (etag [16]byte, _ error) {
	etagStr = strings.TrimSpace(etagStr)
	etagStr = strings.Trim(etagStr, `"`)
	if etagStr == "" {
		return etag, s3errs.ErrInvalidArgument
	}

	decoded, err := hex.DecodeString(etagStr)
	if err != nil || len(decoded) != len(etag) {
		return etag, s3errs.ErrInvalidDigest
	}
	copy(etag[:], decoded)
	return etag, nil
}

// FormatMultipartETag formats the MD5 checksum of concatenated part hashes
// along with the part count to match AWS multipart ETag semantics.
func FormatMultipartETag(hash []byte, partCount int) string {
	return `"` + hex.EncodeToString(hash) + "-" + strconv.Itoa(partCount) + `"`
}
