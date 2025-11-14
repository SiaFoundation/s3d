package s3

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"time"

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

// ListPartsPage specifies pagination options when listing the parts of an
// in-progress multipart upload.
type ListPartsPage struct {
	PartNumberMarker string
	MaxParts         int64
}

// ListPartsResult contains metadata about uploaded parts.
type ListPartsResult struct {
	Parts                []UploadPart
	IsTruncated          bool
	NextPartNumberMarker string
	OwnerID              string
	OwnerDisplayName     string
	StorageClass         StorageClass
	InitiatorID          string
	InitiatorDisplayName string
}

// UploadPart represents a single uploaded part that can be returned by
// ListParts.
type UploadPart struct {
	PartNumber   int
	LastModified time.Time
	Size         int64
	ContentMD5   [16]byte
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
	case http.MethodGet:
		return s.listUploadParts(w, r, validatedKey, bucket, object, uploadID)
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

func (s *s3) listUploadParts(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object, uploadID string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("uploadID", uploadID),
	)
	log.Debug("list multipart parts")

	// parse pagination options
	page, err := listPartsPageFromQuery(r.URL.Query())
	if err != nil {
		return err
	}

	// list parts
	result, err := s.backend.ListParts(r.Context(), accessKeyID, bucket, object, uploadID, page)
	if err != nil {
		return err
	}

	// build response
	partNumberMarker, _ := strconv.Atoi(page.PartNumberMarker) // already validated
	resp := ListPartsResponse{
		Xmlns:            "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:           bucket,
		Key:              object,
		UploadID:         uploadID,
		PartNumberMarker: partNumberMarker,
		MaxParts:         page.MaxParts,
		IsTruncated:      result.IsTruncated,
		StorageClass:     result.StorageClass,
	}
	if result.IsTruncated && result.NextPartNumberMarker != "" {
		if next, err := strconv.Atoi(result.NextPartNumberMarker); err == nil {
			resp.NextPartNumberMarker = next
		}
	}

	resp.Owner = globalUserInfo
	if result.OwnerID != "" || result.OwnerDisplayName != "" {
		resp.Owner = &UserInfo{
			ID:          result.OwnerID,
			DisplayName: result.OwnerDisplayName,
		}
	}
	resp.Initiator = globalUserInfo
	if result.InitiatorID != "" || result.InitiatorDisplayName != "" {
		resp.Initiator = &UserInfo{
			ID:          result.InitiatorID,
			DisplayName: result.InitiatorDisplayName,
		}
	}

	for _, part := range result.Parts {
		resp.Parts = append(resp.Parts, ListedPartResponse{
			PartNumber:   part.PartNumber,
			LastModified: NewContentTime(part.LastModified),
			ETag:         FormatETag(part.ContentMD5[:]),
			Size:         part.Size,
		})
	}

	return writeXMLResponse(w, resp)
}

func parsePartNumber(raw string) (int, error) {
	if raw == "" {
		return 0, s3errs.ErrInvalidRequest
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, s3errs.ErrInvalidArgument
	}
	if val < 1 || val > MaxUploadPartNumber {
		return 0, s3errs.ErrInvalidArgument
	}
	return val, nil
}

func listPartsPageFromQuery(query url.Values) (ListPartsPage, error) {
	page := ListPartsPage{
		MaxParts: DefaultMaxUploadListParts,
	}

	if rawMarker := query.Get("part-number-marker"); rawMarker != "" {
		val, err := strconv.Atoi(rawMarker)
		if err != nil {
			return page, s3errs.ErrInvalidArgument
		}
		if val < 0 || val >= MaxUploadPartNumber {
			return page, s3errs.ErrInvalidArgument
		}
		page.PartNumberMarker = rawMarker
	}

	if rawMax := query.Get("max-parts"); rawMax != "" {
		val, err := strconv.Atoi(rawMax)
		if err != nil {
			return page, s3errs.ErrInvalidArgument
		}
		if val < 1 || val > MaxUploadListParts {
			return page, s3errs.ErrInvalidArgument
		}
		page.MaxParts = int64(val)
	}

	return page, nil
}
