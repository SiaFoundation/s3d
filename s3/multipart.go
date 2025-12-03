package s3

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

// UploadPartCopyOptions contains options for copying an individual part in a
// multipart upload.
type UploadPartCopyOptions struct {
	PartNumber int
	Range      ObjectRange
}

// UploadPartResult contains metadata about an uploaded part, such as the
// computed MD5 checksum.
type UploadPartResult struct {
	ContentMD5   [16]byte
	LastModified time.Time
}

// UploadPartCopyResult contains metadata about a copied part, such as the
// computed MD5 checksum and the object's last modification time.
type UploadPartCopyResult struct {
	ContentMD5   [16]byte
	LastModified time.Time
}

// ListPartsPage specifies pagination options when listing the parts of an
// in-progress multipart upload.
type ListPartsPage struct {
	PartNumberMarker int
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

// ListMultipartUploadsOptions contains options for listing in-progress
// multipart uploads for a bucket.
type ListMultipartUploadsOptions struct {
	Prefix         string
	Delimiter      string
	KeyMarker      string
	UploadIDMarker string
	MaxUploads     int64
}

// MultipartUploadInfo represents a single multipart upload in a listing.
type MultipartUploadInfo struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

// ListMultipartUploadsResult contains the uploads returned by the backend
// along with pagination metadata.
type ListMultipartUploadsResult struct {
	Uploads            []MultipartUploadInfo
	CommonPrefixes     []string
	IsTruncated        bool
	NextKeyMarker      string
	NextUploadIDMarker string
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
		return s.addUploadPart(w, r, validatedKey, bucket, object, uploadID)
	case http.MethodGet:
		return s.listUploadParts(w, r, validatedKey, bucket, object, uploadID)
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
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	switch r.Method {
	case http.MethodPost:
		return s.createMultipartUpload(w, r, validatedKey, bucket, object)
	case http.MethodGet:
		return s.listMultipartUploads(w, r, validatedKey, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
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

func (s *s3) listMultipartUploads(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	if bucket == "" {
		return s3errs.ErrInvalidRequest
	}

	query := r.URL.Query()
	maxUploads, err := parseClampedInt(query.Get("max-uploads"), DefaultMaxMultipartUploads, 0, MaxMultipartUploads)
	if err != nil {
		return err
	}

	opts := ListMultipartUploadsOptions{
		Prefix:         query.Get("prefix"),
		Delimiter:      query.Get("delimiter"),
		KeyMarker:      query.Get("key-marker"),
		UploadIDMarker: query.Get("upload-id-marker"),
		MaxUploads:     maxUploads,
	}

	result, err := s.backend.ListMultipartUploads(r.Context(), accessKeyID, bucket, opts)
	if err != nil {
		return err
	}

	resp := ListMultipartUploadsResponse{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:             bucket,
		Prefix:             opts.Prefix,
		Delimiter:          opts.Delimiter,
		KeyMarker:          opts.KeyMarker,
		UploadIDMarker:     opts.UploadIDMarker,
		MaxUploads:         maxUploads,
		IsTruncated:        result.IsTruncated,
		NextKeyMarker:      result.NextKeyMarker,
		NextUploadIDMarker: result.NextUploadIDMarker,
	}

	for _, cp := range result.CommonPrefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, CommonPrefix{Prefix: cp})
	}

	for _, upload := range result.Uploads {
		resp.Uploads = append(resp.Uploads, ListedMultipartUpload{
			Key:          upload.Key,
			UploadID:     upload.UploadID,
			Initiator:    globalUserInfo,
			Owner:        globalUserInfo,
			StorageClass: StorageClass("STANDARD"),
			Initiated:    NewContentTime(upload.Initiated),
		})
	}

	return writeXMLResponse(w, resp)
}

func (s *s3) copyPart(w http.ResponseWriter, r *http.Request, accessKeyID, dstBucket, dstObject, uploadID string, partNumber int) error {
	source := r.Header.Get("X-Amz-Copy-Source")
	rnge := r.Header.Get("X-Amz-Copy-Source-Range")
	log := s.logger.With(zap.String("dstBucket", dstBucket),
		zap.String("dstObject", dstObject),
		zap.String("uploadID", uploadID),
		zap.String("source", source),
		zap.String("range", rnge),
		zap.Int("partNumber", partNumber),
	)
	log.Debug("copy part")

	// parse source
	srcBucket, srcObject, err := parseSource(source)
	if err != nil {
		return err
	}

	// fetch source metadata to determine size and validate range
	obj, err := s.backend.HeadObject(r.Context(), &accessKeyID, srcBucket, srcObject, nil, nil)
	if err != nil {
		return err
	} else if obj.Body != nil {
		obj.Body.Close()
	}

	// parse range
	objRange, err := parseRange(rnge, obj.Size)
	if err != nil {
		return err
	}

	result, err := s.backend.UploadPartCopy(r.Context(), accessKeyID, srcBucket, srcObject, dstBucket, dstObject, uploadID, UploadPartCopyOptions{
		PartNumber: partNumber,
		Range:      objRange,
	})
	if err != nil {
		return err
	}

	etag := FormatETag(result.ContentMD5[:])
	w.Header().Set("ETag", etag)
	return writeXMLResponse(w, PartCopyResult{
		ETag:         etag,
		LastModified: NewContentTime(result.LastModified),
	})
}

func (s *s3) addUploadPart(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object, uploadID string) error {
	log := s.logger.With(
		zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("uploadID", uploadID),
		zap.String("partNumber", r.URL.Query().Get("partNumber")),
	)
	log.Debug("upload multipart part")

	// parse part number
	partNumber, err := parsePartNumber(r.URL.Query().Get("partNumber"))
	if err != nil {
		return err
	} else if partNumber == nil {
		return s3errs.ErrInvalidRequest
	}

	// copy part
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		return s.copyPart(w, r, accessKeyID, bucket, object, uploadID, int(*partNumber))
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
		PartNumber:    int(*partNumber),
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
	resp := ListPartsResponse{
		Xmlns:            "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:           bucket,
		Key:              object,
		UploadID:         uploadID,
		PartNumberMarker: page.PartNumberMarker,
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

func parseCompletedParts(parts []CompleteMultipartPartXML) ([]CompletedPart, error) {
	if len(parts) == 0 {
		return nil, s3errs.ErrInvalidRequest
	}

	completed := make([]CompletedPart, 0, len(parts))
	prev := 0
	for i, part := range parts {
		if part.PartNumber < 1 || part.PartNumber > MaxUploadPartNumber {
			return nil, s3errs.ErrInvalidArgument // invalid part number
		}
		if i > 0 && part.PartNumber < prev {
			return nil, s3errs.ErrInvalidPartOrder // invalid part order
		}
		prev = part.PartNumber

		etag, err := parseCompletedPartETag(part.ETag)
		if err != nil {
			return nil, err
		}

		if len(completed) > 0 && completed[len(completed)-1].PartNumber == part.PartNumber {
			completed[len(completed)-1] = CompletedPart{ // overwrite duplicate part number
				PartNumber: part.PartNumber,
				ETag:       etag,
			}
		} else {
			completed = append(completed, CompletedPart{
				PartNumber: part.PartNumber,
				ETag:       etag,
			})
		}
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

func parsePartNumber(s string) (*int32, error) {
	if s == "" {
		return nil, nil
	}

	partNumber, err := strconv.Atoi(s)
	if err != nil {
		return nil, s3errs.ErrInvalidArgument
	}
	if partNumber < 1 || partNumber > MaxUploadPartNumber {
		return nil, s3errs.ErrInvalidArgument
	}
	val := int32(partNumber)
	return &val, nil
}

// FormatMultipartETag formats the MD5 checksum of concatenated part hashes
// along with the part count to match AWS multipart ETag semantics.
func FormatMultipartETag(hash []byte, partCount int) string {
	return `"` + hex.EncodeToString(hash) + "-" + strconv.Itoa(partCount) + `"`
}

// parseRange validates the X-Amz-Copy-Source-Range header. It only allows a
// single range of the form "bytes=start-end" and returns ErrInvalidArgument for
// malformed headers or ErrInvalidRange if the range exceeds the source object
// size.
func parseRange(header string, size int64) (ObjectRange, error) {
	header = strings.TrimSpace(header)

	if size <= 0 {
		return ObjectRange{}, s3errs.ErrInvalidRange
	} else if header == "" {
		return ObjectRange{Start: 0, Length: size}, nil
	}

	var start, end int64
	var suffix string
	_, err := fmt.Sscanf(header, "bytes=%d-%d%s", &start, &end, &suffix)
	if err != nil && !errors.Is(err, io.EOF) {
		return ObjectRange{}, s3errs.ErrInvalidArgument
	}

	if suffix != "" {
		return ObjectRange{}, s3errs.ErrInvalidArgument
	} else if start < 0 || end < start {
		return ObjectRange{}, s3errs.ErrInvalidArgument
	} else if end >= size {
		return ObjectRange{}, s3errs.ErrInvalidRange
	}

	return ObjectRange{
		Start:  start,
		Length: end - start + 1,
	}, nil
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
		page.PartNumberMarker = val
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
