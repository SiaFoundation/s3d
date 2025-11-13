package s3

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strconv"

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

// initiateMultipartUploadResponse matches the XML response returned by AWS
// when creating a multipart upload.
type initiateMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// routeMultipartUpload operates on routes that contain '?uploadId=<id>' in the
// query string.
func (s *s3) routeMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object, uploadID string) error {
	if r.Method != http.MethodPut {
		return s3errs.ErrMethodNotAllowed
	}

	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	return s.addUploadPart(w, r, validatedKey, bucket, object, uploadID, r.URL.Query().Get("partNumber"))
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

	resp := initiateMultipartUploadResponse{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      object,
		UploadID: result.UploadID,
	}
	return writeXMLResponse(w, resp)
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
