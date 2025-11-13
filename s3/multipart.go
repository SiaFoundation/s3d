package s3

import (
	"net/http"

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

// routeMultipartUpload operates on routes that contain '?uploadId=<id>' in the
// query string.
func (s *s3) routeMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object, uploadID string) error {
	return s3errs.ErrNotImplemented
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

func (s *s3) createMultipartUpload(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket, object string) error {
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
