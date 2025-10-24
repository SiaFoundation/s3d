package s3

import (
	"net/http"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// routeBucket handles URLs that contain only a bucket path segment, not an
// object path segment.
func (s *s3) routeBucket(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	switch r.Method {
	case http.MethodGet:
		// nolint:gocritic
		if _, ok := r.URL.Query()["location"]; ok {
			return s3errs.ErrNotImplemented // getBucketLocation is not implemented
		} else {
			return s3errs.ErrNotImplemented // listBucket is not implemented
		}
	case http.MethodPut:
		return s.createBucket(w, r, validatedKey, bucket)
	case http.MethodDelete:
		return s.deleteBucket(w, r, validatedKey, bucket)
	case http.MethodHead:
		return s3errs.ErrNotImplemented // headBucket is not implemented
	case http.MethodPost:
		// nolint:gocritic
		if _, ok := r.URL.Query()["delete"]; ok {
			return s3errs.ErrNotImplemented // deleteMulti is not implemented
		} else {
			return s3errs.ErrNotImplemented // createObjectBrowserUpload is not implemented
		}
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// createBucket handles PUT Bucket requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (s *s3) createBucket(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("creating bucket", zap.String("bucket", bucket))

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if err := s.backend.CreateBucket(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}

	w.Header().Set("Location", "/"+bucket)
	return nil
}

func (s *s3) deleteBucket(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("deleting bucket", zap.String("bucket", bucket))

	if err := s.backend.DeleteBucket(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// listBuckets handles the top-level route with no bucket or object path
// segments.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (s *s3) listBuckets(w http.ResponseWriter, r *http.Request, accessKeyID *string) error {
	s.logger.Debug("listing buckets")

	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	buckets, err := s.backend.ListBuckets(r.Context(), validatedKey)
	if err != nil {
		return err
	}

	resp := &ListBucketsResponse{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: buckets,
		Owner:   globalUserInfo,
	}
	return writeXMLResponse(w, resp)
}

func (s *s3) listBucketVersions(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket string) error {
	return s3errs.ErrNotImplemented
}
