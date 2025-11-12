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
			return s.bucketLocation(w, r, bucket)
		} else {
			return s.listObjectsV2(w, r, accessKeyID, bucket)
		}
	case http.MethodPut:
		return s.createBucket(w, r, validatedKey, bucket)
	case http.MethodDelete:
		return s.deleteBucket(w, r, validatedKey, bucket)
	case http.MethodHead:
		return s.headBucket(w, r, validatedKey, bucket)
	case http.MethodPost:
		// nolint:gocritic
		if _, ok := r.URL.Query()["delete"]; ok {
			return s.deleteObjects(w, r, *accessKeyID, bucket)
		} else {
			return s3errs.ErrNotImplemented // createObjectBrowserUpload is not implemented
		}
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// bucketLocation handles GET Bucket location requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (s *s3) bucketLocation(w http.ResponseWriter, r *http.Request, bucket string) error {
	s.logger.Debug("getting bucket location", zap.String("bucket", bucket))

	region := s.region
	if region == "" {
		// Per AWS S3 API, "null" is used for the us-east-1 region. So we use it
		// here as a default as well.
		region = Null
	}

	return writeXMLResponse(w, GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: region,
	})
}

// createBucket handles PUT Bucket requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (s *s3) createBucket(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("creating bucket", zap.String("bucket", bucket))

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	q := r.URL.Query()
	if _, hasACL := q["acl"]; hasACL {
		return s3errs.ErrNotImplemented // ACLs are not implemented
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

// headBucket handles HEAD Bucket requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (s *s3) headBucket(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("heading bucket", zap.String("bucket", bucket))
	return s.backend.HeadBucket(r.Context(), accessKeyID, bucket)
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
