package s3

import (
	"errors"
	"net/http"

	"go.uber.org/zap"
)

// routeBucket handles URLs that contain only a bucket path segment, not an
// object path segment.
func (s *s3) routeBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	switch r.Method {
	case "GET":
		if _, ok := r.URL.Query()["location"]; ok {
			return errors.New("getBucketLocation is not implemented")
		} else {
			return errors.New("listBucket is not implemented")
		}
	case "PUT":
		return s.createBucket(w, r, bucket)
	case "DELETE":
		return errors.New("deleteBucket is not implemented")
	case "HEAD":
		return errors.New("headBucket is not implemented")
	case "POST":
		if _, ok := r.URL.Query()["delete"]; ok {
			return errors.New("deleteMulti is not implemented")
		} else {
			return errors.New("createObjectBrowserUpload is not implemented")
		}
	default:
		panic("TODO: implement S3 compliant error handling")
	}
}

// createBucket handles PUT Bucket requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (s *s3) createBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	s.logger.Debug("creating bucket", zap.String("bucket", bucket))

	// TODO: Validate bucket name once we have proper errors to return

	if err := s.backend.CreateBucket(r.Context(), bucket); err != nil {
		return err
	}

	w.Header().Set("Location", "/"+bucket)
	return nil
}

// listBuckets handles the top-level route with no bucket or object path
// segments.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (s *s3) listBuckets(w http.ResponseWriter, r *http.Request) error {
	s.logger.Debug("listing buckets")

	buckets, err := s.backend.ListBuckets(r.Context())
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
