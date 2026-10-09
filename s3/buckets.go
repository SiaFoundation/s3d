package s3

import (
	"fmt"
	"net/http"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

var unsupportedBucketSubresources = map[string]struct{}{
	"abac":                {},
	"acl":                 {},
	"accelerate":          {},
	"analytics":           {},
	"cors":                {},
	"encryption":          {},
	"intelligent-tiering": {},
	"inventory":           {},
	"logging":             {},
	"metrics":             {},
	"notification":        {},
	"object-lock":         {},
	"ownershipControls":   {},
	"publicAccessBlock":   {},
	"replication":         {},
	"requestPayment":      {},
	"tagging":             {},
	"website":             {},
}

// routeBucket routes URLs that contain only a bucket path segment, not an
// object path segment.
func (s *s3) routeBucket(r *http.Request) (operation, error) {
	q := r.URL.Query()
	for param := range q {
		if _, ok := unsupportedBucketSubresources[param]; ok {
			return operation{}, fmt.Errorf("unsupported query subresource %q: %w", param, s3errs.ErrNotImplemented)
		}
	}

	// the subresources read or configure the bucket itself
	switch {
	case q.Has("lifecycle"):
		return s.routeBucketLifecycle(r)
	case q.Has("policy"):
		return s.routeBucketPolicy(r)
	case q.Has("policyStatus"):
		return s.routeBucketPolicyStatus(r)
	case q.Has("location"):
		if r.Method != http.MethodGet {
			return operation{}, s3errs.ErrMethodNotAllowed
		}
		return operation{action: actionGetBucketLocation, serve: s.getBucketLocation}, nil
	}

	switch r.Method {
	case http.MethodGet:
		if q.Get("list-type") == "2" {
			return operation{action: actionListBucket, serve: s.listObjectsV2}, nil
		}
		return operation{action: actionListBucket, serve: s.listObjectsV1}, nil
	case http.MethodHead:
		return operation{action: actionListBucket, serve: s.headBucket}, nil
	case http.MethodPut:
		return operation{action: actionCreateBucket, serve: s.createBucket}, nil
	case http.MethodDelete:
		return operation{action: actionDeleteBucket, serve: s.deleteBucket}, nil
	case http.MethodPost:
		if !q.Has("delete") {
			return operation{}, s3errs.ErrNotImplemented // createObjectBrowserUpload is not implemented
		}
		return operation{multiObjectDelete: true, serve: s.deleteObjects}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}

// isDefaultRegion reports whether the handler serves the S3 default region,
// either explicitly or because no region was configured.
func (s *s3) isDefaultRegion() bool {
	return s.region == "" || s.region == DefaultRegion
}

// getBucketLocation handles GET Bucket location requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (s *s3) getBucketLocation(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("getting bucket location", zap.String("bucket", access.bucket))

	// S3 reports us-east-1 as an empty LocationConstraint
	region := s.region
	if s.isDefaultRegion() {
		region = ""
	}

	return writeXMLResponse(w, http.StatusOK, GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: region,
	})
}

// createBucket handles PUT Bucket requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (s *s3) createBucket(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	bucket := access.bucket
	s.logger.Debug("creating bucket", zap.String("bucket", bucket))

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if r.URL.Query().Has("acl") {
		return s3errs.ErrNotImplemented // ACLs are not implemented
	}

	accessKeyID, err := access.assertAuth()
	if err != nil {
		return err
	}
	if err := s.backend.CreateBucket(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}

	w.Header().Set("Location", "/"+bucket)
	return nil
}

func (s *s3) deleteBucket(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("deleting bucket", zap.String("bucket", access.bucket))

	if err := s.backend.DeleteBucket(r.Context(), access.bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// headBucket handles HEAD Bucket requests. Resolving the caller's access
// already checked that the bucket exists.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (s *s3) headBucket(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("heading bucket", zap.String("bucket", access.bucket))
	return nil
}

// listBuckets handles the top-level route with no bucket or object path
// segments.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (s *s3) listBuckets(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("listing buckets")

	accessKeyID, err := access.assertAuth()
	if err != nil {
		return err
	}

	buckets, err := s.backend.ListBuckets(r.Context(), accessKeyID)
	if err != nil {
		return err
	}

	owner, err := s.backend.UserInfo(r.Context(), accessKeyID)
	if err != nil {
		return err
	}

	resp := &ListBucketsResponse{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: buckets,
		Owner:   owner,
	}
	return writeXMLResponse(w, http.StatusOK, resp)
}
