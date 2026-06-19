package s3

import (
	"net/http"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Bucket versioning states.
const (
	// VersioningStatusEnabled means new objects receive unique version IDs and
	// existing versions are retained.
	VersioningStatusEnabled = "Enabled"
	// VersioningStatusSuspended means new objects receive the null version ID while
	// previously created versions are retained.
	VersioningStatusSuspended = "Suspended"
)

// putBucketVersioning handles PUT Bucket versioning requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
func (s *s3) putBucketVersioning(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("putting bucket versioning configuration", zap.String("bucket", bucket))

	var config VersioningConfiguration
	if err := decodeXMLBody(r.Body, &config); err != nil {
		return err
	}

	// MFA delete is not supported. AWS only includes the element when it has
	// been configured, so an omitted or explicitly Disabled value is accepted.
	if config.MfaDelete == "Enabled" {
		return s3errs.ErrNotImplemented
	}

	switch config.Status {
	case VersioningStatusEnabled, VersioningStatusSuspended:
	default:
		return s3errs.ErrIllegalVersioningConfigurationException
	}

	return s.backend.PutBucketVersioning(r.Context(), accessKeyID, bucket, config.Status)
}

// getBucketVersioning handles GET Bucket versioning requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
func (s *s3) getBucketVersioning(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("getting bucket versioning configuration", zap.String("bucket", bucket))

	status, err := s.backend.GetBucketVersioning(r.Context(), accessKeyID, bucket)
	if err != nil {
		return err
	}
	// A bucket that has never been configured returns an empty
	// VersioningConfiguration with no Status element.
	return writeXMLResponse(w, http.StatusOK, VersioningConfiguration{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Status: status,
	})
}
