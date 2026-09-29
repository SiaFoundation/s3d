package s3

import (
	"fmt"
	"net/http"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Object lock retention modes.
const (
	// ObjectLockModeGovernance protects a version from everyone who does not
	// send x-amz-bypass-governance-retention.
	ObjectLockModeGovernance = "GOVERNANCE"
	// ObjectLockModeCompliance protects a version from everyone until its
	// retain until date passes.
	ObjectLockModeCompliance = "COMPLIANCE"
)

// Object lock wire values and the retention limits AWS enforces.
const (
	ObjectLockEnabled             = "Enabled"
	HeaderBucketObjectLockEnabled = "X-Amz-Bucket-Object-Lock-Enabled"

	maxRetentionDays  = 36500
	maxRetentionYears = 100
)

// Validate checks that the object lock configuration is well formed and that
// every feature it asks for is implemented.
func (c ObjectLockConfiguration) Validate() error {
	if c.ObjectLockEnabled != ObjectLockEnabled {
		return fmt.Errorf("object lock enabled must be %q: %w", ObjectLockEnabled, s3errs.ErrMalformedXML)
	}
	if c.Rule == nil {
		return nil
	}
	if c.Rule.DefaultRetention == nil {
		return fmt.Errorf("rule requires a default retention: %w", s3errs.ErrMalformedXML)
	}
	return c.Rule.DefaultRetention.validate()
}

func (d DefaultRetention) validate() error {
	if d.Mode != ObjectLockModeGovernance && d.Mode != ObjectLockModeCompliance {
		return fmt.Errorf("unknown retention mode %q: %w", d.Mode, s3errs.ErrMalformedXML)
	}

	if d.DefaultEventHold != nil {
		return fmt.Errorf("default event hold: %w", s3errs.ErrNotImplemented)
	}

	switch {
	case d.Days != nil && d.Years != nil:
		return fmt.Errorf("days and years are mutually exclusive: %w", s3errs.ErrMalformedXML)
	case d.Days == nil && d.Years == nil:
		return fmt.Errorf("a default retention requires days or years: %w", s3errs.ErrMalformedXML)
	case d.Days != nil && *d.Days <= 0:
		return fmt.Errorf("days must be positive: %w", s3errs.ErrInvalidRetentionPeriod)
	case d.Years != nil && *d.Years <= 0:
		return fmt.Errorf("years must be positive: %w", s3errs.ErrInvalidRetentionPeriod)
	case d.Days != nil && *d.Days > maxRetentionDays:
		return fmt.Errorf("days must not exceed %d: %w", maxRetentionDays, s3errs.ErrInvalidRetentionPeriod)
	case d.Years != nil && *d.Years > maxRetentionYears:
		return fmt.Errorf("years must not exceed %d: %w", maxRetentionYears, s3errs.ErrInvalidRetentionPeriod)
	}
	return nil
}

// routeBucketObjectLock dispatches the ?object-lock bucket subresource.
func (s *s3) routeBucketObjectLock(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodPut:
		return s.putBucketObjectLock(w, r, validatedKey, bucket)
	case http.MethodGet:
		return s.getBucketObjectLock(w, r, validatedKey, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putBucketObjectLock handles PUT Bucket object-lock requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
func (s *s3) putBucketObjectLock(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("putting bucket object lock configuration", zap.String("bucket", bucket))

	var config ObjectLockConfiguration
	if err := decodeXMLBody(r.Body, &config); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}

	return s.backend.PutBucketObjectLockConfiguration(r.Context(), accessKeyID, bucket, config)
}

// getBucketObjectLock handles GET Bucket object-lock requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html
func (s *s3) getBucketObjectLock(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("getting bucket object lock configuration", zap.String("bucket", bucket))

	config, err := s.backend.GetBucketObjectLockConfiguration(r.Context(), accessKeyID, bucket)
	if err != nil {
		return err
	}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	return writeXMLResponse(w, http.StatusOK, config)
}
