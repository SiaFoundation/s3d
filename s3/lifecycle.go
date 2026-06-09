package s3

import (
	"fmt"
	"net/http"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Lifecycle rule statuses.
const (
	// LifecycleStatusEnabled marks a lifecycle rule as active.
	LifecycleStatusEnabled = "Enabled"
	// LifecycleStatusDisabled marks a lifecycle rule as inactive.
	LifecycleStatusDisabled = "Disabled"
)

// Enabled reports whether the rule is enabled.
func (r LifecycleRule) Enabled() bool {
	return r.Status == LifecycleStatusEnabled
}

// EffectivePrefix returns the object key prefix the rule applies to, drawn from
// either the deprecated rule-level prefix or the filter.
func (r LifecycleRule) EffectivePrefix() string {
	if r.Prefix != nil {
		return *r.Prefix
	} else if r.Filter != nil {
		if r.Filter.Prefix != nil {
			return *r.Filter.Prefix
		} else if r.Filter.And != nil && r.Filter.And.Prefix != nil {
			return *r.Filter.And.Prefix
		}
	}
	return ""
}

// ExpiryCutoff returns the cutoff time for current-version expiration relative
// to now. Objects last modified at or before the cutoff are expired. ok is false when
// the expiration is not currently active (e.g. a future Date, or a rule that
// only references ExpiredObjectDeleteMarker, which is unsupported without
// versioning).
func (e *LifecycleExpiration) ExpiryCutoff(now time.Time) (cutoff time.Time, ok bool) {
	if e == nil {
		return time.Time{}, false
	} else if e.Days > 0 {
		return daysCutoff(now, e.Days), true
	} else if e.Date != "" {
		date, err := parseLifecycleDate(e.Date)
		if err != nil || now.Before(date) {
			return time.Time{}, false
		}
		// the date has passed; every existing object matching the rule expires
		return now, true
	}
	return time.Time{}, false
}

// AbortCutoff returns the cutoff time for aborting incomplete multipart
// uploads relative to now. Uploads initiated at or before the cutoff are
// aborted.
func (a *AbortIncompleteMultipartUpload) AbortCutoff(now time.Time) time.Time {
	return daysCutoff(now, a.DaysAfterInitiation)
}

// daysCutoff returns the cutoff matching S3's day rounding: an action scheduled
// days after an event is due only once midnight UTC of (event time + days) has
// passed. That is equivalent to a cutoff of (start of the current UTC day)
// minus days, which honors the full days window rather than acting up to a day
// early.
func daysCutoff(now time.Time, days int) time.Time {
	y, m, d := now.UTC().Date()
	startOfDay := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	return startOfDay.Add(-time.Duration(days) * 24 * time.Hour)
}

// parseLifecycleDate parses an ISO8601 lifecycle Date value.
func parseLifecycleDate(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z07:00", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid lifecycle date %q", s)
}

// Validate checks that the lifecycle configuration is well-formed and only uses
// features supported by this server.
func (c LifecycleConfiguration) Validate() error {
	if len(c.Rules) == 0 {
		return fmt.Errorf("lifecycle configuration must contain at least one rule: %w", s3errs.ErrMalformedXML)
	}
	for _, rule := range c.Rules {
		if err := rule.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r LifecycleRule) validate() error {
	if r.Status != LifecycleStatusEnabled && r.Status != LifecycleStatusDisabled {
		return fmt.Errorf("invalid rule status %q: %w", r.Status, s3errs.ErrMalformedXML)
	} else if r.Prefix != nil && r.Filter != nil {
		return fmt.Errorf("rule may not specify both Prefix and Filter: %w", s3errs.ErrMalformedXML)
	} else if r.Prefix == nil && r.Filter == nil {
		// AWS rejects rules without a Prefix or Filter; accepting one would
		// silently apply the rule to the entire bucket. An explicitly empty
		// Filter is the supported way to match every object.
		return fmt.Errorf("rule must specify a Prefix or Filter: %w", s3errs.ErrMalformedXML)
	} else if err := r.Filter.validate(); err != nil {
		return err
	} else if len(r.Transitions) > 0 || len(r.NoncurrentVersionTransitions) > 0 || r.NoncurrentVersionExpiration != nil {
		return fmt.Errorf("transition and noncurrent-version lifecycle actions are not supported: %w", s3errs.ErrNotImplemented)
	} else if r.Expiration == nil && r.AbortIncompleteMultipartUpload == nil {
		return fmt.Errorf("rule must specify at least one action: %w", s3errs.ErrMalformedXML)
	}

	if e := r.Expiration; e != nil {
		if e.Days > 0 && e.Date != "" {
			return fmt.Errorf("expiration may not specify both Days and Date: %w", s3errs.ErrMalformedXML)
		} else if e.ExpiredObjectDeleteMarker != nil && (e.Days > 0 || e.Date != "") {
			return fmt.Errorf("expiration may not specify ExpiredObjectDeleteMarker with Days or Date: %w", s3errs.ErrMalformedXML)
		} else if e.Days < 0 {
			return fmt.Errorf("expiration Days must be a positive integer: %w", s3errs.ErrMalformedXML)
		} else if e.Date != "" {
			if _, err := parseLifecycleDate(e.Date); err != nil {
				return fmt.Errorf("%w: %w", err, s3errs.ErrMalformedXML)
			}
		} else if e.ExpiredObjectDeleteMarker != nil {
			return fmt.Errorf("ExpiredObjectDeleteMarker lifecycle expiration is not supported: %w", s3errs.ErrNotImplemented)
		} else if e.Days == 0 && e.ExpiredObjectDeleteMarker == nil {
			return fmt.Errorf("expiration must specify Days or Date: %w", s3errs.ErrMalformedXML)
		}
	}
	if a := r.AbortIncompleteMultipartUpload; a != nil && a.DaysAfterInitiation <= 0 {
		return fmt.Errorf("DaysAfterInitiation must be a positive integer: %w", s3errs.ErrMalformedXML)
	}
	return nil
}

// validate rejects filter predicates that this server cannot honor. Filtering
// by tag or object size is unsupported; silently ignoring such predicates would
// cause a rule to match more objects than intended, so we reject them outright.
func (f *LifecycleFilter) validate() error {
	if f == nil {
		return nil
	} else if f.Tag != nil || f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
		return fmt.Errorf("tag and object-size lifecycle filters are not supported: %w", s3errs.ErrNotImplemented)
	} else if f.And != nil && (len(f.And.Tags) > 0 || f.And.ObjectSizeGreaterThan != nil || f.And.ObjectSizeLessThan != nil) {
		return fmt.Errorf("tag and object-size lifecycle filters are not supported: %w", s3errs.ErrNotImplemented)
	}

	predicates := 0
	if f.Prefix != nil {
		predicates++
	}
	if f.And != nil {
		predicates++
		if f.And.Prefix == nil {
			return fmt.Errorf("lifecycle And filter must contain a supported predicate: %w", s3errs.ErrMalformedXML)
		}
	}
	if predicates > 1 {
		return fmt.Errorf("lifecycle filter may specify only one predicate: %w", s3errs.ErrMalformedXML)
	}
	return nil
}

// routeBucketLifecycle dispatches the ?lifecycle bucket subresource.
func (s *s3) routeBucketLifecycle(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	switch r.Method {
	case http.MethodPut:
		return s.putBucketLifecycle(w, r, accessKeyID, bucket)
	case http.MethodGet:
		return s.getBucketLifecycle(w, r, accessKeyID, bucket)
	case http.MethodDelete:
		return s.deleteBucketLifecycle(w, r, accessKeyID, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putBucketLifecycle handles PUT Bucket lifecycle requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (s *s3) putBucketLifecycle(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("putting bucket lifecycle configuration", zap.String("bucket", bucket))

	var config LifecycleConfiguration
	if err := decodeXMLBody(r.Body, &config); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}

	return s.backend.PutBucketLifecycleConfiguration(r.Context(), accessKeyID, bucket, config)
}

// getBucketLifecycle handles GET Bucket lifecycle requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (s *s3) getBucketLifecycle(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("getting bucket lifecycle configuration", zap.String("bucket", bucket))

	config, err := s.backend.GetBucketLifecycleConfiguration(r.Context(), accessKeyID, bucket)
	if err != nil {
		return err
	}
	return writeXMLResponse(w, http.StatusOK, LifecycleConfiguration{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Rules: config.Rules,
	})
}

// deleteBucketLifecycle handles DELETE Bucket lifecycle requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (s *s3) deleteBucketLifecycle(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("deleting bucket lifecycle configuration", zap.String("bucket", bucket))

	if err := s.backend.DeleteBucketLifecycleConfiguration(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
