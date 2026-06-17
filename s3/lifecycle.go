package s3

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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
// versioning). dayDuration is the wall-clock duration treated as a single
// "day" when evaluating a Days window.
func (e *LifecycleExpiration) ExpiryCutoff(now time.Time, dayDuration time.Duration) (cutoff time.Time, ok bool) {
	if e == nil {
		return time.Time{}, false
	} else if e.Days > 0 {
		return daysCutoff(now, e.Days, dayDuration), true
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
// aborted. dayDuration is the wall-clock duration treated as a single "day".
func (a *AbortIncompleteMultipartUpload) AbortCutoff(now time.Time, dayDuration time.Duration) time.Time {
	return daysCutoff(now, a.DaysAfterInitiation, dayDuration)
}

// daysCutoff returns the cutoff for an action scheduled days after an event.
// With the standard 24h day it follows S3's rounding, anchoring to the start of
// the current UTC day so the full window is honored. A shortened dayDuration
// (used by tests) drops the calendar anchoring for a flat now-minus-window.
func daysCutoff(now time.Time, days int, dayDuration time.Duration) time.Time {
	window := time.Duration(days) * dayDuration
	if dayDuration == 24*time.Hour {
		return startOfDayUTC(now).Add(-window)
	}
	return now.Add(-window)
}

// startOfDayUTC returns midnight UTC of the day containing t.
func startOfDayUTC(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
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

// expiryDate returns the date an object last modified at lastModified expires
// under this rule, adding Days and rounding up to the next midnight UTC per S3.
// ok is false when the rule yields no fixed date. Unlike ExpiryCutoff it always
// uses real calendar days, since it powers the advisory x-amz-expiration header.
func (e *LifecycleExpiration) expiryDate(lastModified time.Time) (time.Time, bool) {
	if e == nil {
		return time.Time{}, false
	} else if e.Days > 0 {
		return ceilToMidnightUTC(lastModified.Add(time.Duration(e.Days) * 24 * time.Hour)), true
	} else if e.Date != "" {
		date, err := parseLifecycleDate(e.Date)
		if err != nil {
			return time.Time{}, false
		}
		return date, true
	}
	return time.Time{}, false
}

// ceilToMidnightUTC rounds t up to the next midnight UTC, leaving it unchanged
// if it already falls on a midnight boundary.
func ceilToMidnightUTC(t time.Time) time.Time {
	midnight := startOfDayUTC(t)
	if t.UTC().Equal(midnight) {
		return midnight
	}
	return midnight.Add(24 * time.Hour)
}

// ExpirationHeader returns the x-amz-expiration response header value for an
// object with the given key and last-modified time, or "" if no enabled
// expiration rule applies. When several rules match, the soonest expiration
// wins.
func (c LifecycleConfiguration) ExpirationHeader(objectKey string, lastModified time.Time) string {
	var (
		soonest time.Time
		ruleID  string
		found   bool
	)
	for _, rule := range c.Rules {
		if !rule.Enabled() || !strings.HasPrefix(objectKey, rule.EffectivePrefix()) {
			continue
		}
		expiry, ok := rule.Expiration.expiryDate(lastModified)
		if !ok {
			continue
		}
		if !found || expiry.Before(soonest) {
			soonest, ruleID, found = expiry, rule.ID, true
		}
	}
	if !found {
		return ""
	}
	return fmt.Sprintf("expiry-date=%q, rule-id=%q", soonest.Format(http.TimeFormat), ruleID)
}

// assignRuleIDs fills in a unique generated ID for every rule that was
// submitted without one, mirroring S3's behavior of always returning a rule ID.
func (c *LifecycleConfiguration) assignRuleIDs() {
	used := make(map[string]bool)
	for _, rule := range c.Rules {
		if rule.ID != "" {
			used[rule.ID] = true
		}
	}
	for i := range c.Rules {
		if c.Rules[i].ID != "" {
			continue
		}
		id := newLifecycleRuleID()
		for used[id] {
			id = newLifecycleRuleID()
		}
		used[id] = true
		c.Rules[i].ID = id
	}
}

// newLifecycleRuleID returns a random, unique lifecycle rule ID.
func newLifecycleRuleID() string {
	return hex.EncodeToString(frand.Bytes(16))
}

// Validate checks that the lifecycle configuration is well-formed and only uses
// features supported by this server.
func (c LifecycleConfiguration) Validate() error {
	if len(c.Rules) == 0 {
		return fmt.Errorf("lifecycle configuration must contain at least one rule: %w", s3errs.ErrMalformedXML)
	}
	seen := make(map[string]bool)
	for _, rule := range c.Rules {
		if rule.ID != "" {
			if seen[rule.ID] {
				return fmt.Errorf("duplicate lifecycle rule ID %q: %w", rule.ID, s3errs.ErrInvalidArgument)
			}
			seen[rule.ID] = true
		}
		if err := rule.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r LifecycleRule) validate() error {
	if len(r.ID) > LifecycleRuleIDSizeLimit {
		return fmt.Errorf("lifecycle rule ID exceeds %d characters: %w", LifecycleRuleIDSizeLimit, s3errs.ErrInvalidArgument)
	} else if r.Status != LifecycleStatusEnabled && r.Status != LifecycleStatusDisabled {
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
			return fmt.Errorf("expiration Days must be a positive integer: %w", s3errs.ErrInvalidArgument)
		} else if e.Date != "" {
			date, err := parseLifecycleDate(e.Date)
			if err != nil {
				return fmt.Errorf("%w: %w", err, s3errs.ErrMalformedXML)
			} else if !date.Equal(date.Truncate(24 * time.Hour)) {
				// S3 requires expiration dates to be at midnight UTC.
				return fmt.Errorf("expiration Date must be at midnight UTC: %w", s3errs.ErrInvalidArgument)
			}
		} else if e.ExpiredObjectDeleteMarker != nil {
			return fmt.Errorf("ExpiredObjectDeleteMarker lifecycle expiration is not supported: %w", s3errs.ErrNotImplemented)
		} else if e.Days == 0 && e.ExpiredObjectDeleteMarker == nil {
			return fmt.Errorf("expiration Days must be a positive integer: %w", s3errs.ErrInvalidArgument)
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
	config.assignRuleIDs()

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
