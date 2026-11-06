package s3

import (
	"encoding/hex"
	"encoding/xml"
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

// EffectiveSizeRange returns the object size bounds the rule applies to, drawn
// from either the filter or its And operator. Both bounds are exclusive, and a
// nil bound is unbounded.
func (r LifecycleRule) EffectiveSizeRange() (greaterThan, lessThan *int64) {
	if r.Filter == nil {
		return nil, nil
	} else if r.Filter.And != nil {
		return r.Filter.And.ObjectSizeGreaterThan, r.Filter.And.ObjectSizeLessThan
	}
	return r.Filter.ObjectSizeGreaterThan, r.Filter.ObjectSizeLessThan
}

// EffectiveTags returns every tag the rule's filter requires.
func (r LifecycleRule) EffectiveTags() TagSet {
	return r.Filter.tags()
}

// matchesSize reports whether an object of the given size falls within the
// rule's exclusive size bounds. A negative size is unknown and matches only a
// rule carrying no size bound.
func (r LifecycleRule) matchesSize(size int64) bool {
	greaterThan, lessThan := r.EffectiveSizeRange()
	if greaterThan == nil && lessThan == nil {
		return true
	} else if size < 0 {
		return false
	}
	return (greaterThan == nil || size > *greaterThan) && (lessThan == nil || size < *lessThan)
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
// object with the given key, size, last-modified time and tags, or "" if no
// enabled expiration rule applies. When several rules match, the soonest
// expiration wins.
func (c LifecycleConfiguration) ExpirationHeader(objectKey string, size int64, lastModified time.Time, tags TagSet) string {
	var (
		soonest time.Time
		ruleID  string
		found   bool
	)
	for _, rule := range c.Rules {
		if !rule.Enabled() || !strings.HasPrefix(objectKey, rule.EffectivePrefix()) || !rule.matchesSize(size) || !rule.EffectiveTags().Matches(tags) {
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
		if err := rule.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks the rule against the features this server supports.
func (r LifecycleRule) Validate() error {
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
	if a := r.AbortIncompleteMultipartUpload; a != nil {
		if a.DaysAfterInitiation <= 0 {
			return fmt.Errorf("DaysAfterInitiation must be a positive integer: %w", s3errs.ErrMalformedXML)
		} else if len(r.EffectiveTags()) > 0 {
			return fmt.Errorf("AbortIncompleteMultipartUpload cannot use tag filters: %w", s3errs.ErrInvalidRequest)
		} else if r.Filter.hasSizeBounds() {
			return fmt.Errorf("AbortIncompleteMultipartUpload cannot be specified with object size filters: %w", s3errs.ErrInvalidRequest)
		}
	}
	return nil
}

// tags returns every tag the filter requires, from its Tag or its And
// operator.
func (f *LifecycleFilter) tags() TagSet {
	if f == nil {
		return nil
	} else if f.Tag != nil {
		return TagSet{*f.Tag}
	} else if f.And != nil {
		return f.And.Tags
	}
	return nil
}

// hasSizeBounds reports whether the filter carries an object size bound.
func (f *LifecycleFilter) hasSizeBounds() bool {
	if f == nil {
		return false
	} else if f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
		return true
	}
	return f.And != nil && (f.And.ObjectSizeGreaterThan != nil || f.And.ObjectSizeLessThan != nil)
}

// UnmarshalXML rejects sibling Tag elements, which decoding directly into Tag
// would silently collapse into the last one.
func (f *LifecycleFilter) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type filter LifecycleFilter
	var decoded struct {
		filter
		Tags TagSet `xml:"Tag"`
	}
	if err := d.DecodeElement(&decoded, &start); err != nil {
		return err
	} else if len(decoded.Tags) > 1 {
		return fmt.Errorf("multiple lifecycle tags must be wrapped in And: %w", s3errs.ErrMalformedXML)
	}
	*f = LifecycleFilter(decoded.filter)
	if len(decoded.Tags) == 1 {
		f.Tag = &decoded.Tags[0]
	}
	return nil
}

// validate checks lifecycle predicates, including unique tag keys.
func (f *LifecycleFilter) validate() error {
	if f == nil {
		return nil
	}
	if err := f.tags().validate(objectTagLimits); err != nil {
		return err
	}

	if f.Prefix != nil && f.And != nil {
		return fmt.Errorf("lifecycle filter may not specify both Prefix and And: %w", s3errs.ErrMalformedXML)
	}

	predicates := 0
	if f.Tag != nil {
		predicates++
	}
	if f.Prefix != nil && *f.Prefix != "" {
		predicates++
	}
	if f.ObjectSizeGreaterThan != nil {
		predicates++
	}
	if f.ObjectSizeLessThan != nil {
		predicates++
	}
	if f.And != nil {
		predicates++
		if f.And.Prefix == nil && f.And.ObjectSizeGreaterThan == nil && f.And.ObjectSizeLessThan == nil && len(f.And.Tags) == 0 {
			return fmt.Errorf("lifecycle And filter must contain a supported predicate: %w", s3errs.ErrMalformedXML)
		}
		if err := validateSizeRange(f.And.ObjectSizeGreaterThan, f.And.ObjectSizeLessThan); err != nil {
			return err
		}
	}
	if predicates > 1 {
		return fmt.Errorf("lifecycle filter may specify only one predicate: %w", s3errs.ErrMalformedXML)
	}
	return validateSizeRange(f.ObjectSizeGreaterThan, f.ObjectSizeLessThan)
}

// validateSizeRange checks a pair of object size bounds, which exclude their
// own values.
func validateSizeRange(greaterThan, lessThan *int64) error {
	if greaterThan != nil && *greaterThan < 0 {
		return fmt.Errorf("ObjectSizeGreaterThan must not be negative: %w", s3errs.ErrInvalidArgument)
	} else if lessThan != nil && *lessThan < 0 {
		return fmt.Errorf("ObjectSizeLessThan must not be negative: %w", s3errs.ErrInvalidArgument)
	} else if greaterThan != nil && lessThan != nil && *greaterThan >= *lessThan {
		return fmt.Errorf("ObjectSizeGreaterThan must be less than ObjectSizeLessThan: %w", s3errs.ErrInvalidArgument)
	}
	return nil
}

// routeBucketLifecycle routes the ?lifecycle bucket subresource.
func (s *s3) routeBucketLifecycle(r *http.Request) (operation, error) {
	switch r.Method {
	case http.MethodPut:
		return operation{action: actionPutLifecycleConfiguration, serve: s.putBucketLifecycle}, nil
	case http.MethodGet:
		return operation{action: actionGetLifecycleConfiguration, serve: s.getBucketLifecycle}, nil
	case http.MethodDelete:
		// S3 has no separate action for deleting the configuration
		return operation{action: actionPutLifecycleConfiguration, serve: s.deleteBucketLifecycle}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}

// putBucketLifecycle handles PUT Bucket lifecycle requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (s *s3) putBucketLifecycle(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("putting bucket lifecycle configuration", zap.String("bucket", access.bucket))

	var config LifecycleConfiguration
	if err := decodeXMLBody(r.Body, &config); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	config.assignRuleIDs()

	return s.backend.PutBucketLifecycleConfiguration(r.Context(), access.bucketID, config)
}

// getBucketLifecycle handles GET Bucket lifecycle requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (s *s3) getBucketLifecycle(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("getting bucket lifecycle configuration", zap.String("bucket", access.bucket))

	config, err := s.backend.GetBucketLifecycleConfiguration(r.Context(), access.bucketID)
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
func (s *s3) deleteBucketLifecycle(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("deleting bucket lifecycle configuration", zap.String("bucket", access.bucket))

	if err := s.backend.DeleteBucketLifecycleConfiguration(r.Context(), access.bucketID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
