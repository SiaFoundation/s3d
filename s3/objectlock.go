package s3

import (
	"fmt"
	"net/http"
	"time"

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

// the two legal hold states. A version that never had one carries neither.
const (
	LegalHoldOn  = "ON"
	LegalHoldOff = "OFF"
)

// the per version lock headers a write may carry. The event hold headers are
// parsed only so they can be refused.
const (
	HeaderObjectLockMode                   = "X-Amz-Object-Lock-Mode"
	HeaderObjectLockRetainUntilDate        = "X-Amz-Object-Lock-Retain-Until-Date"
	HeaderObjectLockLegalHold              = "X-Amz-Object-Lock-Legal-Hold"
	HeaderObjectLockEventHold              = "X-Amz-Object-Lock-Event-Hold"
	HeaderObjectLockEventHoldDurationDays  = "X-Amz-Object-Lock-Event-Hold-Duration-Days"
	HeaderObjectLockEventHoldDurationYears = "X-Amz-Object-Lock-Event-Hold-Duration-Years"
)

// retainUntilLayout is the ISO8601 form AWS reports a retain until date in.
// Its millisecond precision matches what the store keeps.
const retainUntilLayout = "2006-01-02T15:04:05.000Z07:00"

// ObjectLockState is the lock applied to a single object version. Mode and
// RetainUntil are set together or not at all. LegalHold is "", ON or OFF, where
// "" means no legal hold has ever been applied.
type ObjectLockState struct {
	Mode        string
	RetainUntil time.Time
	LegalHold   string
}

// Retained reports whether the state carries a retention period. A nil state
// carries none.
func (s *ObjectLockState) Retained() bool {
	return s != nil && s.Mode != ""
}

// requestObjectLock reads the per version lock headers a write carries,
// returning nil when it carries none.
func requestObjectLock(h http.Header) (*ObjectLockState, error) {
	for _, name := range []string{HeaderObjectLockEventHold, HeaderObjectLockEventHoldDurationDays, HeaderObjectLockEventHoldDurationYears} {
		if h.Get(name) != "" {
			return nil, fmt.Errorf("%s: %w", name, s3errs.ErrNotImplemented)
		}
	}

	mode := h.Get(HeaderObjectLockMode)
	date := h.Get(HeaderObjectLockRetainUntilDate)
	hold := h.Get(HeaderObjectLockLegalHold)
	if mode == "" && date == "" && hold == "" {
		return nil, nil
	}

	var state ObjectLockState
	if (mode == "") != (date == "") {
		return nil, fmt.Errorf("a retention needs both a mode and a retain until date: %w", s3errs.ErrInvalidRequest)
	} else if mode != "" {
		if mode != ObjectLockModeGovernance && mode != ObjectLockModeCompliance {
			return nil, fmt.Errorf("unknown retention mode %q: %w", mode, s3errs.ErrInvalidRequest)
		}
		retain, err := parseRetainUntilDate(date)
		if err != nil {
			return nil, err
		}
		state.Mode, state.RetainUntil = mode, retain
	}

	if hold != "" {
		if hold != LegalHoldOn && hold != LegalHoldOff {
			return nil, fmt.Errorf("unknown legal hold status %q: %w", hold, s3errs.ErrInvalidRequest)
		}
		state.LegalHold = hold
	}
	return &state, nil
}

// writeObjectLockHeaders reports a version's lock state on a GET or HEAD. A
// version that never had a legal hold reports no legal hold header at all,
// which AWS distinguishes from an explicit OFF.
func writeObjectLockHeaders(h http.Header, state *ObjectLockState) {
	if state == nil {
		return
	}
	if state.Retained() {
		h.Set(HeaderObjectLockMode, state.Mode)
		h.Set(HeaderObjectLockRetainUntilDate, state.RetainUntil.UTC().Format(retainUntilLayout))
	}
	if state.LegalHold != "" {
		h.Set(HeaderObjectLockLegalHold, state.LegalHold)
	}
}

// parseRetainUntilDate parses an ISO8601 retain until date, which must lie in
// the future. The result is rounded up to the millisecond the store keeps, so a
// deadline is never truncated into the past.
func parseRetainUntilDate(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid retain until date %q: %w", s, s3errs.ErrInvalidRequest)
	} else if !t.After(time.Now()) {
		return time.Time{}, fmt.Errorf("retain until date %q is not in the future: %w", s, s3errs.ErrInvalidRequest)
	}
	return t.UTC().Add(time.Millisecond - time.Nanosecond).Truncate(time.Millisecond), nil
}

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
