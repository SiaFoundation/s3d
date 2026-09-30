package s3

import (
	"fmt"
	"net/http"
	"strings"
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

// HeaderBypassGovernanceRetention lets a caller weaken or clear a GOVERNANCE
// retention. It has no effect on COMPLIANCE.
const HeaderBypassGovernanceRetention = "X-Amz-Bypass-Governance-Retention"

// bypassGovernanceRetention reports whether the request asked to bypass a
// GOVERNANCE retention.
func bypassGovernanceRetention(h http.Header) bool {
	return strings.EqualFold(h.Get(HeaderBypassGovernanceRetention), "true")
}

// Validate checks a retention document from a PutObjectRetention body. A
// document with neither a mode nor a date clears the retention.
func (r ObjectRetention) Validate() error {
	if r.EventHold != nil || r.EventHoldDuration != nil {
		return fmt.Errorf("event hold: %w", s3errs.ErrNotImplemented)
	} else if r.Mode == "" && r.RetainUntilDate == "" {
		return nil
	} else if (r.Mode == "") != (r.RetainUntilDate == "") {
		return fmt.Errorf("a retention needs both a mode and a retain until date: %w", s3errs.ErrInvalidRequest)
	} else if r.Mode != ObjectLockModeGovernance && r.Mode != ObjectLockModeCompliance {
		return fmt.Errorf("unknown retention mode %q: %w", r.Mode, s3errs.ErrMalformedXML)
	}
	return nil
}

// state converts a validated retention document into the state to store. The
// zero value clears the retention.
func (r ObjectRetention) state() (ObjectLockState, error) {
	if r.Mode == "" {
		return ObjectLockState{}, nil
	}
	t, err := parseRetainUntilDate(r.RetainUntilDate)
	if err != nil {
		return ObjectLockState{}, err
	}
	return ObjectLockState{Mode: r.Mode, RetainUntil: t}, nil
}

// Validate checks a legal hold document from a PutObjectLegalHold body.
func (l ObjectLegalHold) Validate() error {
	if l.Status != LegalHoldOn && l.Status != LegalHoldOff {
		return fmt.Errorf("unknown legal hold status %q: %w", l.Status, s3errs.ErrMalformedXML)
	}
	return nil
}

func (s *s3) routeObjectRetention(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string, version VersionRequest) error {
	if object == "" {
		return s3errs.ErrMethodNotAllowed
	}
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodPut:
		return s.putObjectRetention(w, r, validatedKey, bucket, object, version)
	case http.MethodGet:
		return s.getObjectRetention(w, r, validatedKey, bucket, object, version)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putObjectRetention handles PUT Object retention requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
func (s *s3) putObjectRetention(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object string, version VersionRequest) error {
	s.logger.Debug("putting object retention", zap.String("bucket", bucket), zap.String("object", object))

	var doc ObjectRetention
	if err := decodeXMLBody(r.Body, &doc); err != nil {
		return err
	}
	if err := doc.Validate(); err != nil {
		return err
	}
	state, err := doc.state()
	if err != nil {
		return err
	}

	return s.backend.PutObjectRetention(r.Context(), accessKeyID, bucket, object, version, state, bypassGovernanceRetention(r.Header))
}

// getObjectRetention handles GET Object retention requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
func (s *s3) getObjectRetention(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object string, version VersionRequest) error {
	s.logger.Debug("getting object retention", zap.String("bucket", bucket), zap.String("object", object))

	state, err := s.backend.GetObjectRetention(r.Context(), accessKeyID, bucket, object, version)
	if err != nil {
		return err
	}
	return writeXMLResponse(w, http.StatusOK, ObjectRetention{
		Xmlns:           "http://s3.amazonaws.com/doc/2006-03-01/",
		Mode:            state.Mode,
		RetainUntilDate: state.RetainUntil.UTC().Format(retainUntilLayout),
	})
}

func (s *s3) routeObjectLegalHold(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string, version VersionRequest) error {
	if object == "" {
		return s3errs.ErrMethodNotAllowed
	}
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodPut:
		return s.putObjectLegalHold(w, r, validatedKey, bucket, object, version)
	case http.MethodGet:
		return s.getObjectLegalHold(w, r, validatedKey, bucket, object, version)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putObjectLegalHold handles PUT Object legal-hold requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
func (s *s3) putObjectLegalHold(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object string, version VersionRequest) error {
	s.logger.Debug("putting object legal hold", zap.String("bucket", bucket), zap.String("object", object))

	var doc ObjectLegalHold
	if err := decodeXMLBody(r.Body, &doc); err != nil {
		return err
	}
	if err := doc.Validate(); err != nil {
		return err
	}

	return s.backend.PutObjectLegalHold(r.Context(), accessKeyID, bucket, object, version, doc.Status)
}

// getObjectLegalHold handles GET Object legal-hold requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html
func (s *s3) getObjectLegalHold(w http.ResponseWriter, r *http.Request, accessKeyID, bucket, object string, version VersionRequest) error {
	s.logger.Debug("getting object legal hold", zap.String("bucket", bucket), zap.String("object", object))

	status, err := s.backend.GetObjectLegalHold(r.Context(), accessKeyID, bucket, object, version)
	if err != nil {
		return err
	}
	if status == "" {
		return s3errs.ErrNoSuchObjectLockConfiguration
	}
	return writeXMLResponse(w, http.StatusOK, ObjectLegalHold{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Status: status,
	})
}
