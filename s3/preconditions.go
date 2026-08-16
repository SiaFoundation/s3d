package s3

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// wildcardETag is the ETag condition that matches any existing object.
const wildcardETag = "*"

// etagMatches reports whether an ETag condition matches etag. The condition may
// be the wildcard or a comma-separated list of ETags.
func etagMatches(condition, etag string) bool {
	if condition == wildcardETag {
		return true
	}
	for _, v := range strings.Split(condition, ",") {
		if strings.TrimSpace(v) == etag {
			return true
		}
	}
	return false
}

// ObjectAttrs are the attributes of a stored version that preconditions are
// matched against.
type ObjectAttrs struct {
	ETag           string
	Size           int64
	LastModified   time.Time
	IsDeleteMarker bool
}

// ObjectPreconditions are the conditional headers evaluated against an object.
// Reads and copy sources use all four fields; writes only support the ETag
// fields. A nil field is an absent header.
type ObjectPreconditions struct {
	IfMatch           *string
	IfNoneMatch       *string
	IfModifiedSince   *time.Time
	IfUnmodifiedSince *time.Time
}

// check evaluates the preconditions against an existing object, applying the
// RFC 7232 Section 6 precedence: If-Match > If-Unmodified-Since > If-None-Match
// > If-Modified-Since.
func (p ObjectPreconditions) check(etag string, lastModified time.Time) error {
	if p.IfMatch != nil {
		if !etagMatches(*p.IfMatch, etag) {
			return s3errs.ErrPreconditionFailed
		}
	} else if p.IfUnmodifiedSince != nil && lastModified.After(*p.IfUnmodifiedSince) {
		return s3errs.ErrPreconditionFailed
	}

	if p.IfNoneMatch != nil {
		if etagMatches(*p.IfNoneMatch, etag) {
			return s3errs.ErrNotModified
		}
	} else if p.IfModifiedSince != nil && !lastModified.After(*p.IfModifiedSince) {
		return s3errs.ErrNotModified
	}
	return nil
}

// CheckCopySource evaluates the preconditions against the source object of a
// copy. A delete marker has nothing to copy and is resolved first, since its
// zeroed ETag would otherwise satisfy an ETag precondition; srcVersion decides
// how it is reported. A copy has no cached representation to revalidate, so a
// precondition that would report a 304 on a read reports ErrPreconditionFailed.
func (p ObjectPreconditions) CheckCopySource(src ObjectAttrs, srcVersion VersionRequest) error {
	if src.IsDeleteMarker {
		if srcVersion.Specified {
			return s3errs.ErrInvalidRequest
		}
		return s3errs.ErrNoSuchKey
	}
	err := p.check(src.ETag, src.LastModified)
	if errors.Is(err, s3errs.ErrNotModified) {
		return s3errs.ErrPreconditionFailed
	}
	return err
}

// CheckWrite evaluates the preconditions against the object a write would
// replace; current is nil when there is none, as is a delete marker. A backend
// must call this in the same atomic operation as the write itself, so that of
// two concurrent writers only one can satisfy the same condition.
//
// An If-Match with no object to match against returns ErrNoSuchKey, the wildcard
// included. Any other failure returns ErrPreconditionFailed.
func (p ObjectPreconditions) CheckWrite(current *ObjectAttrs) error {
	if current == nil || current.IsDeleteMarker {
		if p.IfMatch != nil {
			return s3errs.ErrNoSuchKey
		}
		return nil
	}
	if p.IfMatch != nil && !etagMatches(*p.IfMatch, current.ETag) {
		return s3errs.ErrPreconditionFailed
	} else if p.IfNoneMatch != nil && etagMatches(*p.IfNoneMatch, current.ETag) {
		return s3errs.ErrPreconditionFailed
	}
	return nil
}

// HasWritePreconditions reports whether any precondition a write honors is set,
// letting a backend skip looking up the object it would replace.
func (p ObjectPreconditions) HasWritePreconditions() bool {
	return p.IfMatch != nil || p.IfNoneMatch != nil
}

// HasPreconditions reports whether the delete carries any If-Match-style
// precondition, letting a backend skip looking up the version it would match
// against.
func (o ObjectID) HasPreconditions() bool {
	return o.ETag != nil || o.Size != nil || o.LastModifiedTime != nil
}

// HasSpecificPreconditions reports whether any precondition names a particular
// object rather than only asserting that one is there, which is all the ETag
// wildcard does.
func (o ObjectID) HasSpecificPreconditions() bool {
	return (o.ETag != nil && *o.ETag != wildcardETag) || o.Size != nil || o.LastModifiedTime != nil
}

// CheckDelete evaluates the delete's preconditions against the version being
// deleted, returning ErrPreconditionFailed if any of them is set and does not
// match. A delete marker fails every precondition, the ETag wildcard included.
func (o ObjectID) CheckDelete(a ObjectAttrs) error {
	if !o.HasPreconditions() {
		return nil
	} else if a.IsDeleteMarker {
		return s3errs.ErrPreconditionFailed
	}

	if o.ETag != nil && !etagMatches(*o.ETag, a.ETag) {
		return s3errs.ErrPreconditionFailed
	} else if o.Size != nil && *o.Size != a.Size {
		return s3errs.ErrPreconditionFailed
	} else if o.LastModifiedTime != nil && !a.LastModified.Truncate(time.Second).Equal(o.LastModifiedTime.StdTime()) {
		return s3errs.ErrPreconditionFailed
	}
	return nil
}

// requestPreconditions returns the preconditions carried by the request's own
// If-* headers, which address the object the request operates on.
func requestPreconditions(h http.Header) ObjectPreconditions {
	return preconditionsFromHeaders(h, "")
}

// copySourcePreconditions returns the preconditions carried by the
// x-amz-copy-source-if-* headers, which address the source of a copy.
func copySourcePreconditions(h http.Header) ObjectPreconditions {
	return preconditionsFromHeaders(h, "X-Amz-Copy-Source-")
}

// preconditionsFromHeaders reads a set of conditional headers, each named by
// prefix followed by its RFC 7232 header name. An unparsable date is treated as
// an absent one.
func preconditionsFromHeaders(h http.Header, prefix string) ObjectPreconditions {
	var p ObjectPreconditions
	if v := h.Get(prefix + "If-Match"); v != "" {
		p.IfMatch = &v
	}
	if v := h.Get(prefix + "If-None-Match"); v != "" {
		p.IfNoneMatch = &v
	}
	if t, err := http.ParseTime(h.Get(prefix + "If-Modified-Since")); err == nil {
		p.IfModifiedSince = &t
	}
	if t, err := http.ParseTime(h.Get(prefix + "If-Unmodified-Since")); err == nil {
		p.IfUnmodifiedSince = &t
	}
	return p
}

// deleteObjectID returns the object a delete addresses, along with the
// preconditions carried by its If-Match, x-amz-if-match-size and
// x-amz-if-match-last-modified-time headers. Unlike the read preconditions, a
// malformed value is rejected rather than ignored, since dropping it would turn
// a guarded delete into an unguarded one.
func deleteObjectID(h http.Header, object string, version VersionRequest) (ObjectID, error) {
	oid := ObjectID{Key: object}
	if version.Specified {
		oid.VersionID = &version.ID
	}

	// the wildcard is passed through for the backend to match against any
	// existing object
	if v := h.Get("If-Match"); v != "" {
		oid.ETag = &v
	}
	if v := h.Get("X-Amz-If-Match-Size"); v != "" {
		size, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ObjectID{}, s3errs.ErrInvalidArgument
		}
		oid.Size = &size
	}
	if v := h.Get("X-Amz-If-Match-Last-Modified-Time"); v != "" {
		t, err := http.ParseTime(v)
		if err != nil {
			return ObjectID{}, s3errs.ErrInvalidArgument
		}
		lastMod := NewHttpTime(t)
		oid.LastModifiedTime = &lastMod
	}
	return oid, nil
}
