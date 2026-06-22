package s3

import (
	"time"
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

// FormatVersion renders an internal version ID for an S3 response. The null
// version (the empty string) is rendered as the literal "null".
func FormatVersion(versionID string) string {
	if versionID == "" {
		return Null
	}
	return versionID
}

// VersionRequest identifies which version of an object an operation addresses.
// It distinguishes "no version was specified" (target the current version) from
// "the null version was specified", a distinction a raw version string cannot
// make.
type VersionRequest struct {
	// Specified reports whether the request addressed a particular version. When
	// false the operation targets the current version.
	Specified bool
	// ID is the internal version ID when Specified is true; "" is the null
	// version.
	ID string
}

// NoVersion returns a VersionRequest addressing the current version.
func NoVersion() VersionRequest { return VersionRequest{} }

// SpecificVersion returns a VersionRequest addressing the given internal version
// ID ("" is the null version).
func SpecificVersion(id string) VersionRequest {
	return VersionRequest{Specified: true, ID: id}
}

// LogValue renders the requested version for logging: empty when no version was
// specified, otherwise the wire encoding.
func (v VersionRequest) LogValue() string {
	if !v.Specified {
		return ""
	}
	return FormatVersion(v.ID)
}

// prefixSet records which common prefixes have already been seen.
type prefixSet map[string]bool

// Add records prefix as seen, returning false if it had already been added.
func (s prefixSet) Add(prefix string) bool {
	if s[prefix] {
		return false
	}
	s[prefix] = true
	return true
}

// ListObjectVersionsPage specifies pagination options for listing object
// versions in a bucket.
type ListObjectVersionsPage struct {
	// FetchOwner specifies whether owner information should be included.
	FetchOwner *bool

	// KeyMarker is the key to resume listing after, or nil to start from the
	// beginning.
	KeyMarker *string

	// VersionIDMarker is the version to resume within KeyMarker, or nil for all
	// of KeyMarker's versions. The wire value "null" is mapped to "".
	VersionIDMarker *string

	// MaxKeys sets the maximum number of versions returned in the response.
	MaxKeys int64
}

// ObjectVersion is a single version (or delete marker) of an object, as
// returned by ListObjectVersions.
type ObjectVersion struct {
	Key            string
	VersionID      string // "" represents the null version
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   time.Time
	ETag           string // empty for delete markers
	Size           int64
	Owner          *UserInfo
}

// ObjectVersionsListResult contains the result of a ListObjectVersions
// operation. Versions are ordered by key ascending, then by version creation
// order descending (newest first), with delete markers interleaved.
type ObjectVersionsListResult struct {
	CommonPrefixes []CommonPrefix
	Versions       []ObjectVersion
	IsTruncated    bool
	NextKeyMarker  string
	// NextVersionIDMarker is wire-encoded: "null" for the null version, "" when
	// the truncation boundary is a common prefix (no version applies).
	NextVersionIDMarker string

	// prefixes maintains an index of common prefixes that have already been
	// rolled up, so repeated keys under the same prefix are deduped.
	prefixes prefixSet
	maxKeys  int64
}

// NewObjectVersionsListResult creates a new, empty ObjectVersionsListResult. Use
// AddVersion and AddPrefix to populate it.
func NewObjectVersionsListResult(maxKeys int64) *ObjectVersionsListResult {
	return &ObjectVersionsListResult{maxKeys: maxKeys}
}

// Count returns the number of versions and common prefixes added so far.
func (r *ObjectVersionsListResult) Count() int64 {
	return int64(len(r.Versions) + len(r.CommonPrefixes))
}

// AddVersion appends a version (or delete marker), or marks the result truncated
// if the page is already full.
func (r *ObjectVersionsListResult) AddVersion(v ObjectVersion) {
	if r.Count() >= r.maxKeys {
		r.IsTruncated = true
		return
	}
	r.Versions = append(r.Versions, v)
	// wire-encode the marker; an empty null-version value would be dropped by
	// the encoder, breaking mid-key resumption.
	r.NextKeyMarker, r.NextVersionIDMarker = v.Key, FormatVersion(v.VersionID)
}

// AddPrefix rolls a key up under a common prefix (deduping repeats), or marks
// the result truncated if the page is already full.
func (r *ObjectVersionsListResult) AddPrefix(prefix string) {
	if r.prefixes == nil {
		r.prefixes = prefixSet{}
	}
	if !r.prefixes.Add(prefix) {
		return
	}
	if r.Count() >= r.maxKeys {
		r.IsTruncated = true
		return
	}
	r.CommonPrefixes = append(r.CommonPrefixes, CommonPrefix{Prefix: prefix})
	r.NextKeyMarker, r.NextVersionIDMarker = prefix, ""
}
