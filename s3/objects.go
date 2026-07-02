package s3

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

// Object represents an S3 object stored on the backend.
type Object struct {
	Body         io.ReadCloser
	ContentMD5   [16]byte
	LastModified time.Time
	Metadata     map[string]string
	Range        *ObjectRange
	Size         int64

	// VersionID is the version ID of the object, or "" for the null version.
	VersionID string

	// Versioned reports whether the object's bucket has versioning
	// configured (Enabled or Suspended). When false, no version header is
	// emitted.
	Versioned bool

	// IsDeleteMarker is true when the requested version is a delete marker.
	IsDeleteMarker bool

	// PartsCount will be set for objects that are multipart uploads, but only
	// if a multipart part number is specified.
	PartsCount *int32
}

// CopyObjectResult contains information about the result of a CopyObject
// operation.
type CopyObjectResult struct {
	ContentMD5   [16]byte
	LastModified time.Time
	// VersionID is the wire-encoded version of the new copy ("" on a suspended
	// or unversioned bucket, neither of which reports a version).
	VersionID string
	// SourceVersionID is the wire-encoded version copied, reported when the
	// source bucket is versioned (Enabled or Suspended), else "".
	SourceVersionID string
	PartsCount      int32
}

// DeleteObjectResult contains information about the result of a DeleteObject
// operation.
type DeleteObjectResult struct {
	// Specifies whether the versioned object that was permanently deleted was
	// (true) or was not (false) a delete marker. In a simple DELETE, this
	// header indicates whether (true) or not (false) a delete marker was
	// created.
	IsDeleteMarker bool

	// VersionID is the wire-encoded version affected by the delete, or "" when
	// no version applies (unversioned bucket).
	VersionID string
}

// Prefix represents an optional prefix and delimiter for listing objects in a
// bucket.
type Prefix struct {
	HasPrefix bool
	Prefix    string

	HasDelimiter bool
	Delimiter    string
}

// CommonPrefix computes the common prefix for the given key based on this
// prefix's delimiter. Returns an empty string if the key doesn't match the
// prefix or if no delimiter is set.
func (p Prefix) CommonPrefix(key string) string {
	if !p.HasDelimiter {
		return ""
	}

	after, ok := strings.CutPrefix(key, p.Prefix)
	if !ok {
		return ""
	}

	idx := strings.Index(after, p.Delimiter)
	if idx == -1 {
		return ""
	}

	return p.Prefix + after[:idx+len(p.Delimiter)]
}

func prefixFromQuery(query url.Values) Prefix {
	prefix := Prefix{
		Prefix:    query.Get("prefix"),
		Delimiter: query.Get("delimiter"),
	}
	_, prefix.HasPrefix = query["prefix"]
	_, prefix.HasDelimiter = query["delimiter"]

	prefix.HasPrefix = prefix.HasPrefix && prefix.Prefix != ""
	prefix.HasDelimiter = prefix.HasDelimiter && prefix.Delimiter != ""
	return prefix
}

// PutObjectResult contains information about the result of a PutObject
// operation.
type PutObjectResult struct {
	// ContentMD5 is the MD5 checksum of the object data.
	ContentMD5 [16]byte

	// VersionID is the wire-encoded version to report, or "" on a suspended or
	// unversioned bucket.
	VersionID string
}

// PutObjectOptions contains options for a PutObject operation.
//
// ContentMD5 and ContentSHA256 are optional checksums of the object data. If
// set, the backend needs to validate the data against the provided checksums
// and return an error if they don't match.
type PutObjectOptions struct {
	Meta          map[string]string
	ContentLength int64
	ContentMD5    *[16]byte
	ContentSHA256 *[32]byte
}

var unsupportedObjectSubresources = map[string]struct{}{
	"acl":          {},
	"attributes":   {},
	"legal-hold":   {},
	"renameObject": {},
	"restore":      {},
	"retention":    {},
	"select":       {},
	"tagging":      {},
	"torrent":      {},
}

// routeObject handles URLs that contain both a bucket path segment and an
// object path segment.
func (s *s3) routeObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string) error {
	for param := range r.URL.Query() {
		if _, ok := unsupportedObjectSubresources[param]; ok {
			return fmt.Errorf("unsupported query subresource %q: %w", param, s3errs.ErrNotImplemented)
		}
	}

	// routes with optional authentication
	switch r.Method {
	case http.MethodGet:
		return s.getObject(w, r, accessKeyID, bucket, object, NoVersion())
	case http.MethodHead:
		return s.headObject(w, r, accessKeyID, bucket, object, NoVersion())
	default:
	}

	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}

	// routes with mandatory authentication
	switch r.Method {
	case http.MethodPut:
		return s.putObject(w, r, validatedKey, bucket, object)
	case http.MethodDelete:
		return s.deleteObject(w, r, validatedKey, bucket, object, NoVersion())
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

func (s *s3) copyObject(w http.ResponseWriter, r *http.Request, accessKeyID, dstBucket, dstObject string, meta map[string]string) error {
	source := meta["X-Amz-Copy-Source"]
	log := s.logger.With(zap.String("dstBucket", dstBucket),
		zap.String("dstObject", dstObject),
		zap.String("source", source),
	)
	log.Debug("copy object")

	if len(source) > KeySizeLimit {
		return s3errs.ErrKeyTooLongError
	}

	// parse source
	srcBucket, srcObject, srcVersion, err := parseSource(source)
	if err != nil {
		return err
	}

	// copying to the same key without REPLACE is not allowed, unless a specific
	// source version is addressed (a version restore).
	replace := r.Header.Get("x-amz-metadata-directive") == "REPLACE"
	if srcBucket == dstBucket && srcObject == dstObject && !replace && !srcVersion.Specified {
		return s3errs.ErrInvalidRequest
	}

	// if If-Match or If-None-Match headers are present, handle them
	ifMatch := r.Header.Get("X-Amz-Copy-Source-If-Match")
	ifNoneMatch := r.Header.Get("X-Amz-Copy-Source-If-None-Match")
	if ifMatch != "" || ifNoneMatch != "" {
		obj, err := s.backend.HeadObject(r.Context(), &accessKeyID, srcBucket, srcObject, srcVersion, nil, nil)
		if err != nil {
			return err
		} else if obj.Body != nil {
			obj.Body.Close()
		}

		// a delete marker has no data to copy. Resolve this before the ETag
		// preconditions, which would otherwise match its zeroed ETag.
		if obj.IsDeleteMarker {
			if srcVersion.Specified {
				return s3errs.ErrInvalidRequest
			}
			return s3errs.ErrNoSuchKey
		}

		var partsCount int
		if obj.PartsCount != nil {
			partsCount = int(*obj.PartsCount)
		}
		etag := FormatETag(obj.ContentMD5[:], partsCount)
		if ifMatch != "" && !etagMatches(ifMatch, etag) {
			return s3errs.ErrPreconditionFailed
		}
		if ifNoneMatch != "" && etagMatches(ifNoneMatch, etag) {
			return s3errs.ErrPreconditionFailed
		}
	}

	result, err := s.backend.CopyObject(r.Context(), accessKeyID, srcBucket, srcObject, srcVersion, dstBucket, dstObject, replace, meta)
	if err != nil {
		return err
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	if result.SourceVersionID != "" {
		w.Header().Set("x-amz-copy-source-version-id", result.SourceVersionID)
	}

	etag := FormatETag(result.ContentMD5[:], int(result.PartsCount))
	w.Header().Set("ETag", etag)
	return writeXMLResponse(w, http.StatusOK, ObjectCopyResult{
		ETag:         etag,
		LastModified: NewContentTime(result.LastModified),
	})
}

func (s *s3) deleteObject(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket, object string, version VersionRequest) error {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("version", version.LogValue()))
	log.Debug("delete object")

	// check preconditions
	oid := ObjectID{Key: object}
	if version.Specified {
		id := version.ID
		oid.VersionID = &id
	}
	if v, exists := r.Header["X-Amz-If-Match-Last-Modified-Time"]; exists {
		t, err := time.Parse(http.TimeFormat, v[0])
		if err != nil {
			return s3errs.ErrInvalidArgument
		}
		lastMod := NewHttpTime(t)
		oid.LastModifiedTime = &lastMod
	}

	if v, exists := r.Header["X-Amz-If-Match-Size"]; exists {
		size, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			return s3errs.ErrInvalidArgument
		}
		oid.Size = &size
	}

	if v, exists := r.Header["If-Match"]; exists && v[0] != "*" {
		oid.ETag = &v[0]
	}

	result, err := s.backend.DeleteObject(r.Context(), accessKeyID, bucket, oid)
	if err != nil {
		return err
	}

	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *s3) deleteObjects(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket string) error {
	log := s.logger.With(zap.String("bucket", bucket))
	log.Debug("delete objects")

	var req DeleteRequest
	if err := decodeXMLBody(r.Body, &req); err != nil {
		return err
	}

	if len(req.Objects) > MaxBucketKeys {
		return s3errs.ErrMalformedXML
	}

	for i := range req.Objects {
		// the wire value "null" maps to the null version (empty internally); a
		// nil VersionID means no version was specified.
		if v := req.Objects[i].VersionID; v != nil && *v == Null {
			empty := ""
			req.Objects[i].VersionID = &empty
		}
	}

	res, err := s.backend.DeleteObjects(r.Context(), accessKeyID, bucket, req.Objects)
	if err != nil {
		return err
	}

	if req.Quiet {
		res.Deleted = nil
	}
	return writeXMLResponse(w, http.StatusOK, res)
}

func (s *s3) getObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string, version VersionRequest) error {
	return s.serveObject(w, r, accessKeyID, bucket, object, version, false)
}

func (s *s3) headObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string, version VersionRequest) error {
	return s.serveObject(w, r, accessKeyID, bucket, object, version, true)
}

// serveObject implements the shared logic for GET and HEAD on a /bucket/object
// URL. The only meaningful difference is that HEAD never streams the body. When
// head is true, the backend's HeadObject is used and any returned body is
// closed; otherwise GetObject is used and the body is streamed to the response.
func (s *s3) serveObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string, version VersionRequest, head bool) error {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("version", version.LogValue()))
	if head {
		log.Debug("head object")
	} else {
		log.Debug("get object")
	}

	partNumber, err := parsePartNumber(r.URL.Query().Get("partNumber"))
	if err != nil {
		return err
	}

	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	} else if rnge != nil && partNumber != nil {
		return s3errs.ErrInvalidRequest // can't combine range and partNumber
	}

	// retrieve object (HEAD fetches metadata only)
	var obj *Object
	if head {
		obj, err = s.backend.HeadObject(r.Context(), accessKeyID, bucket, object, version, rnge, partNumber)
	} else {
		obj, err = s.backend.GetObject(r.Context(), accessKeyID, bucket, object, version, rnge, partNumber)
	}
	if err != nil {
		return err
	}
	// the body is only consumed on the successful GET path below; close it on
	// every other path (HEAD, delete marker, error)
	defer func() {
		if obj.Body != nil {
			obj.Body.Close()
		}
	}()

	// a delete marker has no data and cannot be retrieved with GET or HEAD.
	if obj.IsDeleteMarker {
		return deleteMarkerError(w, obj, version)
	}

	// write headers
	setVersionHeaders(w, obj)
	if accessKeyID != nil {
		s.setLifecycleExpirationHeader(r.Context(), w, *accessKeyID, bucket, object, obj.LastModified)
	}
	if err := writeGetOrHeadObjectHeaders(obj, w, r); err != nil {
		return err
	}

	if !head {
		// write body
		body := newErrTrackingReader(obj.Body)
		if _, err := io.Copy(w, body); err != nil {
			if readErr := body.Err(); readErr != nil {
				log.Error("failed to read object body", zap.Error(readErr))
			} else {
				log.Debug("failed to write object body", zap.Error(err))
			}
		}
	}
	return nil
}

// setVersionHeaders sets x-amz-version-id (for versioned objects) and
// x-amz-delete-marker (for delete markers) on the response.
func setVersionHeaders(w http.ResponseWriter, obj *Object) {
	if obj.Versioned {
		w.Header().Set("x-amz-version-id", FormatVersion(obj.VersionID))
	}
	if obj.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
}

// deleteMarkerError returns the error for a GET/HEAD that resolved to a delete
// marker: 404 (NoSuchKey) for the current version, 405 (MethodNotAllowed) for a
// specific version.
func deleteMarkerError(w http.ResponseWriter, obj *Object, version VersionRequest) error {
	setVersionHeaders(w, obj)
	if !version.Specified {
		return s3errs.ErrNoSuchKey
	}
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Allow", "DELETE")
	return s3errs.ErrMethodNotAllowed
}

// prefixSet tracks the common prefixes already rolled up into a listing result,
// so a prefix repeated across rows is only emitted once.
type prefixSet map[string]bool

// Add records prefix as seen, returning false if it had already been added.
func (s prefixSet) Add(prefix string) bool {
	if s[prefix] {
		return false
	}
	s[prefix] = true
	return true
}

// ObjectsListResult contains the result of a ListObjects operation.
type ObjectsListResult struct {
	CommonPrefixes []CommonPrefix
	Contents       []*Content
	IsTruncated    bool
	NextMarker     string

	// prefixes maintains an index of prefixes that have already been seen.
	// This is a convenience for backend implementers like s3bolt and s3mem,
	// which operate on a full, flat list of keys.
	prefixes prefixSet
	maxKeys  int64
}

// NewObjectsListResult creates a new, empty ObjectsListResult. Use Add and
// AddPrefix to populate it.
func NewObjectsListResult(maxKeys int64) *ObjectsListResult {
	return &ObjectsListResult{maxKeys: maxKeys}
}

// Add adds an object to the result.
func (b *ObjectsListResult) Add(item *Content) {
	if len(b.Contents)+len(b.CommonPrefixes) < int(b.maxKeys) {
		b.Contents = append(b.Contents, item)
	}
	if len(b.Contents)+len(b.CommonPrefixes) >= int(b.maxKeys) {
		if b.NextMarker == "" {
			b.NextMarker = item.Key
		} else {
			b.IsTruncated = true
		}
	}
}

// AddPrefix adds a common prefix to the result. If the prefix has already been
// added, this is a no-op.
func (b *ObjectsListResult) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = prefixSet{}
	}
	if !b.prefixes.Add(prefix) {
		return
	}
	if len(b.Contents)+len(b.CommonPrefixes) < int(b.maxKeys) {
		b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
	}
	if len(b.Contents)+len(b.CommonPrefixes) >= int(b.maxKeys) {
		if b.NextMarker == "" {
			b.NextMarker = prefix
		} else {
			b.IsTruncated = true
		}
	}
}

func (s *s3) listObjectsV1(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	log := s.logger.With(zap.String("bucket", bucket))
	log.Debug("list objects v1")

	// parse arguments
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	maxKeys, err := parseClampedInt(q.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return err
	}

	page := ListObjectsPage{MaxKeys: maxKeys}
	if _, hasMarker := q["marker"]; hasMarker {
		marker := q.Get("marker")
		page.Marker = &marker
	}

	// list objects
	objects, err := s.backend.ListObjects(r.Context(), accessKeyID, bucket, prefix, page)
	if err != nil {
		return err
	}

	// URL-escape object keys and common prefixes if requested
	if r.FormValue("encoding-type") == "url" {
		for i := range objects.Contents {
			objects.Contents[i].Key = urlEscape(objects.Contents[i].Key)
		}
		for i := range objects.CommonPrefixes {
			objects.CommonPrefixes[i].Prefix = urlEscape(objects.CommonPrefixes[i].Prefix)
		}
	}

	result := &ListObjectsV1Result{
		ListObjectsResultBase: ListObjectsResultBase{
			Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:           bucket,
			CommonPrefixes: objects.CommonPrefixes,
			Contents:       objects.Contents,
			IsTruncated:    objects.IsTruncated,
			Delimiter:      prefix.Delimiter,
			Prefix:         prefix.Prefix,
			MaxKeys:        page.MaxKeys,
			EncodingType:   q.Get("encoding-type"),
		},
		Marker:     q.Get("marker"),
		NextMarker: objects.NextMarker,
	}

	return writeXMLResponse(w, http.StatusOK, result)
}

func (s *s3) listObjectsV2(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	log := s.logger.With(zap.String("bucket", bucket))
	log.Debug("list objects")

	// parse arguments
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listObjectsPageFromQuery(q)
	if err != nil {
		return err
	}

	// don't allow unordered listing with delimiter
	if _, unordered := q["allow-unordered"]; unordered && prefix.Delimiter != "" {
		return s3errs.ErrInvalidArgument
	}

	// list objects
	objects, err := s.backend.ListObjects(r.Context(), accessKeyID, bucket, prefix, page)
	if err != nil {
		return err
	}

	// URL-escape object keys and common prefixes if requested
	if r.FormValue("encoding-type") == "url" {
		for i := range objects.Contents {
			objects.Contents[i].Key = urlEscape(objects.Contents[i].Key)
		}
		for i := range objects.CommonPrefixes {
			objects.CommonPrefixes[i].Prefix = urlEscape(objects.CommonPrefixes[i].Prefix)
		}
	}

	// continuation token should be omitted if not set, but if it is set to an
	// empty string, it should still be included.
	var continuationToken *string
	if _, hasToken := q["continuation-token"]; hasToken {
		token := q.Get("continuation-token")
		continuationToken = &token
	}

	// prepare result
	var result = &ListObjectsV2Result{
		ListObjectsResultBase: ListObjectsResultBase{
			Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:           bucket,
			CommonPrefixes: objects.CommonPrefixes,
			Contents:       objects.Contents,
			IsTruncated:    objects.IsTruncated,
			Delimiter:      prefix.Delimiter,
			Prefix:         prefix.Prefix,
			MaxKeys:        page.MaxKeys,
			EncodingType:   q.Get("encoding-type"),
		},
		KeyCount:          int64(len(objects.CommonPrefixes) + len(objects.Contents)),
		StartAfter:        q.Get("start-after"),
		ContinuationToken: continuationToken,
	}
	if objects.NextMarker != "" {
		// S3 continuation tokens are opaque base64-like values, so we just
		// base64 encode the next marker and hand it out as a continuation
		// token.
		result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(objects.NextMarker))
	}

	return writeXMLResponse(w, http.StatusOK, result)
}

func (s *s3) listObjectVersions(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	log := s.logger.With(zap.String("bucket", bucket))
	log.Debug("list object versions")

	// parse arguments
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listObjectVersionsPageFromQuery(q)
	if err != nil {
		return err
	}

	versions, err := s.backend.ListObjectVersions(r.Context(), accessKeyID, bucket, prefix, page)
	if err != nil {
		return err
	}

	encodeURL := q.Get("encoding-type") == "url"
	escape := func(s string) string {
		if encodeURL {
			return urlEscape(s)
		}
		return s
	}

	result := ListObjectVersionsResult{
		ListObjectsResultBase: ListObjectsResultBase{
			Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:        bucket,
			Prefix:      escape(prefix.Prefix),
			MaxKeys:     page.MaxKeys,
			Delimiter:   escape(prefix.Delimiter),
			IsTruncated: versions.IsTruncated,
		},
		KeyMarker:       escape(q.Get("key-marker")),
		VersionIDMarker: q.Get("version-id-marker"),
		Versions:        []VersionListEntry{},
	}
	if encodeURL {
		result.EncodingType = "url"
	}
	if versions.IsTruncated {
		result.NextKeyMarker = escape(versions.NextKeyMarker)
		// NextVersionIDMarker is already wire-encoded by the backend ("null" for
		// the null version, "" for a common-prefix boundary).
		result.NextVersionIDMarker = versions.NextVersionIDMarker
	}
	for _, v := range versions.Versions {
		if v.IsDeleteMarker {
			result.Versions = append(result.Versions, DeleteMarker{
				Key:          escape(v.Key),
				VersionID:    FormatVersion(v.VersionID),
				IsLatest:     v.IsLatest,
				LastModified: NewContentTime(v.LastModified),
				Owner:        v.Owner,
			})
			continue
		}
		result.Versions = append(result.Versions, Version{
			Key:          escape(v.Key),
			VersionID:    FormatVersion(v.VersionID),
			IsLatest:     v.IsLatest,
			LastModified: NewContentTime(v.LastModified),
			Size:         v.Size,
			ETag:         v.ETag,
			Owner:        v.Owner,
		})
	}
	result.CommonPrefixes = versions.CommonPrefixes
	for i := range result.CommonPrefixes {
		result.CommonPrefixes[i].Prefix = escape(result.CommonPrefixes[i].Prefix)
	}
	return writeXMLResponse(w, http.StatusOK, result)
}

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

// VersionFromQuery resolves the ?versionId= subresource into a VersionRequest.
// An absent or empty value addresses the current version; the wire value "null"
// (sent by Boto) addresses the null version, represented internally as "".
func VersionFromQuery(qv []string) VersionRequest {
	if len(qv) == 0 || qv[0] == "" {
		return NoVersion()
	}
	if qv[0] == Null {
		return SpecificVersion("") // the null version
	}
	return SpecificVersion(qv[0])
}

// LogValue renders the requested version for logging: empty when no version was
// specified, otherwise the wire encoding.
func (v VersionRequest) LogValue() string {
	if !v.Specified {
		return ""
	}
	return FormatVersion(v.ID)
}

func (s *s3) putObject(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket, object string) (err error) {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object))
	log.Debug("put object")

	// check key length
	if len(object) > KeySizeLimit {
		return s3errs.ErrKeyTooLongError
	}

	// extract metadata headers
	meta, err := metadataHeaders(r.Header, MetadataSizeLimit)
	if err != nil {
		return err
	}

	if _, ok := meta["X-Amz-Copy-Source"]; ok {
		return s.copyObject(w, r, accessKeyID, bucket, object, meta)
	}

	// content length is mandatory
	if r.ContentLength < 0 {
		return s3errs.ErrMissingContentLength
	}

	// extract Content-MD5 header
	var contentMD5 *[16]byte
	if _, exists := r.Header["Content-Md5"]; exists {
		md5Base64 := r.Header.Get("Content-Md5")
		contentMD5 = new([16]byte)
		if n, err := base64.StdEncoding.Decode(contentMD5[:], []byte(md5Base64)); err != nil || n != len(contentMD5) {
			return s3errs.ErrInvalidDigest
		}
	}

	// extract SHA256 checksum from "X-Amz-Content-Sha256" header if present
	contentSHA256, err := auth.Sha256HashFromRequest(r)
	if err != nil {
		return err
	}

	res, err := s.backend.PutObject(r.Context(), accessKeyID, bucket, object, r.Body, PutObjectOptions{
		ContentLength: r.ContentLength,
		ContentMD5:    contentMD5,
		ContentSHA256: contentSHA256,
		Meta:          meta,
	})
	if err != nil {
		return err
	}

	s.setLifecycleExpirationHeader(r.Context(), w, accessKeyID, bucket, object, time.Now())
	if res.VersionID != "" {
		w.Header().Set("x-amz-version-id", res.VersionID)
	}
	w.Header().Set("ETag", FormatETag(res.ContentMD5[:], 0))
	return nil
}

// FormatETag formats the given hash as an S3 ETag string.
func FormatETag(hash []byte, partsCount int) string {
	if partsCount > 0 {
		return `"` + hex.EncodeToString(hash) + "-" + strconv.Itoa(partsCount) + `"`
	}
	return `"` + hex.EncodeToString(hash) + `"`
}

// ParseETag attempts to parse the given ETag string into a 16-byte MD5 sum.
// Returns a zero array if the ETag is empty or invalid.
func ParseETag(s string) [16]byte {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	if s == "" {
		return [16]byte{}
	}

	// strip multipart suffix
	if idx := strings.LastIndex(s, "-"); idx != -1 {
		s = s[:idx]
	}

	var etag [16]byte
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return [16]byte{}
	}
	copy(etag[:], decoded)

	return etag
}

func etagMatches(header, etag string) bool {
	if header == "*" {
		return true
	}
	for _, v := range strings.Split(header, ",") {
		if strings.TrimSpace(v) == etag {
			return true
		}
	}
	return false
}

// parseSource parses an X-Amz-Copy-Source string and returns the bucket,
// object and the source version from a "?versionId=<id>" suffix (the current
// version when absent).
func parseSource(source string) (bucket, object string, version VersionRequest, err error) {
	parts := strings.SplitN(strings.TrimPrefix(source, "/"), "/", 2)
	if len(parts) != 2 {
		return "", "", NoVersion(), s3errs.ErrInvalidArgument
	}
	srcBucket := parts[0]
	objAndQuery := strings.SplitN(parts[1], "?", 2)

	srcObject, err := url.QueryUnescape(objAndQuery[0])
	if err != nil {
		return "", "", NoVersion(), s3errs.ErrInvalidArgument
	}
	if len(objAndQuery) == 2 {
		q, err := url.ParseQuery(objAndQuery[1])
		if err != nil {
			return "", "", NoVersion(), s3errs.ErrInvalidArgument
		}
		version = VersionFromQuery(q["versionId"])
	}
	return srcBucket, srcObject, version, nil
}

// metadataHeaders extracts S3 metadata headers from the given HTTP headers.
func metadataHeaders(headers map[string][]string, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		hk = textproto.CanonicalMIMEHeaderKey(hk)
		if strings.HasPrefix(hk, "X-Amz-") ||
			hk == "Content-Type" ||
			hk == "Content-Disposition" ||
			hk == "Content-Encoding" ||
			hk == "Cache-Control" ||
			hk == "Expires" {
			meta[hk] = hv[0]
		}
	}

	// strip aws-chunked from Content-Encoding since it is a transfer encoding
	// detail that should not be persisted
	if ce, ok := meta["Content-Encoding"]; ok {
		var parts []string
		for _, p := range strings.Split(ce, ",") {
			p = strings.TrimSpace(p)
			if p != "" && !strings.EqualFold(p, "aws-chunked") {
				parts = append(parts, p)
			}
		}
		if len(parts) == 0 {
			delete(meta, "Content-Encoding")
		} else {
			meta["Content-Encoding"] = strings.Join(parts, ", ")
		}
	}

	metaSize := 0
	for k, v := range meta {
		metaSize += len(k) + len(v)
	}

	if sizeLimit > 0 && metaSize > sizeLimit {
		return meta, s3errs.ErrMetadataTooLarge
	}

	return meta, nil
}

// ListObjectsPage specifies pagination options for listing objects in a bucket.
type ListObjectsPage struct {
	// FetchOwner specifies whether owner information should be included in
	// the response. If nil or false, the Owner field of returned objects will
	// be nil. If true, the Owner field of returned objects will be set.
	FetchOwner *bool

	// Marker specifies the key in the bucket that represents the last item in
	// the previous page. The first key in the returned page will be the next
	// lexicographically (UTF-8 binary) sorted key after Marker. If HasMarker is
	// true, this must be non-empty.
	Marker *string

	// MaxKeys sets the maximum number of keys returned in the response body.
	// The response might contain fewer keys, but will never contain more. If
	// additional keys satisfy the search criteria, but were not returned
	// because max-keys was exceeded, the response contains
	// <isTruncated>true</isTruncated>. To return the additional keys, see
	// key-marker and version-id-marker.
	//
	MaxKeys int64
}

// ListObjectVersionsPage specifies pagination options for listing object
// versions in a bucket.
type ListObjectVersionsPage struct {
	// FetchOwner specifies whether owner information should be included.
	FetchOwner *bool

	// KeyMarker is the key to resume listing after, or nil to start from the
	// beginning.
	KeyMarker *string

	// VersionIDMarker is the wire-encoded version to resume within KeyMarker, or
	// nil for all of KeyMarker's versions. The wire value "null" represents the
	// null version.
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

// ObjectRange specifies a byte range within an object. The backend can derive
// this from a ObjectRangeRequest using the size of the object.
type ObjectRange struct {
	Start, Length int64
}

// ObjectRangeRequest specifies a requested byte range within an object. Clients
// provide this since they don't necessarily know the size of an object.
type ObjectRangeRequest struct {
	Start, End int64
	FromEnd    bool
}

// Range computes the actual byte range to retrieve based on the
// ObjectRangeRequest and the total size of the object.
func (o *ObjectRangeRequest) Range(size int64) (*ObjectRange, error) {
	if o == nil {
		return nil, nil
	}

	var start, length int64

	if !o.FromEnd {
		start = o.Start
		end := o.End

		if o.End == -1 {
			// If no end is specified, range extends to end of the file.
			length = size - start
		} else {
			length = end - start + 1
		}
	} else {
		// If no start is specified, end specifies the range start relative
		// to the end of the file.
		end := o.End
		start = size - end
		length = size - start
	}

	if start < 0 || length < 0 || start >= size {
		return nil, s3errs.ErrInvalidRange
	}

	if start+length > size {
		return &ObjectRange{Start: start, Length: size - start}, nil
	}

	return &ObjectRange{Start: start, Length: length}, nil
}

func decodeXMLBody(r io.Reader, v interface{}) error {
	// limit reader to prevent attacks
	limited := io.LimitReader(r, 1<<20) // 1 MiB should be enough for any S3 XML body
	decoder := xml.NewDecoder(limited)
	if err := decoder.Decode(&v); err != nil {
		return s3errs.ErrMalformedXML
	}
	return nil
}

func listObjectsPageFromQuery(query url.Values) (page ListObjectsPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}
	page.MaxKeys = maxKeys

	if _, hasMarker := query["continuation-token"]; hasMarker {
		// list Objects V2 uses continuation-token preferentially, or
		// start-after if continuation-token is missing. continuation-token is
		// an opaque value that looks like this: 1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=.
		// This just looks like base64 junk so we just cheat and base64 encode
		// the next marker and hide it in a continuation-token.
		tok, err := base64.URLEncoding.DecodeString(query.Get("continuation-token"))
		if err != nil {
			return page, s3errs.ErrInvalidArgument
		}
		page.Marker = aws.String(string(tok))
	} else if _, hasMarker := query["start-after"]; hasMarker {
		// list Objects V2 uses start-after if continuation-token is missing:
		page.Marker = aws.String(query.Get("start-after"))
	}

	if query.Get("fetch-owner") == "true" {
		page.FetchOwner = aws.Bool(true)
	}

	return page, nil
}

func listObjectVersionsPageFromQuery(query url.Values) (page ListObjectVersionsPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}

	page.MaxKeys = maxKeys
	page.FetchOwner = aws.Bool(true) // always fetch owner for the versions endpoint

	if _, ok := query["key-marker"]; ok {
		page.KeyMarker = aws.String(query.Get("key-marker"))
	}
	if _, ok := query["version-id-marker"]; ok {
		// a version-id marker is meaningless without a key marker
		if page.KeyMarker == nil {
			return page, s3errs.ErrInvalidArgument
		}
		// the wire value "null" addresses the null version internally.
		v := query.Get("version-id-marker")
		if v == Null {
			v = ""
		}
		page.VersionIDMarker = &v
	}

	return page, nil
}

func parseClampedInt(in string, defaultValue, minValue, maxValue int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, s3errs.ErrInvalidArgument
		}
	}

	if v < minValue {
		v = minValue
	} else if v > maxValue {
		v = maxValue
	}

	return v, nil
}

// parseRangeHeader parses a single byte range from the Range header.
//
// Amazon S3 doesn't support retrieving multiple ranges of data per GET request:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func parseRangeHeader(s string) (*ObjectRangeRequest, error) {
	if s == "" {
		return nil, nil
	}

	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, s3errs.ErrInvalidRange
	}

	ranges := strings.Split(s[len(b):], ",")
	if len(ranges) > 1 {
		return nil, s3errs.ErrInvalidRange
	}

	rnge := strings.TrimSpace(ranges[0])
	if rnge == "" {
		return nil, s3errs.ErrInvalidRange
	}

	i := strings.Index(rnge, "-")
	if i < 0 {
		return nil, s3errs.ErrInvalidRange
	}

	var o ObjectRangeRequest

	start, end := strings.TrimSpace(rnge[:i]), strings.TrimSpace(rnge[i+1:])
	if start == "" {
		o.FromEnd = true

		i, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return nil, s3errs.ErrInvalidRange
		}
		o.End = i
	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i < 0 {
			return nil, s3errs.ErrInvalidRange
		}
		o.Start = i
		if end != "" {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || o.Start > i {
				return nil, s3errs.ErrInvalidRange
			}
			o.End = i
		} else {
			o.End = -1
		}
	}

	return &o, nil
}

// setLifecycleExpirationHeader sets the x-amz-expiration response header when an
// enabled lifecycle expiration rule applies to the object. It is best-effort:
// any error fetching the configuration is swallowed so the request is unaffected.
func (s *s3) setLifecycleExpirationHeader(ctx context.Context, w http.ResponseWriter, accessKeyID, bucket, object string, lastModified time.Time) {
	config, err := s.backend.GetBucketLifecycleConfiguration(ctx, accessKeyID, bucket)
	if err != nil {
		return
	}
	if v := config.ExpirationHeader(object, lastModified); v != "" {
		w.Header().Set("x-amz-expiration", v)
	}
}

// writeGetOrHeadObjectHeaders contains shared logic for constructing headers for
// a HEAD and a GET request for a /bucket/object URL.
func writeGetOrHeadObjectHeaders(obj *Object, w http.ResponseWriter, r *http.Request) error {
	const (
		checksumPrefix = "X-Amz-Checksum-"
		metaPrefix     = "X-Amz-Meta-"
	)

	for mk, mv := range obj.Metadata {
		// ranged responses should not include checksum headers, this prevents
		// clients from checking the checksum of a partial object against the
		// full object checksum
		if obj.Range != nil && strings.HasPrefix(mk, checksumPrefix) {
			continue
		}

		// user metadata key is always returned in lowercase
		if key, found := strings.CutPrefix(mk, metaPrefix); found {
			w.Header()[fmt.Sprintf("%s%s", metaPrefix, strings.ToLower(key))] = []string{mv}
		} else {
			w.Header().Set(mk, mv)
		}
	}

	var partsCount int
	if obj.PartsCount != nil && r.URL.Query().Get("partNumber") == "" {
		partsCount = int(*obj.PartsCount)
	}
	etag := FormatETag(obj.ContentMD5[:], partsCount)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))

	// evaluate conditional headers per RFC 7232 Section 6 precedence:
	// If-Match > If-Unmodified-Since > If-None-Match > If-Modified-Since
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if !etagMatches(ifMatch, etag) {
			return s3errs.ErrPreconditionFailed
		}
	} else if ifUnmodifiedSince := r.Header.Get("If-Unmodified-Since"); ifUnmodifiedSince != "" {
		t, _ := http.ParseTime(ifUnmodifiedSince)
		if !t.IsZero() && obj.LastModified.After(t) {
			return s3errs.ErrPreconditionFailed
		}
	}

	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if etagMatches(ifNoneMatch, etag) {
			return s3errs.ErrNotModified
		}
	} else {
		ifModifiedSince, _ := http.ParseTime(r.Header.Get("If-Modified-Since"))
		if !ifModifiedSince.IsZero() && !ifModifiedSince.Before(obj.LastModified) {
			return s3errs.ErrNotModified
		}
	}

	if obj.PartsCount != nil && r.URL.Query().Get("partNumber") != "" {
		w.Header().Set("x-amz-mp-parts-count", fmt.Sprintf("%d", *obj.PartsCount))
	}

	w.Header().Set("Accept-Ranges", "bytes")
	if obj.Range != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", obj.Range.Start, obj.Range.Start+obj.Range.Length-1, obj.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Range.Length))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	}

	return nil
}

// errTrackingReader is the interface returned by [newErrTrackingReader]: an
// io.Reader that also reports, via Err, the last non-EOF error its underlying
// reader returned.
type errTrackingReader interface {
	io.Reader
	Err() error
}

// newErrTrackingReader wraps r so that, after copying it with io.Copy, the
// caller can tell a read failure from a write failure: io.Copy returns the
// error from whichever side failed, and a nil Err means the failure happened
// while writing to the destination rather than reading from r.
//
// If r implements io.WriterTo the wrapper does too, preserving io.Copy's
// WriterTo fast path. WriteTo bypasses Read, so read errors are not tracked on
// that path, but the standard library's WriterTo readers only fail on the write
// side, so such a failure is correctly attributed to the writer (Err stays
// nil).
func newErrTrackingReader(r io.Reader) errTrackingReader {
	tr := &trackingReader{r: r}
	if wt, ok := r.(io.WriterTo); ok {
		return &trackingWriterTo{trackingReader: tr, wt: wt}
	}
	return tr
}

type trackingReader struct {
	r   io.Reader
	err error
}

func (t *trackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}

func (t *trackingReader) Err() error { return t.err }

// trackingWriterTo is a trackingReader whose underlying reader also implements
// io.WriterTo, so io.Copy keeps using the WriterTo fast path.
type trackingWriterTo struct {
	*trackingReader
	wt io.WriterTo
}

func (t *trackingWriterTo) WriteTo(w io.Writer) (int64, error) { return t.wt.WriteTo(w) }
