package s3

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
}

// ObjectDeleteResult contains information about the result of a DeleteObject
// operation.
type ObjectDeleteResult struct {
	// Specifies whether the versioned object that was permanently deleted was
	// (true) or was not (false) a delete marker. In a simple DELETE, this
	// header indicates whether (true) or not (false) a delete marker was
	// created.
	IsDeleteMarker bool

	// Returns the version ID of the delete marker created as a result of the
	// DELETE operation. If you delete a specific object version, the value
	// returned by this header is the version ID of the object version deleted.
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
}

// PutObjectOptions contains options for a PutObject operation.
type PutObjectOptions struct {
	Meta          map[string]string
	ContentLength int64
	ContentMD5    *[16]byte
}

// routeObject handles URLs that contain both a bucket path segment and an
// object path segment.
func (s *s3) routeObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string) error {
	// routes with optional authentication
	switch r.Method {
	case http.MethodGet:
		return s.getObject(w, r, accessKeyID, bucket, object, "")
	case http.MethodHead:
		return s.headObject(w, r, accessKeyID, bucket, object, "")
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
		return s.deleteObject(w, r, validatedKey, bucket, object)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

func (s *s3) deleteObject(w http.ResponseWriter, r *http.Request, accessKeyID string, bucket, object string) error {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object))
	log.Debug("delete object")

	result, err := s.backend.DeleteObject(r.Context(), accessKeyID, bucket, object)
	if err != nil {
		return err
	}

	w.Header().Set("x-amz-delete-marker", fmt.Sprint(result.IsDeleteMarker))
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
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
		if req.Objects[i].VersionID == Null {
			req.Objects[i].VersionID = ""
		} else if req.Objects[i].VersionID != "" {
			return s3errs.ErrNotImplemented // versioning not supported
		}
	}

	res, err := s.backend.DeleteObjects(r.Context(), accessKeyID, bucket, req.Objects)
	if err != nil {
		return err
	}

	if req.Quiet {
		res.Deleted = nil
	}
	return writeXMLResponse(w, res)
}

func (s *s3) getObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object, version string) error {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("version", version))
	log.Debug("get object")

	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}

	// retrieve object
	var obj *Object
	if version == "" {
		obj, err = s.backend.GetObject(r.Context(), accessKeyID, bucket, object, rnge)
	} else {
		return s3errs.ErrNotImplemented // versioning not supported
	}
	if err != nil {
		return err
	}
	defer obj.Body.Close()

	// write headers
	if err := writeGetOrHeadObjectHeaders(obj, w, r); err != nil {
		return err
	}

	// write body
	if _, err := io.Copy(w, obj.Body); err != nil {
		// at this point we can't inform the client of the error anymore so
		// we just log it
		s.logger.Error("failed to write object body", zap.Error(err))
	}
	return nil
}

func (s *s3) headObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object, version string) error {
	log := s.logger.With(zap.String("bucket", bucket),
		zap.String("object", object),
		zap.String("version", version))
	log.Debug("head object")

	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}

	// retrieve object metadata
	var obj *Object
	if version == "" {
		obj, err = s.backend.HeadObject(r.Context(), accessKeyID, bucket, object, rnge)
	} else {
		return s3errs.ErrNotImplemented // versioning not supported
	}
	if err != nil {
		return err
	}
	if obj.Body != nil {
		_ = obj.Body.Close() // just in case
	}

	// write headers
	return writeGetOrHeadObjectHeaders(obj, w, r)
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
	prefixes map[string]bool
}

// NewObjectsListResult creates a new, empty ObjectsListResult. Use Add and
// AddPrefix to populate it.
func NewObjectsListResult() *ObjectsListResult {
	return &ObjectsListResult{}
}

// Add adds an object to the result.
func (b *ObjectsListResult) Add(item *Content) {
	b.Contents = append(b.Contents, item)
}

// AddPrefix adds a common prefix to the result. If the prefix has already been
// added, this is a no-op.
func (b *ObjectsListResult) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
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

	// list objects
	objects, err := s.backend.ListObjects(r.Context(), accessKeyID, bucket, prefix, page)
	if err != nil {
		return err
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
		},
		KeyCount:          int64(len(objects.CommonPrefixes) + len(objects.Contents)),
		StartAfter:        q.Get("start-after"),
		ContinuationToken: q.Get("continuation-token"),
	}
	if objects.NextMarker != "" {
		// S3 continuation tokens are opaque base64-like values, so we just
		// base64 encode the next marker and hand it out as a continuation
		// token.
		result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(objects.NextMarker))
	}

	// if fetch-owner is not set, redact owner information
	if _, ok := q["fetch-owner"]; !ok {
		for _, v := range result.Contents {
			v.Owner = nil
		}
	}
	return writeXMLResponse(w, result)
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

	// list objects
	objects, err := s.backend.ListObjects(r.Context(), accessKeyID, bucket, prefix, page)
	if err != nil {
		return err
	}

	// prepare result
	result := ListObjectVersionsResult{
		Xmlns:           "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:            bucket,
		CommonPrefixes:  objects.CommonPrefixes,
		Versions:        []Version{},
		IsTruncated:     objects.IsTruncated,
		Delimiter:       prefix.Delimiter,
		Prefix:          prefix.Prefix,
		MaxKeys:         page.MaxKeys,
		NextKeyMarker:   objects.NextMarker,
		VersionIDMarker: "", // versioning not supported
	}
	for _, obj := range objects.Contents {
		result.Versions = append(result.Versions, Version{
			Key:          obj.Key,
			VersionID:    Null, // versioning not supported
			IsLatest:     true, // versioning not supported
			LastModified: obj.LastModified,
			Size:         obj.Size,
			ETag:         obj.ETag,
			Owner:        obj.Owner,
		})
	}
	return writeXMLResponse(w, result)
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

	// content length is mandatory
	if r.ContentLength < 0 {
		return s3errs.ErrMissingContentLength
	}

	// extract Content-MD5 header
	var contentMD5 *[16]byte
	if md5Base64 := r.Header.Get("Content-MD5"); md5Base64 != "" {
		contentMD5 = new([16]byte)
		if n, err := base64.StdEncoding.Decode(contentMD5[:], []byte(md5Base64)); err != nil || n != len(contentMD5) {
			return s3errs.ErrInvalidDigest
		}
	}

	res, err := s.backend.PutObject(r.Context(), accessKeyID, bucket, object, r.Body, PutObjectOptions{
		ContentLength: r.ContentLength,
		ContentMD5:    contentMD5,
		Meta:          meta,
	})
	if err != nil {
		return err
	}

	w.Header().Set("ETag", FormatETag(res.ContentMD5[:]))
	return nil
}

// FormatETag formats the given hash as an S3 ETag string.
func FormatETag(hash []byte) string {
	return `"` + hex.EncodeToString(hash) + `"`
}

// metadataHeaders extracts S3 metadata headers from the given HTTP headers.
func metadataHeaders(headers map[string][]string, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		if strings.HasPrefix(hk, "X-Amz-") ||
			hk == "Content-Type" ||
			hk == "Content-Disposition" ||
			hk == "Content-Encoding" {
			meta[hk] = hv[0]
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

	return page, nil
}

func listObjectVersionsPageFromQuery(query url.Values) (page ListObjectsPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}

	page.MaxKeys = maxKeys
	page.Marker = aws.String(query.Get("key-marker"))

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

// writeGetOrHeadObjectHeaders contains shared logic for constructing headers for
// a HEAD and a GET request for a /bucket/object URL.
func writeGetOrHeadObjectHeaders(obj *Object, w http.ResponseWriter, r *http.Request) error {
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}

	etag := FormatETag(obj.ContentMD5[:])
	w.Header().Set("ETag", etag)

	if r.Header.Get("If-None-Match") == etag {
		return s3errs.ErrNotModified
	}

	lastModified, _ := time.Parse(http.TimeFormat, obj.Metadata["Last-Modified"])
	ifModifiedSince, _ := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since"))
	if !lastModified.IsZero() && !ifModifiedSince.Before(lastModified) {
		return s3errs.ErrNotModified
	}

	w.Header().Set("Accept-Ranges", "bytes")
	if obj.Range != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", obj.Range.Start, obj.Range.Start+obj.Range.Length-1, obj.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Range.Length))
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	}
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))

	return nil
}
