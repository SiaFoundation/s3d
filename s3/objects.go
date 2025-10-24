package s3

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Object represents an S3 object stored on the backend.
type Object struct {
	Body         io.ReadCloser
	Hash         []byte
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

	hash, err := s.backend.PutObject(r.Context(), accessKeyID, bucket, object, meta, r.Body, r.ContentLength)
	if err != nil {
		return err
	}

	w.Header().Set("ETag", formatETag(hash))
	return nil
}

func formatETag(hash []byte) string {
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

	etag := `"` + hex.EncodeToString(obj.Hash) + `"`
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
