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

type Object struct {
	Body     io.ReadCloser
	Hash     []byte
	Metadata map[string]string
	Range    *ObjectRange
	Size     int64
}

// routeObject oandles URLs that contain both a bucket path segment and an
// object path segment.
func (s *s3) routeObject(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket, object string) error {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return s.getObject(w, r, accessKeyID, bucket, object, "")
	case http.MethodPut:
		return s3errs.ErrNotImplemented
	case http.MethodDelete:
		return s3errs.ErrNotImplemented
	default:
		return s3errs.ErrMethodNotAllowed
	}
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
	if obj.Body != nil {
		_ = obj.Body.Close() // just in case
	}

	// write headers
	if err := writeGetOrHeadObjectHeaders(obj, w, r); err != nil {
		return err
	}
	return nil
}

type ObjectRange struct {
	Start, Length int64
}

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
	if len(rnge) == 0 {
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

	etag := `"` + hex.EncodeToString(obj.Hash[:]) + `"`
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

	return nil
}
