package s3

import (
	"context"
	"encoding/xml"
	"net"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

// Backend defines the interface for an S3 backend that data uploaded via the S3
// API will be stored in.
type Backend interface {
	// CreateBucket creates a new bucket with the given name. If the bucket
	// already exists, ErrBucketAlreadyExists must be returned.
	CreateBucket(ctx context.Context, name string) error

	// ListBuckets lists all available buckets.
	ListBuckets(ctx context.Context) ([]BucketInfo, error)
}

type s3 struct {
	backend         Backend
	hostBucketBases []string
	logger          *zap.Logger
}

// Option is a configuration option for the S3 API handler.
type Option func(*s3)

// WithLogger sets the logger for the S3 API handler.
func WithLogger(logger *zap.Logger) Option {
	return func(s *s3) {
		s.logger = logger
	}
}

// WithHostBucketBases sets the host bucket bases for the S3 API handler.
// e.g. if you run the handler on "s3.example.com", you would set the base to
// "s3.example.com" to make sure that requests to "mybucket.s3.example.com" are
// routed to the "mybucket" bucket.
func WithHostBucketBases(bases []string) Option {
	return func(s *s3) {
		s.hostBucketBases = bases
	}
}

// New creates an instance of the S3 API handler using the provided backend.
func New(b Backend, opts ...Option) http.Handler {
	s3 := &s3{
		backend: b,
		logger:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(s3)
	}
	s3.logger = s3.logger.Named("s3")

	// base router
	handler := http.Handler(http.HandlerFunc(s3.routeBase))

	// TODO: We might have to wrap the base router in a CORS middleware

	// handle virtual-hosted style bucket URLs
	if len(s3.hostBucketBases) > 0 {
		handler = s3.hostBucketBaseMiddleware(handler)
	}

	// authentication middleware
	// NOTE: This must be the outermost middleware
	handler = s3.authMiddleware(handler)

	return handler
}

// hostBucketBaseMiddleware forces the server to use VirtualHost-style bucket URLs:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func (s *s3) hostBucketBaseMiddleware(handler http.Handler) http.Handler {
	bases := make([]string, len(s.hostBucketBases))
	for idx, base := range s.hostBucketBases {
		bases[idx] = "." + strings.Trim(base, ".")
	}

	matchBucket := func(host string) (bucket string, ok bool) {
		for _, base := range bases {
			if !strings.HasSuffix(host, base) {
				continue
			}
			bucket = host[:len(host)-len(base)]
			if idx := strings.IndexByte(bucket, '.'); idx >= 0 {
				continue
			}
			return bucket, true
		}
		return "", false
	}

	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		host, _, err := net.SplitHostPort(rq.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		bucket, ok := matchBucket(host)
		if !ok {
			handler.ServeHTTP(w, rq)
			return
		}
		p := rq.URL.Path
		rq.URL.Path = "/" + bucket
		if p != "/" {
			rq.URL.Path += p
		}
		handler.ServeHTTP(w, rq)
	})
}

// routeBase is a http.HandlerFunc that dispatches top level routes for
// GoFakeS3.
//
// URLs are assumed to break down into two common path segments, in the
// following format:
//
//	/<bucket>/<object>
//
// The operation for most of the core functionality is built around HTTP
// verbs, but outside the core functionality, the clean separation starts
// to degrade, especially around multipart uploads.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
func (s *s3) routeBase(w http.ResponseWriter, r *http.Request) {
	var (
		path   = strings.TrimPrefix(r.URL.Path, "/")
		parts  = strings.SplitN(path, "/", 2)
		bucket = parts[0]
		query  = r.URL.Query()
		object = ""
		err    error
	)
	if len(parts) == 2 {
		object = parts[1]
	}

	s.logger.Debug("new request", zap.Stringer("url", r.URL),
		zap.String("host", r.Host),
		zap.Strings("parts", parts),
		zap.String("bucket", bucket),
		zap.String("object", object),
	)

	// NOTE: Other projects set some common headers here, such as
	// "x-amz-request-id", "x-amz-id-2" and "Server". It's probably fine to omit
	// them but in case we want to revisit this later, we can find a list of
	// common headers at
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html.
	//
	if uploadID := query.Get("uploadId"); uploadID != "" {
		err = s.routeMultipartUpload(w, r)
	} else if _, ok := query["uploads"]; ok {
		err = s.routeMultipartUploadBase(w, r)
	} else if _, ok := query["versioning"]; ok {
		err = s.routeVersioning(w, r)
	} else if _, ok := query["versions"]; ok {
		err = s.routeVersions(w, r)
	} else if versionID := versionFromQuery(query["versionId"]); versionID != "" {
		err = s.routeVersion(w, r)
	} else if bucket != "" && object != "" {
		err = s.routeObject(w, r)
	} else if bucket != "" {
		err = s.routeBucket(w, r, bucket)
	} else if r.Method == "GET" {
		err = s.listBuckets(w, r)
	} else {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeErrorResponse(w, err)
	}
}

// routeMultipartUpload operates on routes that contain '?uploadId=<id>' in the
// query string.
func (s *s3) routeMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeMultipartUpload is not implemented", http.StatusNotImplemented)
	return nil
}

// routeMultipartUploadBase operates on routes that contain '?uploads' in the
// query string. These routes may or may not have a value for bucket or object;
// this is validated and handled in the target handler functions.
func (s *s3) routeMultipartUploadBase(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeMultipartUploadBase is not implemented", http.StatusNotImplemented)
	return nil
}

// routeVersioningBase operates on routes that contain '?versioning' in the
// query string. These routes may or may not have a value for bucket; this is
// validated and handled in the target handler functions.
func (s *s3) routeVersioning(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeVersioning is not implemented", http.StatusNotImplemented)
	return nil
}

// routeVersions operates on routes that contain '?versions' in the query string.
func (s *s3) routeVersions(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeVersions is not implemented", http.StatusNotImplemented)
	return nil
}

// routeVersion operates on routes that contain '?versionId=<id>' in the
// query string.
func (s *s3) routeVersion(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeVersions is not implemented", http.StatusNotImplemented)
	return nil
}

// routeObject oandles URLs that contain both a bucket path segment and an
// object path segment.
func (s *s3) routeObject(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, "routeObject is not implemented", http.StatusNotImplemented)
	return nil
}

func versionFromQuery(qv []string) string {
	// The versionId subresource may be the string 'null'; this has been
	// observed coming in via Boto. The S3 documentation for the "DELETE
	// object" endpoint describes a 'null' version explicitly, but we don't
	// want backend implementers to have to special-case this string, so
	// let's hide it in here:
	if len(qv) > 0 && qv[0] != "" && qv[0] != "null" {
		return qv[0]
	}
	return ""
}

func writeXMLResponse(w http.ResponseWriter, resp any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))

	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")

	return xe.Encode(resp)
}
