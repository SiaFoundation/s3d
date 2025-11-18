package s3

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Backend defines the interface for an S3 backend that data uploaded via the S3
// API will be stored in.
type Backend interface {
	auth.KeyStore

	// CopyObject copies an object from the source bucket and object key to the
	// destination bucket and object key. The provided metadata map contains any
	// metadata that should be merged into the copied object except for the
	// x-amz-acl header.
	//
	// - If the source bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the source object does not exist, [ErrNoSuchKey] must be returned.
	//
	// - If the destination bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the access key does not have permission to read the source object or
	//   write to the destination bucket, [ErrAccessDenied] must be returned.
	//
	// - If the source and destination are the same, the object is kept but its metadata
	//   is merged with the provided metadata.
	CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, meta map[string]string) (*CopyObjectResult, error)

	// CreateBucket creates a new bucket with the given name for the user
	// identified by the given access key. If the bucket already exists,
	// [ErrBucketAlreadyExists] or [ErrBucketAlreadyOwnedByYou] must be
	// returned depending on the ownership of the bucket.
	CreateBucket(ctx context.Context, accessKeyID, name string) error

	// DeleteBucket deletes the bucket with the given name for the user
	// identified by the given access key.
	//
	// - If the access key does not have permission to delete the bucket,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the bucket is not empty, [ErrBucketNotEmpty] must be returned.
	DeleteBucket(ctx context.Context, accessKeyID, name string) error

	// DeleteObject deletes the object with the given key from the specified
	// bucket for the user identified by the given access key.
	//
	// - If the access key does not have permission to delete the object,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the object with the given key in the specified bucket does not exist,
	//   [ErrNoSuchKey] must be returned.
	DeleteObject(ctx context.Context, accessKeyID, bucket, object string) (*DeleteObjectResult, error)

	// DeleteObjects deletes multiple objects from the specified bucket for the
	// user identified by the given access key.
	//
	// - If the access key does not have permission to delete the objects,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If any of the objects with the given keys in the specified bucket do not
	//   exist, they must still be reported as deleted.
	DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []ObjectID) (*ObjectsDeleteResult, error)

	// GetObject retrieves the object with the given key from the specified
	// bucket for the user identified by the given access key. The provided
	// range is either nil if no range was requested, or contains the requested,
	// byte range.
	//
	// - If the access key does not have permission to access the object,
	//   [ErrAccessDenied] must be returned. A 'nil' accessKeyID indicates the
	//   anonymous user.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the object with the given key in the specified bucket does not exist,
	//   [ErrNoSuchKey] must be returned.
	//
	// - If the requested range is not satisfiable, [ErrInvalidRange] must be
	//   returned. You can use the 'Range' method on 'rnge' for that.
	GetObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *ObjectRangeRequest) (*Object, error)

	// HeadBucket checks if the bucket with the given name exists and is
	// accessible for the user identified by the given access key.
	//
	// - If the access key does not have permission to access the bucket,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	HeadBucket(ctx context.Context, accessKeyID, name string) error

	// HeadObject is like GetObject but only retrieves the metadata of the
	// object and returns an empty body.
	HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, rnge *ObjectRangeRequest) (*Object, error)

	// ListBuckets lists all available buckets for the user identified by the
	// given access key.
	ListBuckets(ctx context.Context, accessKeyID string) ([]BucketInfo, error)

	// ListObjects lists objects in the specified bucket for the user identified
	// by the given access key. The backend should use the prefix to limit the
	// contents of the bucket and sort the results into the Contents and
	// CommonPrefixes fields of the returned ObjectsListResult.
	//
	// - If the access key does not have permission to list objects in the bucket,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	ListObjects(ctx context.Context, accessKeyID *string, bucket string, prefix Prefix, page ListObjectsPage) (*ObjectsListResult, error)

	// PutObject puts an object with the given key into the specified bucket.
	//
	// - If the access key does not have permission to store the object,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the object already exists, it must be overwritten.
	//   NOTE: Versioning is not supported yet.
	//
	// - If the bytes read from 'r' do not match 'contentLength',
	//   [ErrIncompleteBody] must be returned.
	//
	// - If ContentMD5 is set in opts, and the MD5 checksum of the data read
	//   from 'r' does not match, [ErrBadDigest] must be returned.
	PutObject(ctx context.Context, accessKeyID string, bucket, object string, r io.Reader, opts PutObjectOptions) (*PutObjectResult, error)

	// CreateMultipartUpload creates a new multipart upload for the specified
	// key in the specified bucket.
	//
	// - If the access key does not have permission to store the object,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	CreateMultipartUpload(ctx context.Context, accessKeyID, bucket, object string, opts CreateMultipartUploadOptions) (*CreateMultipartUploadResult, error)

	// UploadPart uploads a single part for a previously initiated multipart
	// upload.
	//
	// - If the access key does not have permission to write to the object,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the multipart upload ID is not known or no longer active,
	//   [ErrNoSuchUpload] must be returned.
	//
	// - If the bytes read from 'r' do not match 'ContentLength',
	//   [ErrIncompleteBody] must be returned.
	//
	// - If ContentMD5 or ContentSHA256 are set in opts, and the checksums of
	//   the data read from 'r' do not match, [ErrBadDigest] must be returned.
	UploadPart(ctx context.Context, accessKeyID, bucket, object, uploadID string, r io.Reader, opts UploadPartOptions) (*UploadPartResult, error)

	// ListParts lists uploaded parts for the specified multipart upload.
	//
	// - If the access key does not have permission to list parts,
	//   [ErrAccessDenied] must be returned.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the multipart upload ID is not known or no longer active,
	//   [ErrNoSuchUpload] must be returned.
	ListParts(ctx context.Context, accessKeyID, bucket, object, uploadID string, page ListPartsPage) (*ListPartsResult, error)

	// CompleteMultipartUpload completes a multipart upload by assembling the
	// previously uploaded parts into the final object.
	//
	// - If the access key does not have permission to write to the object,
	//   [ErrAccessDenied] must be returned.
	//
	// - If any referenced part is missing or its ETag does not match,
	//   [ErrInvalidPart] must be returned.
	//
	// - If the list of parts is not strictly ordered by part number,
	//   [ErrInvalidPartOrder] must be returned.
	CompleteMultipartUpload(ctx context.Context, accessKeyID, bucket, object, uploadID string, parts []CompletedPart) (*CompleteMultipartUploadResult, error)
}

type s3 struct {
	backend         Backend
	hostBucketBases []string
	logger          *zap.Logger
	region          string
}

// Option is a configuration option for the S3 API handler.
type Option func(*s3)

// WithLogger sets the logger for the S3 API handler.
func WithLogger(logger *zap.Logger) Option {
	return func(s *s3) {
		s.logger = logger.Named("s3")
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

// WithRegion sets the AWS region for the S3 API handler. If empty, all regions
// are allowed during authentication. If set, only requests signed for the given
// region will be accepted.
func WithRegion(region string) Option {
	return func(s *s3) {
		s.region = region
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

	// base router
	handler := auth.AuthenticatedHandler(auth.AuthenticatedHandlerFunc(s3.routeBase))

	// TODO: We might have to wrap the base router in a CORS middleware

	// handle virtual-hosted style bucket URLs
	if len(s3.hostBucketBases) > 0 {
		handler = s3.hostBucketBaseMiddleware(handler)
	}

	// authentication middleware
	return s3.authMiddleware(handler)
}

// authMiddleware is an HTTP middleware that authenticates requests using AWS v4
// signing. If authentication is successful, the wrapped handler is called with
// the access key ID of the authenticated user.
// - If authentication fails, an error response is sent and the wrapped handler
// is not called.
// - If the request is not signed, the wrapped handler is called with an empty
// access key ID, indicating an anonymous request.
func (s s3) authMiddleware(handler auth.AuthenticatedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.logger.Debug("authenticating request",
			zap.String("method", req.Method),
			zap.String(auth.HeaderAuthorization, req.Header.Get(auth.HeaderAuthorization)),
			zap.String(auth.HeaderXAMZContentSHA256, req.Header.Get(auth.HeaderXAMZContentSHA256)),
			zap.String(auth.HeaderXAMZDate, req.Header.Get(auth.HeaderXAMZDate)))

		var accessKeyID *string
		if req.Header.Get(auth.HeaderAuthorization) != "" {
			// NOTE: If 'region' is empty here, all regions are allowed.
			region := s.region

			akid, err := auth.HandleAuth(req, s.backend, region, time.Now())
			if err != nil {
				s.logger.Debug("authentication failed", zap.Error(err))
				writeErrorResponse(w, err)
				return
			}
			accessKeyID = &akid
		}

		handler.ServeHTTP(w, req, accessKeyID)
	})
}

// hostBucketBaseMiddleware forces the server to use VirtualHost-style bucket URLs:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func (s *s3) hostBucketBaseMiddleware(handler auth.AuthenticatedHandler) auth.AuthenticatedHandler {
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

	return auth.AuthenticatedHandlerFunc(func(w http.ResponseWriter, rq *http.Request, accessKeyID *string) {
		host, _, err := net.SplitHostPort(rq.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		bucket, ok := matchBucket(host)
		if !ok {
			handler.ServeHTTP(w, rq, accessKeyID)
			return
		}
		p := rq.URL.Path
		rq.URL.Path = "/" + bucket
		if p != "/" {
			rq.URL.Path += p
		}
		handler.ServeHTTP(w, rq, accessKeyID)
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
func (s *s3) routeBase(w http.ResponseWriter, r *http.Request, accessKeyID *string) {
	// close and drain the request body to allow connection reuse
	defer func() {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
	}()

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
		err = s.routeMultipartUpload(w, r, accessKeyID, bucket, object, uploadID)
	} else if _, ok := query["uploads"]; ok {
		err = s.routeMultipartUploadBase(w, r, accessKeyID, bucket, object)
	} else if _, ok := query["versioning"]; ok {
		err = s.routeVersioning(w, r)
	} else if _, ok := query["versions"]; ok {
		err = s.routeVersions(w, r, accessKeyID, bucket)
	} else if versionID := versionFromQuery(query["versionId"]); versionID != "" {
		err = s.routeVersion(w, r)
	} else if bucket != "" && object != "" {
		err = s.routeObject(w, r, accessKeyID, bucket, object)
	} else if bucket != "" {
		err = s.routeBucket(w, r, accessKeyID, bucket)
	} else if r.Method == "GET" {
		err = s.listBuckets(w, r, accessKeyID)
	} else {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeErrorResponse(w, err)
	}
}

// routeVersioningBase operates on routes that contain '?versioning' in the
// query string. These routes may or may not have a value for bucket; this is
// validated and handled in the target handler functions.
func (s *s3) routeVersioning(w http.ResponseWriter, r *http.Request) error {
	return s3errs.ErrNotImplemented
}

// routeVersions operates on routes that contain '?versions' in the query string.
func (s *s3) routeVersions(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	switch r.Method {
	case http.MethodGet:
		return s.listObjectVersions(w, r, accessKeyID, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// routeVersion operates on routes that contain '?versionId=<id>' in the
// query string.
func (s *s3) routeVersion(w http.ResponseWriter, r *http.Request) error {
	return s3errs.ErrNotImplemented
}

// assertAuth checks if the accessKeyID is not nil, returning an error if it is.
// If the accessKeyID is valid, it is returned as a string. This adds a layer of
// safety to ensure that handlers that require authentication are not
// accidentally called with an empty accessKeyID.
func assertAuth(accessKeyID *string) (string, error) {
	if accessKeyID == nil {
		return "", s3errs.ErrAccessDenied
	}
	return *accessKeyID, nil
}

func versionFromQuery(qv []string) string {
	// The versionId subresource may be the string 'null'; this has been
	// observed coming in via Boto. The S3 documentation for the "DELETE
	// object" endpoint describes a 'null' version explicitly, but we don't
	// want backend implementers to have to special-case this string, so
	// let's hide it in here:
	if len(qv) > 0 && qv[0] != "" && qv[0] != Null {
		return qv[0]
	}
	return ""
}
