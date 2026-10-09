package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

// Backend defines the interface for an S3 backend that data uploaded via the S3
// API will be stored in. Bucket and object operations receive already authorized
// requests and must attribute objects and storage to the bucket owner.
type Backend interface {
	auth.KeyStore

	// UserInfo returns user information for the given access key ID.
	UserInfo(ctx context.Context, accessKeyID string) (*UserInfo, error)

	// CopyObject copies an object from the source bucket and object key to the
	// destination bucket and object key. opts.Meta contains any metadata that
	// should either be merged into the copied object or replace the metadata
	// except for the x-amz-acl header. opts.Replace indicates whether the
	// metadata should be replaced (true) or merged (false).
	//
	// - If the source bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the source object does not exist, [ErrNoSuchKey] must be returned.
	//
	// - Errors about the source object, including a failed source
	//   precondition, must be wrapped in [CopySourceError].
	//
	// - If the destination bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the source and destination are the same, the object is kept but its metadata
	//   is merged with the provided metadata.
	//
	// - srcVersion selects the source version: an unspecified request copies the
	//   current version ([ErrNoSuchKey] if it is a delete marker), a specified
	//   request the exact version ("" is the null version, [ErrNoSuchVersion] if
	//   absent).
	//
	// - opts.SourcePreconditions must be evaluated against the source object,
	//   via ObjectPreconditions.CheckCopySource.
	//
	// - opts.DestinationPreconditions must be evaluated against the current
	//   version of the destination, via ObjectPreconditions.CheckWrite.
	CopyObject(ctx context.Context, srcBucket, srcObject string, srcVersion VersionRequest, dstBucket, dstObject string, opts CopyObjectOptions) (*CopyObjectResult, error)

	// CreateBucket creates a new bucket owned by the user identified by the
	// access key. If the bucket exists and is owned by the same user,
	// [ErrBucketAlreadyOwnedByYou] must be returned. If it is owned by another
	// user, [ErrBucketAlreadyExists] must be returned.
	CreateBucket(ctx context.Context, accessKeyID, name string) error

	// DeleteBucket deletes the bucket with the given name.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the bucket is not empty, [ErrBucketNotEmpty] must be returned.
	DeleteBucket(ctx context.Context, name string) error

	// DeleteObject deletes the object with the given key from the specified
	// bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - object's preconditions apply to the version the delete resolves to,
	//   which is the current version unless a version is named, and must be
	//   evaluated via ObjectID.CheckDelete.
	//
	// - With nothing to delete the delete stays the no-op it is when
	//   unconditional, reporting no version and no delete marker. That covers a
	//   key with no versions, a key whose current version is a delete marker,
	//   and a named version that is not there.
	DeleteObject(ctx context.Context, bucket string, object ObjectID) (*DeleteObjectResult, error)

	// DeleteObjects deletes multiple objects from the specified bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If any of the objects with the given keys in the specified bucket do not
	//   exist, they must still be reported as deleted.
	//
	// - Each object carries the same preconditions as DeleteObject, with the
	//   same outcomes, except that a failed precondition fails only that object,
	//   which is reported in the result's Error list.
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectID) (*ObjectsDeleteResult, error)

	// GetObject retrieves the object with the given key from the specified
	// bucket. The provided range is either nil if no range was requested, or
	// contains the requested byte range. If partNumber is not nil, the
	// specified part of a multipart upload is retrieved, this can not be
	// combined with a byte range.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the object with the given key in the specified bucket does not exist,
	//   [ErrNoSuchKey] must be returned. The S3 API handler hides this error
	//   from callers that may not list the bucket.
	//
	// - If the requested range is not satisfiable, [ErrInvalidRange] must be
	//   returned. You can use the 'Range' method on 'rnge' for that.
	//
	// - version unspecified returns the current version; a specified request
	//   returns that exact version ("" is the null version, [ErrNoSuchVersion]
	//   if absent). The result may be a delete marker (Object.IsDeleteMarker).
	GetObject(ctx context.Context, bucket, object string, version VersionRequest, rnge *ObjectRangeRequest, partNumber *int32) (*Object, error)

	// BucketAccessInfo returns the bucket's owner and policy document. It is
	// called on every bucket request.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	BucketAccessInfo(ctx context.Context, bucket string) (BucketAccessInfo, error)

	// HeadObject is like GetObject but only retrieves the metadata of the
	// object and returns an empty body.
	HeadObject(ctx context.Context, bucket, object string, version VersionRequest, rnge *ObjectRangeRequest, partNumber *int32) (*Object, error)

	// ListBuckets lists all available buckets for the user identified by the
	// given access key.
	ListBuckets(ctx context.Context, accessKeyID string) ([]BucketInfo, error)

	// ListObjects lists objects in the specified bucket, using the prefix to
	// limit the contents of the bucket and sort the results into the Contents
	// and CommonPrefixes fields of the returned ObjectsListResult.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	ListObjects(ctx context.Context, bucket string, prefix Prefix, page ListObjectsPage) (*ObjectsListResult, error)

	// PutObject puts an object with the given key into the specified bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - On a versioning-enabled bucket a new version is created and prior
	//   versions are retained; otherwise (unversioned or suspended) the null
	//   version is overwritten in place.
	//
	// - If the bytes read from 'r' do not match 'contentLength',
	//   [ErrIncompleteBody] must be returned.
	//
	// - If ContentMD5 is set in opts, and the MD5 checksum of the data read
	//   from 'r' does not match, [ErrBadDigest] must be returned.
	//
	// - opts.Preconditions must be evaluated against the current version of the
	//   object, via ObjectPreconditions.CheckWrite.
	PutObject(ctx context.Context, bucket, object string, r io.Reader, opts PutObjectOptions) (*PutObjectResult, error)

	// CreateMultipartUpload creates a new multipart upload for the specified
	// key in the specified bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	CreateMultipartUpload(ctx context.Context, bucket, object string, opts CreateMultipartUploadOptions) (*CreateMultipartUploadResult, error)

	// ListMultipartUploads lists in-progress multipart uploads for the given
	// bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	ListMultipartUploads(ctx context.Context, bucket string, opts ListMultipartUploadsOptions, page ListMultipartUploadsPage) (*ListMultipartUploadsResult, error)

	// AbortMultipartUpload aborts an in-progress multipart upload and
	// discards any uploaded parts.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the multipart upload ID is not known or no longer active,
	//   [ErrNoSuchUpload] must be returned.
	AbortMultipartUpload(ctx context.Context, bucket, object string, uploadID UploadID) error

	// UploadPart uploads a single part for a previously initiated multipart
	// upload.
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
	UploadPart(ctx context.Context, bucket, object string, uploadID UploadID, r io.Reader, opts UploadPartOptions) (*UploadPartResult, error)

	// UploadPartCopy copies a part from an existing object as part of a
	// multipart upload.
	//
	// - If either the source or destination bucket does not exist,
	// [ErrNoSuchBucket] must be returned.
	//
	// - If the source object does not exist, [ErrNoSuchKey] must be returned.
	//
	// - Errors about the source object, including a failed source
	//   precondition, must be wrapped in [CopySourceError].
	//
	// - srcVersion selects the source version: an unspecified request copies the
	//   current version ([ErrNoSuchKey] if it is a delete marker), a specified
	//   request the exact version ("" is the null version, [ErrNoSuchVersion] if
	//   absent).
	//
	// - If the multipart upload ID is not known or no longer active,
	//   [ErrNoSuchUpload] must be returned.
	//
	// - opts.SourcePreconditions must be evaluated against the source object,
	//   via ObjectPreconditions.CheckCopySource.
	//
	// - opts.Range must be resolved against the size of the source object, via
	//   CopySourceRange.Range. A resolved range larger than
	//   [MaxUploadPartSize] returns [ErrEntityTooLarge].
	UploadPartCopy(ctx context.Context, srcBucket, srcObject string, srcVersion VersionRequest, dstBucket, dstObject string, uploadID UploadID, opts UploadPartCopyOptions) (*UploadPartCopyResult, error)

	// ListParts lists uploaded parts for the specified multipart upload.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the multipart upload ID is not known or no longer active,
	//   [ErrNoSuchUpload] must be returned.
	ListParts(ctx context.Context, bucket, object string, uploadID UploadID, page ListPartsPage) (*ListPartsResult, error)

	// CompleteMultipartUpload completes a multipart upload by assembling the
	// previously uploaded parts into the final object.
	//
	// - If any referenced part is missing or its ETag does not match,
	//   [ErrInvalidPart] must be returned.
	//
	// - If the part numbers of the parts are not provided in ascending order,
	//   [ErrInvalidPartOrder] must be returned.
	//
	// - If the last part is below the minimum size, [ErrEntityTooSmall] must be returned.
	//
	// - preconditions must be evaluated against the current version of the
	//   object, via ObjectPreconditions.CheckWrite.
	CompleteMultipartUpload(ctx context.Context, bucket, object string, uploadID UploadID, parts []CompleteMultipartPart, preconditions ObjectPreconditions) (*CompleteMultipartUploadResult, error)

	// PutBucketLifecycleConfiguration sets the lifecycle configuration for the
	// specified bucket, replacing any existing configuration.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	PutBucketLifecycleConfiguration(ctx context.Context, bucket string, config LifecycleConfiguration) error

	// GetBucketLifecycleConfiguration returns the lifecycle configuration for
	// the specified bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	//
	// - If the bucket has no lifecycle configuration,
	//   [ErrNoSuchLifecycleConfiguration] must be returned.
	GetBucketLifecycleConfiguration(ctx context.Context, bucket string) (LifecycleConfiguration, error)

	// DeleteBucketLifecycleConfiguration removes the lifecycle configuration
	// for the specified bucket. It is not an error if no configuration exists.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	DeleteBucketLifecycleConfiguration(ctx context.Context, bucket string) error

	// PutBucketPolicy sets the policy for the specified bucket, replacing any
	// existing policy. The document is already validated; the backend stores it
	// verbatim.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	PutBucketPolicy(ctx context.Context, bucket, document string) error

	// DeleteBucketPolicy removes the policy of the specified bucket, revoking
	// any anonymous access. It is not an error if no policy exists.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	DeleteBucketPolicy(ctx context.Context, bucket string) error

	// PutBucketVersioning sets the versioning state of the specified bucket.
	// status is either "Enabled" or "Suspended".
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	PutBucketVersioning(ctx context.Context, bucket, status string) error

	// GetBucketVersioning returns the versioning state of the specified
	// bucket. The status is "" if the bucket has never been configured,
	// otherwise "Enabled" or "Suspended".
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	GetBucketVersioning(ctx context.Context, bucket string) (status string, err error)

	// ListObjectVersions lists all versions (including delete markers) of the
	// objects in the specified bucket.
	//
	// - If the bucket does not exist, [ErrNoSuchBucket] must be returned.
	ListObjectVersions(ctx context.Context, bucket string, prefix Prefix, page ListObjectVersionsPage) (*ObjectVersionsListResult, error)

	// UploadStats returns statistics about the background upload pipeline.
	UploadStats(ctx context.Context) (UploadStats, error)

	// FlushObjects uploads all pending objects to Sia regardless of padding,
	// rather than waiting for the background pipeline to batch them into
	// efficiently packed slabs. It blocks until the uploads complete.
	FlushObjects(ctx context.Context) error

	// CreateSnapshot backs up the database, uploads it to Sia as a tagged
	// snapshot object, and records the object ID.
	CreateSnapshot(ctx context.Context) (Snapshot, error)
}

type s3 struct {
	backend         Backend
	hostBucketBases []string
	logger          *zap.Logger
	region          string

	// policies caches parsed policies by bucket name
	policyMu sync.Mutex
	policies map[string]cachedPolicy
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
// routed to the "mybucket" bucket. "localhost" is always included as a base, so
// virtual-hosted-style requests work out of the box on local setups.
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

	// "localhost" is reserved (RFC 6761) and should therefore never collide
	// with a configured base, so it is always added to support
	// virtual-hosted-style requests during local development.
	if !slices.Contains(s3.hostBucketBases, "localhost") {
		s3.hostBucketBases = append(s3.hostBucketBases, "localhost")
	}

	// base router
	handler := auth.AuthenticatedHandler(auth.AuthenticatedHandlerFunc(s3.routeBase))

	// handle virtual-hosted style bucket URLs
	handler = s3.hostBucketBaseMiddleware(handler)

	// wrap authentication in CORS so unauthenticated preflight requests are
	// handled before they reach the authentication middleware
	return corsMiddleware(s3.authMiddleware(handler))
}

// corsMiddleware adds permissive CORS headers and answers preflight OPTIONS
// requests so browser-based S3 clients can make cross-origin requests.
func corsMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		if r.Header.Get("Origin") != "" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD")
			// the "*" wildcard does not authorize the Authorization header used
			// by AWS SigV4 signing, so it must be listed explicitly.
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, *")
			w.Header().Set("Access-Control-Expose-Headers", "ETag")
			// cache preflight results to avoid re-issuing a preflight for every
			// non-simple request.
			w.Header().Set("Access-Control-Max-Age", "86400")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
}

// NewAdmin creates an HTTP handler that serves the admin API using the provided
// backend. It exposes /prometheus, which serves the background upload stats as
// Prometheus metrics, /stats/uploads, which serves the same stats as JSON,
// /objects/flush, which uploads all pending objects regardless of padding,
// and /snapshots, which backs up the database and uploads it to Sia.
func NewAdmin(b Backend, opts ...Option) http.Handler {
	s3 := &s3{
		backend: b,
		logger:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(s3)
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /prometheus":     s3.handlePrometheus,
		"GET /stats/uploads":  s3.handleGetUploadStats,
		"POST /objects/flush": s3.handleFlushObjects,
		"POST /snapshots":     s3.handleCreateSnapshot,
	})
}

// authMiddleware is an HTTP middleware that authenticates requests using AWS v4
// signing. If authentication is successful, the wrapped handler is called with
// the caller that signed the request.
// - If authentication fails, an error response is sent and the wrapped handler
// is not called.
// - If the request is not signed, the wrapped handler is called with a nil
// caller, indicating an anonymous request.
func (s *s3) authMiddleware(handler auth.AuthenticatedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.logger.Debug("authenticating request",
			zap.String("method", req.Method),
			zap.String(auth.HeaderXAMZContentSHA256, req.Header.Get(auth.HeaderXAMZContentSHA256)),
			zap.String(auth.HeaderXAMZDate, req.Header.Get(auth.HeaderXAMZDate)))

		// NOTE: If 'region' is empty here, all regions are allowed.
		caller, err := auth.HandleAuth(req, s.backend, s.region, time.Now())
		if err != nil {
			s.logger.Debug("authentication failed", zap.Error(err),
				zap.String("accessKeyID", auth.AccessKeyIDFromRequest(req)))
			writeErrorResponse(w, req, err)
			return
		}

		handler.ServeHTTP(w, req, caller)
	})
}

// bucketFromHost returns the bucket name if the given host addresses a bucket
// as a subdomain of one of the configured host bucket bases. The host may
// contain a port.
func (s *s3) bucketFromHost(host string) (bucket string, ok bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	} // otherwise the host contains no port, e.g. behind a reverse proxy
	// listening on 80/443

	for _, base := range s.hostBucketBases {
		suffix := "." + strings.Trim(base, ".")
		if !strings.HasSuffix(host, suffix) {
			continue
		}
		bucket = host[:len(host)-len(suffix)]
		if bucket == "" || strings.IndexByte(bucket, '.') >= 0 {
			continue
		}
		return bucket, true
	}
	return "", false
}

// hostBucketBaseMiddleware handles VirtualHost-style bucket URLs:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func (s *s3) hostBucketBaseMiddleware(handler auth.AuthenticatedHandler) auth.AuthenticatedHandler {
	return auth.AuthenticatedHandlerFunc(func(w http.ResponseWriter, rq *http.Request, caller *auth.Caller) {
		bucket, ok := s.bucketFromHost(rq.Host)
		if !ok {
			handler.ServeHTTP(w, rq, caller)
			return
		}
		p := rq.URL.Path
		rq.URL.Path = "/" + bucket
		if p != "/" {
			rq.URL.Path += p
		}
		handler.ServeHTTP(w, rq, caller)
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
func (s *s3) routeBase(w http.ResponseWriter, r *http.Request, caller *auth.Caller) {
	// NOTE: the request body is not drained here. Handlers consume it on
	// success and writeErrorResponse performs a bounded drain on failure.

	var (
		path   = strings.TrimPrefix(r.URL.Path, "/")
		parts  = strings.SplitN(path, "/", 2)
		bucket = parts[0]
		object = ""
	)
	if len(parts) == 2 {
		object = parts[1]
	}

	log := s.logger.With(zap.String("url", auth.RedactURL(r.URL)),
		zap.String("host", r.Host),
		zap.Strings("parts", parts),
		zap.String("bucket", bucket),
		zap.String("object", object),
	)
	log.Debug("new request")

	// NOTE: Other projects set some common headers here, such as
	// "x-amz-request-id", "x-amz-id-2" and "Server". It's probably fine to omit
	// them but in case we want to revisit this later, we can find a list of
	// common headers at
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html.
	//
	op, err := s.route(r, bucket, object)
	if errors.Is(err, errServiceRootMethod) {
		// an unsigned request arrives here as anonymous, and clients probing the
		// root expect 405 with "Allow: GET" rather than a 403
		w.Header().Set("Allow", http.MethodGet)
		err = s3errs.ErrMethodNotAllowed
	} else if err != nil && caller == nil {
		// an anonymous caller is refused before its request is validated, so it
		// is not told how to correct it
		err = s3errs.ErrAccessDenied
	} else if err == nil {
		var access *bucketAccess
		access, err = s.authorize(r, caller, bucket, object, op)
		if err == nil {
			err = op.serve(w, r, access)
		}
	}
	if err != nil {
		// only consider 5xx errors as "real" errors when logging. Other errors
		// like bad requests or not found errors are not our fault
		var s3Err s3errs.Error
		if errors.As(err, &s3Err) && s3Err.HTTPStatus < http.StatusInternalServerError {
			log.Debug("failed to handle request", zap.Error(err))
		} else {
			log.Error("failed to handle request", zap.Error(err))
		}
		writeErrorResponse(w, r, err)
	}
}

// errServiceRootMethod is returned by route for a request to the service root
// other than ListBuckets.
var errServiceRootMethod = errors.New("method not allowed on the service root")

// route resolves a request to the operation that serves it. Validation is
// left to the handler, so it runs after authorization.
func (s *s3) route(r *http.Request, bucket, object string) (operation, error) {
	query := r.URL.Query()
	if uploadID := query.Get("uploadId"); uploadID != "" {
		return s.routeMultipartUpload(r, object, uploadID)
	} else if _, ok := query["uploads"]; ok {
		return s.routeMultipartUploadBase(r, bucket, object)
	} else if _, ok := query["versioning"]; ok {
		return s.routeVersioning(r)
	} else if _, ok := query["versions"]; ok {
		return s.routeVersions(r)
	} else if version := VersionFromQuery(query["versionId"]); version.Specified {
		return s.routeObjectVersion(r, object, version)
	} else if bucket != "" && object != "" {
		return s.routeObject(r, object)
	} else if bucket != "" {
		return s.routeBucket(r)
	} else if r.Method == http.MethodGet {
		return operation{action: actionListAllMyBuckets, serve: s.listBuckets}, nil
	}
	return operation{}, errServiceRootMethod
}

// routeVersioning routes requests that contain '?versioning' in the query
// string.
func (s *s3) routeVersioning(r *http.Request) (operation, error) {
	switch r.Method {
	case http.MethodGet:
		return operation{action: actionGetBucketVersioning, serve: s.getBucketVersioning}, nil
	case http.MethodPut:
		return operation{action: actionPutBucketVersioning, serve: s.putBucketVersioning}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}

// routeVersions routes requests that contain '?versions' in the query string.
func (s *s3) routeVersions(r *http.Request) (operation, error) {
	if r.Method != http.MethodGet {
		return operation{}, s3errs.ErrMethodNotAllowed
	}
	return operation{action: actionListBucketVersions, serve: s.listObjectVersions}, nil
}

// routeObjectVersion routes GET, HEAD and DELETE requests for a version of an
// object, or for its current version if version is unspecified.
func (s *s3) routeObjectVersion(r *http.Request, object string, version VersionRequest) (operation, error) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		head := r.Method == http.MethodHead
		return operation{action: readAction(version), serve: func(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
			return s.serveObject(w, r, access, object, version, head)
		}}, nil
	case http.MethodDelete:
		return operation{action: deleteAction(version), serve: func(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
			return s.deleteObject(w, r, access, object, version)
		}}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}
