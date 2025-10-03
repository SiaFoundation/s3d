package s3

import (
	"net/http"

	"go.uber.org/zap"
)

// This file is based upon the AWS v4 signing process as documented here:
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html

// The following constants define HTTP header names used in the signing process.
const (
	// The algorithm that was used to calculate the signature.
	HeaderAuthorization = "Authorization"

	// The payload integrity mechanism that was used.
	HeaderXAMZContentSHA256 = "X-Amz-Content-Sha256"

	// The date and time when the signature was calculated. Takes precedence
	// over HeaderDate.
	HeaderXAMZDate = "X-Amz-Date"

	// HeaderDate is the standard HTTP "Date" header. It is used if
	// HeaderXAMZDate is not present.
	HeaderDate = "Date"
)

// The following constants define the supported "Authorization" header
const (
	AuthorizationAWS4HMACSHA256      = "AWS4-HMAC-SHA256"       // SigV4
	AuthorizationAWS4ECDSAP256SHA256 = "AWS4-ECDSA-P256-SHA256" // SigV4A
)

// The following constants define the potential values for the
// "X-Amz-Content-Sha256" header. If the header does not contain one of these
// sentinel values, the value is to be interpreted as the actual checksum of the
// payload.
const (
	ContentUnsignedPayload = "UNSIGNED-PAYLOAD"

	ContentStreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

	ContentStreamingAWS4HMACSHA256Payload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	ContentStreamingAWS4HMACSHA256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	ContentStreamingAWS4ECDSAP256SHA256Payload = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"

	ContentStreamingAWS4ECDSAP256SHA256PayloadTrailer = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
)

// authenticatedHandler is like http.Handler but includes the access key ID of
// the authenticated user.
type authenticatedHandler interface {
	ServeHTTP(w http.ResponseWriter, req *http.Request, accessKeyID string)
}

// authenticatedHandlerFunc is an adapter to allow the use of ordinary functions
// as authenticated handlers. If f is a function with the appropriate signature,
// authenticatedHandlerFunc(f) is an authenticated handler that calls f.
type authenticatedHandlerFunc func(http.ResponseWriter, *http.Request, string)

// ServeHTTP calls f(w, r, accessKeyID).
func (f authenticatedHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request, accessKeyID string) {
	f(w, r, accessKeyID)
}

// authMiddleware is an HTTP middleware that authenticates requests using AWS v4
// signing. If authentication is successful, the wrapped handler is called with
// the access key ID of the authenticated user.
// - If authentication fails, an error response is sent and the wrapped handler
// is not called.
// - If the request is not signed, the wrapped handler is called with an empty
// access key ID, indicating an anonymous request.
func (s *s3) authMiddleware(handler authenticatedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		s.logger.Debug("authenticating request",
			zap.String("method", rq.Method),
			zap.String(HeaderAuthorization, rq.Header.Get(HeaderAuthorization)),
			zap.String(HeaderXAMZContentSHA256, rq.Header.Get(HeaderXAMZContentSHA256)),
			zap.String(HeaderXAMZDate, rq.Header.Get(HeaderXAMZDate)))

		var accessKeyID string // TODO: extract from request

		handler.ServeHTTP(w, rq, accessKeyID)
	})
}
