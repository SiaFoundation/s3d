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

	// The date and time when the signature was calculated. The HTTP Date can
	// also be used but X-AMZ-Date takes precedence.
	HeaderXAMZDate = "X-Amz-Date"
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

func (s *s3) authMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		s.logger.Debug("authenticating request",
			zap.String("method", rq.Method),
			zap.String(HeaderAuthorization, rq.Header.Get(HeaderAuthorization)),
			zap.String(HeaderXAMZContentSHA256, rq.Header.Get(HeaderXAMZContentSHA256)),
			zap.String(HeaderXAMZDate, rq.Header.Get(HeaderXAMZDate)))

		handler.ServeHTTP(w, rq)
	})
}
