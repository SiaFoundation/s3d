package auth

import (
	"net/http"
	"strings"

	"github.com/SiaFoundation/s3d/s3/s3errs"
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

// The following constants define the supported "Authorization" header values
const (
	AuthorizationAWS4HMACSHA256 = "AWS4-HMAC-SHA256" // SigV4

	AuthorizationAWS4ECDSAP256SHA256 = "AWS4-ECDSA-P256-SHA256" // SigV4A
)

// The following constants define the potential values for the
// "X-Amz-Content-Sha256" header. If the header does not contain one of these
// sentinel values, the value is to be interpreted as the actual checksum of the
// payload.
const (
	// Unsigned
	ContentUnsignedPayload                 = "UNSIGNED-PAYLOAD"
	ContentStreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

	// v4 signed
	ContentStreamingAWS4HMACSHA256Payload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	ContentStreamingAWS4HMACSHA256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	// v4a signed
	ContentStreamingAWS4ECDSAP256SHA256Payload        = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
	ContentStreamingAWS4ECDSAP256SHA256PayloadTrailer = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
)

// AuthenticatedHandler is like http.Handler but includes the access key ID of
// the authenticated user.
type AuthenticatedHandler interface {
	ServeHTTP(w http.ResponseWriter, req *http.Request, accessKeyID *string)
}

// authenticatedHandlerFunc is an adapter to allow the use of ordinary functions
// as authenticated handlers. If f is a function with the appropriate signature,
// authenticatedHandlerFunc(f) is an authenticated handler that calls f.
type AuthenticatedHandlerFunc func(http.ResponseWriter, *http.Request, *string)

// ServeHTTP calls f(w, r, accessKeyID).
func (f AuthenticatedHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request, accessKeyID *string) {
	f(w, r, accessKeyID)
}

func HandleAuth(req *http.Request) error {
	authHeader := req.Header.Get(HeaderAuthorization)
	if strings.HasPrefix(authHeader, AuthorizationAWS4HMACSHA256) {
		return handleAuthV4(req)
	} else if strings.HasPrefix(authHeader, AuthorizationAWS4ECDSAP256SHA256) {
		return handleAuthV4a(req)
	} else {
		return s3errs.ErrUnsupportedSignature
	}
}

// handleAuthV4 handles AWS Signature Version 4 authentication using HMAC.
func handleAuthV4(req *http.Request) error {
	switch req.Header.Get(HeaderXAMZContentSHA256) {
	case ContentUnsignedPayload:
		return s3errs.ErrNotImplemented
	case ContentStreamingUnsignedPayloadTrailer:
		return s3errs.ErrNotImplemented
	case ContentStreamingAWS4HMACSHA256Payload:
		return s3errs.ErrNotImplemented
	case ContentStreamingAWS4HMACSHA256PayloadTrailer:
		return s3errs.ErrNotImplemented
	default:
		verifyV4SimpleSignature(req)
	}
	return nil
}

// handleAuthV4a handles AWS Signature Version 4A authentication using ECDSA.
func handleAuthV4a(_ *http.Request) error {
	return s3errs.ErrNotImplemented // Signature Version 4A is not implemented
}
