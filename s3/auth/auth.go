package auth

import (
	"encoding/hex"
	"net/http"
	"strings"
	"time"

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

	// HeaderXAMZDecodedContentLength contains the decoded content length of an
	// aws-chunked encoded request.
	HeaderXAMZDecodedContentLength = "X-Amz-Decoded-Content-Length"

	// HeaderXAMZTrailer contains the expected headers of the payload trailer
	HeaderXAMZTrailer = "X-Amz-Trailer"

	// HeaderDate is the standard HTTP "Date" header. It is used if
	// HeaderXAMZDate is not present.
	HeaderDate = "Date"
)

// The following constants define the supported checksum header names.
const (
	xAmzChecksumCrc32  = "X-Amz-Checksum-Crc32"
	xAmzChecksumCrc32C = "X-Amz-Checksum-Crc32C"
	xAmzChecksumSha1   = "X-Amz-Checksum-Sha1"
	xAmzChecksumSha256 = "X-Amz-Checksum-Sha256"
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

// AuthenticatedHandlerFunc is an adapter to allow the use of ordinary functions
// as authenticated handlers. If f is a function with the appropriate signature,
// authenticatedHandlerFunc(f) is an authenticated handler that calls f.
type AuthenticatedHandlerFunc func(http.ResponseWriter, *http.Request, *string)

// ServeHTTP calls f(w, r, accessKeyID).
func (f AuthenticatedHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request, accessKeyID *string) {
	f(w, r, accessKeyID)
}

// HandleAuth inspects the request to determine the authentication type, verfies
// the signature and returns the used access key ID.
//
// - 'now' refers to the current time and is used to verify request timestamps
// - 'region' is the AWS region the request is targeted to. If the region is an
// empty string, every region is allowed. Otherwise, authentication fails if the
// region doesn't match the provided one.
func HandleAuth(req *http.Request, store KeyStore, region string, now time.Time) (string, error) {
	authHeader := req.Header.Get(HeaderAuthorization)
	if strings.HasPrefix(authHeader, AuthorizationAWS4HMACSHA256) {
		return handleAuthV4(req, store, region, now)
	} else if strings.HasPrefix(authHeader, AuthorizationAWS4ECDSAP256SHA256) {
		return handleAuthV4a(req)
	} else {
		// NOTE: S3 does something interesting here. You'd expect AccessDenied,
		// but for some reason it returns an ErrInvalidDigest with 403. Even
		// though ErrInvalidDigest is usually returned with a 400.
		err := s3errs.ErrInvalidDigest
		err.HTTPStatus = http.StatusForbidden
		return "", err
	}
}

// handleAuthV4 handles AWS Signature Version 4 authentication using HMAC.
func handleAuthV4(req *http.Request, store KeyStore, region string, now time.Time) (string, error) {
	// verify the signed request first
	accessKeyID, err := verifyV4SignedRequest(req, store, region, now)
	if err != nil {
		return "", err
	}

	// at this point the signature is valid, but we still need to check if the
	// signed payload matches
	switch req.Header.Get(HeaderXAMZContentSHA256) {
	// case1: payload is not signed at all
	case ContentUnsignedPayload:
		return accessKeyID, nil
	// case2-4: payload is streamed and possibly signed or has a trailer with
	// additional headers
	case ContentStreamingUnsignedPayloadTrailer,
		ContentStreamingAWS4HMACSHA256Payload,
		ContentStreamingAWS4HMACSHA256PayloadTrailer:
		return accessKeyID, handleAuthV4Streaming(req)
	// case5: the x-amz-content-sha256 header contains the actual payload hash
	default:
		return accessKeyID, nil
	}
}

// handleAuthV4a handles AWS Signature Version 4A authentication using ECDSA.
func handleAuthV4a(_ *http.Request) (string, error) {
	return "", s3errs.ErrNotImplemented // Signature Version 4A is not implemented
}

// Sha256HashFromRequest extracts the SHA256 hash of the payload from the
// request if available. This hash should then be used to verify the integrity
// of the payload.
func Sha256HashFromRequest(req *http.Request) (*[32]byte, error) {
	h := req.Header.Get(HeaderXAMZContentSHA256)
	switch h {
	case ContentUnsignedPayload,
		ContentStreamingUnsignedPayloadTrailer,
		ContentStreamingAWS4HMACSHA256Payload,
		ContentStreamingAWS4HMACSHA256PayloadTrailer:
		return nil, nil
	default:
	}
	hash, err := hex.DecodeString(h)
	if err != nil {
		return nil, s3errs.ErrInvalidDigest
	} else if len(hash) != 32 {
		return nil, s3errs.ErrInvalidDigest
	}
	return (*[32]byte)(hash), nil
}
