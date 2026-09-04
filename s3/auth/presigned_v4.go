package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// maxPresignedExpiry is the maximum lifetime of a presigned URL.
const maxPresignedExpiry = 7 * 24 * time.Hour

// parsedPresignedAuth represents the structured form of the presigned
// authentication query parameters.
type parsedPresignedAuth struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
	Date          time.Time
	Expires       time.Duration
}

// parsePresignedAuth parses and validates the presigned authentication query
// parameters.
func parsePresignedAuth(query url.Values) (*parsedPresignedAuth, error) {
	required := []string{
		QueryXAMZAlgorithm,
		QueryXAMZCredential,
		QueryXAMZDate,
		QueryXAMZExpires,
		QueryXAMZSignedHeaders,
		QueryXAMZSignature,
	}
	for _, key := range required {
		values, ok := query[key]
		if !ok || len(values) != 1 || values[0] == "" {
			return nil, s3errs.ErrAuthorizationQueryParametersError
		}
	}
	switch query.Get(QueryXAMZAlgorithm) {
	case AuthorizationAWS4HMACSHA256:
	case AuthorizationAWS4ECDSAP256SHA256:
		return nil, s3errs.ErrNotImplemented // Signature Version 4A is not implemented
	default:
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}

	credential, ok := parseCredential(query.Get(QueryXAMZCredential))
	if !ok {
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}

	date, err := time.Parse(layoutISO8601, query.Get(QueryXAMZDate))
	if err != nil || !sameDay(date, credential.Scope.Date) {
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}
	// only the upper bound is a parameter error. A non-positive expiry is in
	// range, it just leaves an empty validity window, which S3 reports as an
	// expired request. max() keeps the conversion to a Duration from
	// overflowing, which would otherwise turn a large negative expiry into a
	// positive one and defeat maxPresignedExpiry.
	expiresSeconds, err := strconv.ParseInt(query.Get(QueryXAMZExpires), 10, 64)
	if err != nil || expiresSeconds > int64(maxPresignedExpiry/time.Second) {
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}
	expires := time.Duration(max(expiresSeconds, 0)) * time.Second

	signedHeaders := parseSignedHeaders(query.Get(QueryXAMZSignedHeaders))
	if slices.Contains(signedHeaders, "") {
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}

	signature := query.Get(QueryXAMZSignature)
	if decoded, err := hex.DecodeString(signature); err != nil || len(decoded) != sha256.Size {
		return nil, s3errs.ErrAuthorizationQueryParametersError
	}

	return &parsedPresignedAuth{
		Credential:    credential,
		SignedHeaders: signedHeaders,
		Signature:     signature,
		Date:          date,
		Expires:       expires,
	}, nil
}

// handleAuthV4Presigned handles AWS Signature Version 4 authentication using
// query parameters, i.e. a presigned URL.
func handleAuthV4Presigned(req *http.Request, query url.Values, store KeyStore, region string, now time.Time) (*string, error) {
	params, err := parsePresignedAuth(query)
	if err != nil {
		return nil, err
	}

	if params.Date.After(now.Add(maxClockSkew)) {
		return nil, s3errs.ErrRequestTimeTooSkewed
	} else if params.Expires <= 0 || now.After(params.Date.Add(params.Expires)) {
		// an empty validity window can't contain now
		return nil, s3errs.ErrAccessDeniedExpired
	}

	// X-Amz-Signature is the only parameter excluded from the canonical query
	query.Del(QueryXAMZSignature)

	payloadHash := presignedPayloadHash(req, query, params.SignedHeaders)
	sig := v4Signature{
		Credential:    params.Credential,
		Date:          params.Date,
		SignedHeaders: params.SignedHeaders,
		PayloadHash:   payloadHash,
		Query:         query,
		Signature:     params.Signature,
	}
	result, err := sig.verify(req, store, region, s3errs.ErrAuthorizationQueryParametersError)
	if err != nil {
		return nil, err
	}

	// the rest of the pipeline, e.g. Sha256HashFromRequest, reads the payload
	// hash from the header so make sure it sees the signed value
	req.Header.Set(HeaderXAMZContentSHA256, payloadHash)

	if err := handleAuthV4Payload(req, payloadHash, result); err != nil {
		return nil, err
	}
	return &result.AccessKeyID, nil
}

// presignedPayloadHash returns the payload hash a presigned request was signed
// with: the X-Amz-Content-Sha256 query parameter, else the header of the same
// name if it is signed, else UNSIGNED-PAYLOAD.
func presignedPayloadHash(req *http.Request, query url.Values, signedHeaders []string) string {
	if hash := query.Get(QueryXAMZContentSHA256); hash != "" {
		return hash
	} else if hash := req.Header.Get(HeaderXAMZContentSHA256); hash != "" &&
		slices.Contains(signedHeaders, strings.ToLower(HeaderXAMZContentSHA256)) {
		return hash
	}
	return ContentUnsignedPayload
}
