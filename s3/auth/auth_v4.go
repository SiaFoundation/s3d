package auth

import (
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// parsedAuthHeader represents the structured form of an v4 Authorization
// header.
type parsedAuthHeader struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
}

// credentialHeader data type represents structured form of Credential
// string from authorization header.
type credentialHeader struct {
	accessKey string
	scope     signScope
}

// signScope represents the scope part of the Credential string from
// authorization header.
type signScope struct {
	date    time.Time
	region  string
	service string
	request string
}

// parses the Authorization header and extracts its components.
//
// A typical Authorization header looks like this:
// Authorization: AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39
func parseAuthHeader(header http.Header) (*parsedAuthHeader, error) {
	values := header.Values(HeaderAuthorization)
	if len(values) != 4 {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	// check if header is well-formed
	switch {
	case !strings.HasPrefix(values[1], "Credential="),
		!strings.HasPrefix(values[2], "SignedHeaders="),
		!strings.HasPrefix(values[3], "Signature="):
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	case len(values[3]) != 74: // 64 hex chars and "Signature="
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	// extract components
	credential := strings.TrimPrefix(values[1], "Credential=")
	signedHeaders := strings.TrimPrefix(values[2], "SignedHeaders=")
	signature := strings.TrimPrefix(values[3], "Signature=")

	credentialParts := strings.Split(credential, "/")
	if len(credentialParts) != 5 {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	accessKeyID := credentialParts[0]
	date, err := time.Parse("20060102", credentialParts[1])
	if err != nil {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}
	region := credentialParts[2]
	service := credentialParts[3]
	request := credentialParts[4]

	return &parsedAuthHeader{
		Credential: credentialHeader{
			accessKey: accessKeyID,
			scope: signScope{
				date:    date,
				region:  region,
				service: service,
				request: request,
			},
		},
		SignedHeaders: strings.Split(signedHeaders, ";"),
		Signature:     signature,
	}, nil
}

func verifyV4SimpleSignature(req *http.Request) error {
	payloadHash := req.Header.Get(HeaderXAMZContentSHA256)
	if payloadHash == "" {
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string
	}
	return s3errs.ErrNotImplemented
}
