package auth

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

const (
	layoutISO8601 = "20060102T150405Z"
	yyyymmdd      = "20060102"
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
	AccessKeyID string
	Scope       signScope
}

// signScope represents the scope part of the Credential string from
// authorization header.
type signScope struct {
	Date    time.Time
	Region  string
	Service string
	Request string
}

func (s signScope) Canonical() string {
	return strings.Join([]string{
		s.Date.Format(yyyymmdd),
		s.Region,
		s.Service,
		s.Request,
	}, "/")
}

// parses the Authorization header and extracts its components.
//
// A typical Authorization header looks like this:
// Authorization: AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39
func parseAuthHeader(header http.Header) (*parsedAuthHeader, error) {
	authHeader := strings.ReplaceAll(header.Get(HeaderAuthorization), " ", "")
	authHeader = strings.TrimPrefix(authHeader, AuthorizationAWS4HMACSHA256)
	values := strings.Split(authHeader, ",")
	if len(values) != 3 {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	// check if header is well-formed
	switch {
	case !strings.HasPrefix(values[0], "Credential="),
		!strings.HasPrefix(values[1], "SignedHeaders="),
		!strings.HasPrefix(values[2], "Signature="):
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	case len(values[2]) != 74: // 64 hex chars and "Signature="
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	// extract components
	credential := strings.TrimPrefix(values[0], "Credential=")
	signedHeaders := strings.TrimPrefix(values[1], "SignedHeaders=")
	signature := strings.TrimPrefix(values[2], "Signature=")

	credentialParts := strings.Split(credential, "/")
	if len(credentialParts) != 5 {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}

	accessKeyID := credentialParts[0]
	date, err := time.Parse(yyyymmdd, credentialParts[1])
	if err != nil {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}
	region := credentialParts[2]
	service := credentialParts[3]
	request := credentialParts[4]

	return &parsedAuthHeader{
		Credential: credentialHeader{
			AccessKeyID: accessKeyID,
			Scope: signScope{
				Date:    date,
				Region:  region,
				Service: service,
				Request: request,
			},
		},
		SignedHeaders: strings.Split(signedHeaders, ";"),
		Signature:     signature,
	}, nil
}

func parseDateHeader(header http.Header) (time.Time, error) {
	var date string
	if date = header.Get(HeaderXAMZDate); date == "" {
		if date = header.Get(HeaderDate); date == "" {
			return time.Time{}, s3errs.ErrMissingAuthenticationToken
		}
	}

	// Parse date header (ISO8601)
	t, e := time.Parse(layoutISO8601, date)
	if e != nil {
		return time.Time{}, s3errs.ErrAuthorizationHeaderMalformed
	}
	return t, nil
}

// extractSignedHeaders extracts the signed headers specified in the
// Authorization header from the http.Request headers.
func extractSignedHeaders(req *http.Request, signedHeaders []string) (http.Header, error) {
	reqHeaders := req.Header
	reqQueries := req.Form
	// find whether "host" is part of list of signed headers.
	// if not return ErrUnsignedHeaders. "host" is mandatory.
	if !slices.Contains(signedHeaders, "host") {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		// `host` will not be found in the headers, can be found in r.Host. but
		// its always necessary that the list of signed headers containing host
		// in it.
		val, ok := reqHeaders[http.CanonicalHeaderKey(header)]
		if !ok {
			// try to set headers from Query String
			val, ok = reqQueries[header]
		}
		if ok {
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = val
			continue
		}
		switch header {
		case "expect":
			// Golang http server strips off 'Expect' header, if the
			// client sent this as part of signed headers we need to
			// handle otherwise we would see a signature mismatch.
			// `aws-cli` sets this as part of signed headers.
			//
			// According to
			// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.20
			// Expect header is always of form:
			//
			//   Expect       =  "Expect" ":" 1#expectation
			//   expectation  =  "100-continue" | expectation-extension
			//
			// So it safe to assume that '100-continue' is what would
			// be sent, for the time being keep this work around.
			// Adding a *TODO* to remove this later when Golang server
			// doesn't filter out the 'Expect' header.
			extractedSignedHeaders.Set(header, "100-continue")
		case "host":
			// Go http server removes "host" from Request.Header
			extractedSignedHeaders.Set(header, req.Host)
		case "transfer-encoding":
			// Go http server removes "host" from Request.Header
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = req.TransferEncoding
		case "content-length":
			// Signature-V4 spec excludes Content-Length from signed headers list for signature calculation.
			// But some clients deviate from this rule. Hence we consider Content-Length for signature
			// calculation to be compatible with such clients.
			extractedSignedHeaders.Set(header, strconv.FormatInt(req.ContentLength, 10))
		default:
			return nil, s3errs.ErrAuthorizationHeaderMalformed
		}
	}
	return extractedSignedHeaders, nil
}
