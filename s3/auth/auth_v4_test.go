package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

type mockKeyStore struct {
}

func (s *mockKeyStore) LoadSecret(_ context.Context, _ string) (SecretAccessKey, error) {
	return nil, s3errs.ErrInvalidAccessKeyId
}

func TestParseAuthHeader(t *testing.T) {
	// Example AWSv4 Authorization header
	// "Authorization: AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39"
	header := make(http.Header)
	header.Set(HeaderAuthorization, "AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100")

	parsed, err := parseAuthHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	date, _ := time.Parse("20060102", "20251017")
	expected := parsedAuthHeader{
		Credential: credentialHeader{
			AccessKeyID: "AKIA7GQ3XN52WQLYDHZP",
			Scope: signScope{
				Date:    date,
				Region:  "us-east-1",
				Service: "s3",
				Request: "aws4_request",
			},
		},
		SignedHeaders: []string{
			"accept-encoding",
			"amz-sdk-invocation-id",
			"amz-sdk-request",
			"content-length",
			"content-type",
			"host",
			"x-amz-content-sha256",
			"x-amz-date",
		},
		Signature: "f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100",
	}

	if !reflect.DeepEqual(*parsed, expected) {
		t.Fatalf("parsed auth header does not match expected\nexpected: %+v\nparsed: %+v", expected, *parsed)
	}
}

func TestDateValidation(t *testing.T) {
	header := make(http.Header)
	now := time.Now().UTC()
	header.Set(HeaderAuthorization, fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/%s/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100", now.Format(yyyymmdd)))
	req := &http.Request{Header: header}
	store := &mockKeyStore{}

	// Case 1: date not set
	_, err := verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrMissingAuthenticationToken) {
		t.Fatalf("expected ErrMissingAuthenticationToken, got %v", err)
	}

	// Case 2: credential date is in the past
	header.Set(HeaderXAMZDate, now.Add(-24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 3: credential date is in the future
	header.Set(HeaderXAMZDate, now.Add(24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 4: date is skewed too far in the past
	header.Set(HeaderXAMZDate, now.Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now.Add(6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 5: date is skewed too far in the future
	_, err = verifyV4SignedRequest(req, store, "", now.Add(-6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 6: date is valid but we don't have the access key
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrInvalidAccessKeyId) {
		t.Fatal(err)
	}
}
