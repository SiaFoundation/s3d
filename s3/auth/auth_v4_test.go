package auth

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestParseAuthHeader(t *testing.T) {
	// Example AWSv4 Authorization header
	// "Authorization: AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39"
	header := make(http.Header)
	header.Add(HeaderAuthorization, AuthorizationAWS4HMACSHA256)
	header.Add(HeaderAuthorization, "Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request")
	header.Add(HeaderAuthorization, "SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date")
	header.Add(HeaderAuthorization, "Signature=f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100")

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
