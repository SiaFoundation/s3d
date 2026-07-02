package auth_test

import (
	"net/url"
	"testing"

	"github.com/SiaFoundation/s3d/s3/auth"
)

func TestRedactAuthorization(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "signature redacted, rest retained",
			in:   "AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39",
			want: "AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=REDACTED",
		},
		{
			name: "no space after algorithm still redacts signature",
			in:   "AWS4-HMAC-SHA256Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39",
			want: "AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=REDACTED",
		},
		{name: "empty", in: "", want: ""},
		{name: "no parameters", in: "AWS4-HMAC-SHA256", want: "AWS4-HMAC-SHA256"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := auth.RedactAuthorization(tc.in); got != tc.want {
				t.Errorf("RedactAuthorization(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string // raw URL, or "" for a nil *url.URL
		want string
	}{
		{name: "nil", in: "", want: ""},
		{
			// signature and security token are redacted; the credential is an
			// identifier rather than a secret and is retained. Encode sorts params.
			name: "signing params redacted",
			in:   "https://example.com/bucket/key?X-Amz-Credential=AKIA%2F20251017&X-Amz-Signature=abc123def456&X-Amz-Security-Token=FQoGsecret",
			want: "https://example.com/bucket/key?X-Amz-Credential=AKIA%2F20251017&X-Amz-Security-Token=REDACTED&X-Amz-Signature=REDACTED",
		},
		{
			name: "no secret params returned verbatim",
			in:   "https://example.com/bucket/key?partNumber=1&versionId=abc",
			want: "https://example.com/bucket/key?partNumber=1&versionId=abc",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var u *url.URL
			if tc.in != "" {
				var err error
				if u, err = url.Parse(tc.in); err != nil {
					t.Fatal(err)
				}
			}
			if got := auth.RedactURL(u); got != tc.want {
				t.Errorf("RedactURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
