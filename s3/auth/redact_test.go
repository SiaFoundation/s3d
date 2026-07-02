package auth_test

import (
	"net/url"
	"testing"

	"github.com/SiaFoundation/s3d/s3/auth"
)

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
