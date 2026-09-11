package s3

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestRequestChecksum(t *testing.T) {
	rawSum := []byte{1, 2, 3, 4}
	sum := base64.StdEncoding.EncodeToString(rawSum)

	tests := []struct {
		name    string
		headers map[string]string
		want    string // "" expects no checksum
		wantErr error
	}{
		{
			name:    "no checksum header",
			headers: map[string]string{"Content-Type": "text/plain"},
		},
		{
			name:    "crc32",
			headers: map[string]string{"X-Amz-Checksum-Crc32": sum},
			want:    "Crc32",
		},
		{
			name:    "lowercase header name",
			headers: map[string]string{"x-amz-checksum-sha256": sum},
			want:    "Sha256",
		},
		{
			// what current AWS SDKs send by default
			name:    "crc64nvme",
			headers: map[string]string{"X-Amz-Checksum-Crc64nvme": sum},
			want:    "Crc64nvme",
		},
		{
			// ignored rather than refused, so a client asking for an algorithm
			// the backend cannot compute still works
			name:    "unknown algorithm",
			headers: map[string]string{"X-Amz-Checksum-Made-Up": sum},
		},
		{
			name:    "more than one checksum",
			headers: map[string]string{"X-Amz-Checksum-Crc32": sum, "X-Amz-Checksum-Sha1": sum},
			wantErr: s3errs.ErrInvalidRequest,
		},
		{
			name:    "sum is not base64",
			headers: map[string]string{"X-Amz-Checksum-Crc32": "not base64!"},
			wantErr: s3errs.ErrInvalidDigest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := make(http.Header)
			for k, v := range tt.headers {
				h.Set(k, v)
			}

			got, err := requestChecksum(h)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}

			if tt.want == "" {
				if got != nil {
					t.Fatalf("unexpected checksum: %+v", got)
				}
				return
			} else if got == nil {
				t.Fatalf("expected %s, got no checksum", tt.want)
			}

			if got.Algorithm != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got.Algorithm)
			}
			if !bytes.Equal(got.Sum, rawSum) {
				t.Errorf("sum mismatch: expected %x, got %x", rawSum, got.Sum)
			}
		})
	}
}
