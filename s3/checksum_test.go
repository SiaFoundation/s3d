package s3

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestChecksumHashers(t *testing.T) {
	// pin every algorithm against a value computed elsewhere, because a hasher
	// on the wrong polynomial would reject correct uploads rather than fail
	// visibly
	const body = "hello growth verification\n"

	tests := map[string]string{
		// produced by the AWS CLI itself against a live s3d, so these check the
		// polynomial against AWS and not against another copy of the same
		// assumption. Neither is in python's stdlib
		"Crc64nvme": "7TQAehZyeq8=",
		"Crc32c":    "RJH9UA==",

		// the rest from python's zlib and hashlib
		"Crc32":  "/7DfVA==",
		"Sha1":   "1y2Y/xz0bv/90p+udC6eiQv0T9o=",
		"Sha256": "eTzjBj+IGti9vVCUPoKKS3FRE3Icm86NkTYwAemfvK4=",
	}

	for algorithm, want := range tests {
		t.Run(algorithm, func(t *testing.T) {
			h := RequestChecksum{Algorithm: algorithm}.NewHash()
			if h == nil {
				t.Fatal("no hasher")
			} else if _, err := h.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}

			if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); got != want {
				t.Errorf("expected %s, got %s", want, got)
			}
		})
	}

	if (RequestChecksum{Algorithm: "MadeUp"}).NewHash() != nil {
		t.Error("unexpected hasher for an unknown algorithm")
	}
}

func TestRequestChecksum(t *testing.T) {
	rawSum := []byte{1, 2, 3, 4}
	sum := base64.StdEncoding.EncodeToString(rawSum)
	rawSum64 := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	sum64 := base64.StdEncoding.EncodeToString(rawSum64)

	tests := []struct {
		name    string
		headers map[string]string
		want    string // "" expects no checksum
		wantSum []byte // defaults to rawSum
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
			headers: map[string]string{"x-amz-checksum-crc32": sum},
			want:    "Crc32",
		},
		{
			// what current AWS SDKs send by default
			name:    "crc64nvme",
			headers: map[string]string{"X-Amz-Checksum-Crc64nvme": sum64},
			want:    "Crc64nvme",
			wantSum: rawSum64,
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
			wantErr: s3errs.ErrInvalidRequest,
		},
		{
			// four bytes is a crc32, not a sha256
			name:    "sum has the wrong length",
			headers: map[string]string{"X-Amz-Checksum-Sha256": sum},
			wantErr: s3errs.ErrInvalidRequest,
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

			wantSum := tt.wantSum
			if wantSum == nil {
				wantSum = rawSum
			}
			if got.Algorithm != tt.want {
				t.Fatal("algorithm mismatch", got.Algorithm)
			} else if !bytes.Equal(got.Sum, wantSum) {
				t.Fatalf("sum mismatch %x", got.Sum)
			}
		})
	}
}
