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
			headers: map[string]string{"X-Amz-Checksum-Crc32": sum, "X-Amz-Checksum-Crc32c": sum},
			wantErr: s3errs.ErrInvalidRequest,
		},
		{
			name:    "more than one checksum, one of them malformed",
			headers: map[string]string{"X-Amz-Checksum-Crc32": sum, "X-Amz-Checksum-Sha256": "not base64!"},
			wantErr: s3errs.ErrInvalidRequest,
		},
		{
			name:    "sum is not base64",
			headers: map[string]string{"X-Amz-Checksum-Crc32": "not base64!"},
			wantErr: s3errs.ErrBadDigest,
		},
		{
			// four bytes is a crc32, not a sha256
			name:    "sum has the wrong length",
			headers: map[string]string{"X-Amz-Checksum-Sha256": sum},
			wantErr: s3errs.ErrBadDigest,
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

func TestTrailingChecksum(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    string // "" expects no checksum
	}{
		{
			name:    "no trailer",
			headers: map[string]string{"Content-Type": "text/plain"},
		},
		{
			name:    "crc32 trailer",
			headers: map[string]string{"X-Amz-Trailer": "x-amz-checksum-crc32"},
			want:    "Crc32",
		},
		{
			name:    "trailer list with padding",
			headers: map[string]string{"X-Amz-Trailer": "x-amz-foo, x-amz-checksum-sha256"},
			want:    "Sha256",
		},
		{
			name:    "unknown algorithm",
			headers: map[string]string{"X-Amz-Trailer": "x-amz-checksum-made-up"},
		},
		{
			name:    "trailer is not a checksum",
			headers: map[string]string{"X-Amz-Trailer": "x-amz-something"},
		},
		{
			// the algorithm header alone promises a checksum without carrying one
			name:    "sdk algorithm header only",
			headers: map[string]string{"X-Amz-Sdk-Checksum-Algorithm": "CRC32"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := make(http.Header)
			for k, v := range tt.headers {
				h.Set(k, v)
			}
			got := trailingChecksum(h)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("unexpected checksum: %+v", got)
				}
				return
			} else if got == nil {
				t.Fatalf("expected %s, got no checksum", tt.want)
			}

			if got.Algorithm != tt.want {
				t.Fatal("algorithm mismatch", got.Algorithm)
			} else if got.Sum != nil {
				t.Fatalf("unexpected sum %x", got.Sum)
			}
		})
	}
}

func TestParseChecksumAlgorithm(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ChecksumAlgorithm
		wantErr error
	}{
		{name: "lowercase", input: "crc32", want: ChecksumCRC32},
		{name: "uppercase", input: "CRC32C", want: ChecksumCRC32C},
		{name: "mixed case", input: "Crc64nvme", want: ChecksumCRC64NVME},
		{name: "sha1", input: "sha1", want: ChecksumSHA1},
		{name: "sha256", input: "SHA256", want: ChecksumSHA256},
		{name: "empty"},
		{name: "unknown algorithm", input: "XXHASH64", wantErr: s3errs.ErrInvalidRequest},
		{name: "md5 is not an additional checksum", input: "MD5", wantErr: s3errs.ErrInvalidRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseChecksumAlgorithm(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestParseChecksumType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ChecksumType
		wantErr error
	}{
		{name: "composite", input: "COMPOSITE", want: ChecksumTypeComposite},
		{name: "full object lowercase", input: "full_object", want: ChecksumTypeFullObject},
		{name: "empty"},
		{name: "unknown type", input: "WHOLE_OBJECT", wantErr: s3errs.ErrInvalidRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseChecksumType(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestChecksumAlgorithmHeader(t *testing.T) {
	if got := ChecksumCRC64NVME.Header(); got != "X-Amz-Checksum-Crc64nvme" {
		t.Fatalf("unexpected header %q", got)
	}
	if got := ChecksumSHA256.HeaderSuffix(); got != "Sha256" {
		t.Fatalf("unexpected suffix %q", got)
	}

	if got, ok := ChecksumAlgorithmFromSuffix("Crc32c"); !ok || got != ChecksumCRC32C {
		t.Fatalf("expected %q, got %q %v", ChecksumCRC32C, got, ok)
	}
	if _, ok := ChecksumAlgorithmFromSuffix("Md5"); ok {
		t.Fatal("expected Md5 to be rejected")
	}
}

func TestChecksumTypeSupport(t *testing.T) {
	tests := []struct {
		algorithm   ChecksumAlgorithm
		composite   bool
		fullObject  bool
		defaultType ChecksumType
	}{
		{ChecksumCRC32, true, true, ChecksumTypeComposite},
		{ChecksumCRC64NVME, false, true, ChecksumTypeFullObject},
		{ChecksumSHA1, true, false, ChecksumTypeComposite},
		{ChecksumSHA256, true, false, ChecksumTypeComposite},
	}

	for _, tt := range tests {
		t.Run(string(tt.algorithm), func(t *testing.T) {
			if got := tt.algorithm.SupportsType(ChecksumTypeComposite); got != tt.composite {
				t.Fatalf("composite: expected %v, got %v", tt.composite, got)
			}
			if got := tt.algorithm.SupportsType(ChecksumTypeFullObject); got != tt.fullObject {
				t.Fatalf("full object: expected %v, got %v", tt.fullObject, got)
			}
			if got := tt.algorithm.DefaultType(); got != tt.defaultType {
				t.Fatalf("default: expected %q, got %q", tt.defaultType, got)
			}
		})
	}
}

func TestObjectChecksumMeta(t *testing.T) {
	want := ObjectChecksum{
		Algorithm: ChecksumSHA256,
		Type:      ChecksumTypeComposite,
		Value:     "abc=-3",
	}

	meta := make(map[string]string)
	want.StoreInMeta(meta)

	got := ObjectChecksumFromMeta(meta)
	if got == nil {
		t.Fatal("expected a checksum")
	} else if *got != want {
		t.Fatalf("expected %+v, got %+v", want, *got)
	}

	if got := ObjectChecksumFromMeta(map[string]string{}); got != nil {
		t.Fatalf("expected no checksum, got %+v", *got)
	}
	if got := ObjectChecksumFromMeta(map[string]string{HeaderXAMZChecksumType: "COMPOSITE"}); got != nil {
		t.Fatalf("expected no checksum, got %+v", *got)
	}

	empty := make(map[string]string)
	ObjectChecksum{Type: ChecksumTypeFullObject}.StoreInMeta(empty)
	if len(empty) != 0 {
		t.Fatalf("expected no metadata, got %v", empty)
	}
}
