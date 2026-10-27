package s3

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
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

func partChecksum(t *testing.T, a ChecksumAlgorithm, data []byte) PartChecksum {
	t.Helper()
	h := a.NewHash()
	if h == nil {
		t.Fatalf("no hash for %q", a)
	}
	h.Write(data)
	return PartChecksum{Sum: h.Sum(nil), Length: int64(len(data))}
}

// the three 5 MiB parts the s3-tests multipart checksum vectors upload
func repeatedParts(t *testing.T, a ChecksumAlgorithm) []PartChecksum {
	t.Helper()
	parts := make([]PartChecksum, 0, 3)
	for _, b := range []byte("ABC") {
		parts = append(parts, partChecksum(t, a, bytes.Repeat([]byte{b}, 5*1024*1024)))
	}
	return parts
}

func assertPartDigests(t *testing.T, parts []PartChecksum, want []string) {
	t.Helper()
	if len(parts) != len(want) {
		t.Fatalf("expected %d parts, got %d", len(want), len(parts))
	}
	for i, digest := range want {
		if got := base64.StdEncoding.EncodeToString(parts[i].Sum); got != digest {
			t.Fatalf("part %d: expected %q, got %q", i+1, digest, got)
		}
	}
}

func TestCompositeChecksum(t *testing.T) {
	single := partChecksum(t, ChecksumSHA256, bytes.Repeat([]byte{'A'}, 1024))
	assertPartDigests(t, []PartChecksum{single}, []string{"arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="})
	if got := CompositeChecksum(ChecksumSHA256, []PartChecksum{single}); got != "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1" {
		t.Fatalf("unexpected composite %q", got)
	}

	tests := []struct {
		algorithm ChecksumAlgorithm
		digests   []string
		want      string
	}{
		{
			algorithm: ChecksumSHA256,
			digests: []string{
				"275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
				"mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
				"Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
			},
			want: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
		},
		{
			algorithm: ChecksumSHA1,
			digests: []string{
				"iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
				"LS/TJ32bAVKEwRu+sE3X7awh/lk=",
				"6DDwovUaHwrKNXDMzOGbuvj9kxI=",
			},
			want: "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.algorithm), func(t *testing.T) {
			parts := repeatedParts(t, tt.algorithm)
			assertPartDigests(t, parts, tt.digests)
			if got := CompositeChecksum(tt.algorithm, parts); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestCombineChecksums(t *testing.T) {
	tests := []struct {
		algorithm ChecksumAlgorithm
		digests   []string
		want      string
	}{
		{
			algorithm: ChecksumCRC32,
			digests:   []string{"JRTCyQ==", "QoZTGg==", "YAgjqw=="},
			want:      "WgDhBQ==",
		},
		{
			algorithm: ChecksumCRC32C,
			digests:   []string{"MDaLrw==", "TH4EZg==", "Z7mBIQ=="},
			want:      "xU+Krw==",
		},
		{
			algorithm: ChecksumCRC64NVME,
			digests:   []string{"L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="},
			want:      "i+6LR0y3eFo=",
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.algorithm), func(t *testing.T) {
			parts := repeatedParts(t, tt.algorithm)
			assertPartDigests(t, parts, tt.digests)

			combined, err := CombineChecksums(tt.algorithm, parts)
			if err != nil {
				t.Fatal(err)
			}
			if got := base64.StdEncoding.EncodeToString(combined); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}

	sha1Parts := []PartChecksum{{Sum: make([]byte, 20), Length: 1}}
	if _, err := CombineChecksums(ChecksumSHA1, sha1Parts); !errors.Is(err, s3errs.ErrInvalidRequest) {
		t.Fatalf("expected %v, got %v", s3errs.ErrInvalidRequest, err)
	}
	if _, err := CombineChecksums(ChecksumCRC32, nil); !errors.Is(err, s3errs.ErrInvalidRequest) {
		t.Fatalf("expected %v, got %v", s3errs.ErrInvalidRequest, err)
	}
}

func TestCrcCombineRandom(t *testing.T) {
	algorithms := []ChecksumAlgorithm{ChecksumCRC32, ChecksumCRC32C, ChecksumCRC64NVME}

	for _, algorithm := range algorithms {
		t.Run(string(algorithm), func(t *testing.T) {
			for range 50 {
				data := frand.Bytes(frand.Intn(1 << 20))
				split := frand.Intn(len(data) + 1)

				parts := []PartChecksum{
					partChecksum(t, algorithm, data[:split]),
					partChecksum(t, algorithm, data[split:]),
				}
				combined, err := CombineChecksums(algorithm, parts)
				if err != nil {
					t.Fatal(err)
				}
				if want := partChecksum(t, algorithm, data).Sum; !bytes.Equal(combined, want) {
					t.Fatalf("%d bytes split at %d: expected %x, got %x", len(data), split, want, combined)
				}
			}
		})
	}
}
