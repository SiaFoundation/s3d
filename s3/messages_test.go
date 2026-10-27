package s3

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestStorageClassXML(t *testing.T) {
	// verify that an empty StorageClass defaults to STANDARD
	var sc StorageClass
	buf, err := xml.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(buf), "STANDARD") {
		t.Fatalf("expected STANDARD, got %q", string(buf))
	}

	// verify that Content always includes StorageClass in XML output,
	// this was a regression where omitempty caused the element to be
	// absent breaking clients like Arq Backup
	c := Content{
		Key:  "test.txt",
		Size: 1024,
	}
	buf, err = xml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(buf), "<StorageClass>STANDARD</StorageClass>") {
		t.Fatalf("expected <StorageClass>STANDARD</StorageClass> in XML output, got %q", string(buf))
	}
}

func TestContentTimeXML(t *testing.T) {
	// Strict S3 client parsers (e.g. jets3t used by older Cyberduck) expect
	// LastModified to always have exactly 3 fractional-second digits. Go's
	// .999 format suppresses trailing zeros, so a whole-second time would
	// previously render as "...43Z" and fail those parsers. Make sure the
	// format always pads to .SSSZ.
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{"whole second", time.Date(2026, 6, 1, 9, 1, 43, 0, time.UTC), "2026-06-01T09:01:43.000Z"},
		{"half second", time.Date(2026, 6, 1, 9, 1, 43, 500*1000*1000, time.UTC), "2026-06-01T09:01:43.500Z"},
		{"five ms", time.Date(2026, 6, 1, 9, 1, 43, 5*1000*1000, time.UTC), "2026-06-01T09:01:43.005Z"},
		{"sub-ms truncates", time.Date(2026, 6, 1, 9, 1, 43, 123456789, time.UTC), "2026-06-01T09:01:43.123Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := xml.Marshal(NewContentTime(tt.in))
			if err != nil {
				t.Fatal(err)
			}
			want := "<ContentTime>" + tt.want + "</ContentTime>"
			if string(buf) != want {
				t.Fatalf("expected %q, got %q", want, string(buf))
			}
		})
	}
}

func TestChecksumElementsValue(t *testing.T) {
	const sha256Value = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="

	tests := []struct {
		name          string
		elements      ChecksumElements
		wantAlgorithm ChecksumAlgorithm
		wantValue     string
		wantErr       error
	}{
		{
			name:          "single element",
			elements:      ChecksumElements{ChecksumSHA256: sha256Value},
			wantAlgorithm: ChecksumSHA256,
			wantValue:     sha256Value,
		},
		{
			name:          "composite suffix",
			elements:      ChecksumElements{ChecksumSHA256: sha256Value + "-3"},
			wantAlgorithm: ChecksumSHA256,
			wantValue:     sha256Value + "-3",
		},
		{
			name:          "crc32",
			elements:      ChecksumElements{ChecksumCRC32: "tzf7Gg=="},
			wantAlgorithm: ChecksumCRC32,
			wantValue:     "tzf7Gg==",
		},
		{
			name: "none set",
		},
		{
			name:     "two elements",
			elements: ChecksumElements{ChecksumCRC32: "tzf7Gg==", ChecksumSHA256: sha256Value},
			wantErr:  s3errs.ErrInvalidRequest,
		},
		{
			name:     "invalid base64",
			elements: ChecksumElements{ChecksumSHA256: "!!!!"},
			wantErr:  s3errs.ErrBadDigest,
		},
		{
			name:     "wrong length",
			elements: ChecksumElements{ChecksumSHA256: "tzf7Gg=="},
			wantErr:  s3errs.ErrBadDigest,
		},
		{
			name:     "composite suffix on a short digest",
			elements: ChecksumElements{ChecksumSHA256: "tzf7Gg==-3"},
			wantErr:  s3errs.ErrBadDigest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algorithm, value, err := tt.elements.Value()
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			if algorithm != tt.wantAlgorithm {
				t.Fatalf("expected algorithm %q, got %q", tt.wantAlgorithm, algorithm)
			} else if value != tt.wantValue {
				t.Fatalf("expected value %q, got %q", tt.wantValue, value)
			}
		})
	}
}
