package s3

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
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
