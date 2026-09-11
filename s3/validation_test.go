package s3

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestValidateBucketName(t *testing.T) {
	type tcase struct {
		name string
		err  *s3errs.Error
	}

	baseCases := []tcase{
		{"", &s3errs.ErrInvalidBucketName},

		// This is not in nameCases because appending labels to it will cause an error:
		{strings.Repeat("1", 63), nil},

		// Appending labels to these causes them to pass:
		{"192.168.1.1", &s3errs.ErrInvalidBucketName},     // IP addresses are not allowed as bucket names. These may trip the "3-char min" rule first.
		{"192.168.111.111", &s3errs.ErrInvalidBucketName}, // These should not trip the 3-char min but should still fail.
	}

	nameCases := []tcase{
		{"yep", nil},
		{"0yep", nil},
		{"yep0", nil},
		{"y-p", nil},
		{"y--p", nil},

		{"NUP", &s3errs.ErrInvalidBucketName},
		{"n🤡p", &s3errs.ErrInvalidBucketName}, // UTF-8 is effectively invalid because the high bytes fall outside the legal range
		{"-nup", &s3errs.ErrInvalidBucketName},
		{"nup-", &s3errs.ErrInvalidBucketName},
		{"-nup-", &s3errs.ErrInvalidBucketName},

		{"1", &s3errs.ErrInvalidBucketName},  // Too short
		{"12", &s3errs.ErrInvalidBucketName}, // Too short
		{"123", nil},
		{strings.Repeat("1", 64), &s3errs.ErrInvalidBucketName},
	}

	// All the same rules that apply to names apply to "labels" (the "."-separated
	// portions of a bucket name, like DNS):
	var labelCases []tcase
	for _, tc := range nameCases {
		labelCases = append(labelCases, []tcase{
			{name: fmt.Sprintf("%s.label", tc.name), err: tc.err},
			{name: fmt.Sprintf("label.%s", tc.name), err: tc.err},
			{name: fmt.Sprintf("label.%s.label", tc.name), err: tc.err},
		}...)
	}

	var cases []tcase
	cases = append(cases, baseCases...)
	cases = append(cases, nameCases...)
	cases = append(cases, labelCases...)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBucketName(tc.name)
			if tc.err != nil && !errors.Is(err, *tc.err) {
				t.Fatalf("name %q: expected error %v, got %v", tc.name, tc.err, err)
			} else if tc.err == nil && err != nil {
				t.Fatalf("name %q: expected no error, got %v", tc.name, err)
			}
		})
	}
}

// TestMetadataHeadersKeepsOnlyObjectMetadata guards the allowlist. Request
// plumbing must never reach stored metadata, because everything stored is
// echoed back on every read, and for the encryption headers that would mean
// handing a client's own key to whoever reads the object next.
func TestMetadataHeadersKeepsOnlyObjectMetadata(t *testing.T) {
	kept := map[string]string{
		"X-Amz-Meta-Colour":    "blue",
		"X-Amz-Checksum-Crc32": "NhCmhg==",
		"Content-Type":         "text/plain",
		"Content-Disposition":  "inline",
		"Content-Encoding":     "gzip",
		"Cache-Control":        "no-cache",
		"Expires":              "Thu, 01 Jan 2026 00:00:00 GMT",
	}
	dropped := []string{
		// request signing
		"X-Amz-Content-Sha256",
		"X-Amz-Date",
		"X-Amz-Decoded-Content-Length",
		"X-Amz-Sdk-Checksum-Algorithm",
		"X-Amz-Trailer",
		// request routing
		"X-Amz-Copy-Source",
		"X-Amz-Metadata-Directive",
		// encryption, including a name no constant declares
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Key",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
		"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key",
		"X-Amz-Server-Side-Encryption-Not-Yet-Invented",
	}

	h := http.Header{}
	for k, v := range kept {
		h.Set(k, v)
	}
	for _, k := range dropped {
		h.Set(k, "value")
	}

	meta, err := metadataHeaders(h, MetadataSizeLimit)
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range kept {
		if meta[k] != want {
			t.Fatal("mismatch", k, meta[k])
		}
	}
	for _, k := range dropped {
		if got, ok := meta[k]; ok {
			t.Fatal("stored", k, got)
		}
	}
	if len(meta) != len(kept) {
		t.Fatal("unexpected", len(meta))
	}
}
