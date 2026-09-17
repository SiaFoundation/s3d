package s3

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// TestMetadataHeaders checks that only the object's own headers are stored.
func TestMetadataHeaders(t *testing.T) {
	kept := map[string]string{
		"X-Amz-Meta-Colour":    "blue",
		"X-Amz-Checksum-Crc32": "NhCmhg==",
		"Content-Type":         "text/plain",
		"Content-Disposition":  "inline",
		"Content-Encoding":     "gzip",
		"Content-Language":     "en-GB",
		"Cache-Control":        "no-cache",
		"Expires":              "Thu, 01 Jan 2026 00:00:00 GMT",
	}
	dropped := []string{
		// signing
		"X-Amz-Content-Sha256",
		"X-Amz-Date",
		"X-Amz-Decoded-Content-Length",
		"X-Amz-Sdk-Checksum-Algorithm",
		"X-Amz-Trailer",
		"X-Amz-Security-Token",
		// routing
		"X-Amz-Copy-Source",
		"X-Amz-Metadata-Directive",
		// accepted and not implemented, so they must not read back as applied
		"X-Amz-Acl",
		"X-Amz-Storage-Class",
		"X-Amz-Tagging",
		"X-Amz-Object-Lock-Mode",
		"X-Amz-Object-Lock-Retain-Until-Date",
		// encryption
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Key",
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

// TestWriteGetOrHeadObjectHeaders checks that metadata stored by an earlier
// version is filtered out of a response rather than served back.
func TestWriteGetOrHeadObjectHeaders(t *testing.T) {
	obj := &Object{
		LastModified: time.Now(),
		Metadata: map[string]string{
			"X-Amz-Server-Side-Encryption-Customer-Key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
			"X-Amz-Security-Token":                      "session",
			"X-Amz-Date":                                "20260908T000000Z",
			"X-Amz-Object-Lock-Mode":                    "GOVERNANCE",
			"x-amz-meta-keep":                           "kept",
			"Content-Type":                              "text/plain",
		},
	}

	w := httptest.NewRecorder()
	if err := writeGetOrHeadObjectHeaders(obj, w, httptest.NewRequest(http.MethodGet, "/bucket/object", nil)); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{
		"X-Amz-Server-Side-Encryption-Customer-Key",
		"X-Amz-Security-Token",
		"X-Amz-Date",
		"X-Amz-Object-Lock-Mode",
	} {
		if got := w.Header().Get(k); got != "" {
			t.Fatal("served", k, got)
		}
	}
	// user metadata keys are served lowercased
	if got := w.Header()["X-Amz-Meta-keep"]; len(got) != 1 || got[0] != "kept" {
		t.Fatal("mismatch", got)
	}
	if got := w.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatal("mismatch", got)
	}
}
