package s3

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestValidateBucketName(t *testing.T) {
	type tcase struct {
		name string
		err  *S3Error
	}

	baseCases := []tcase{
		{"", &ErrInvalidBucketName},

		// This is not in nameCases because appending labels to it will cause an error:
		{strings.Repeat("1", 63), nil},

		// Appending labels to these causes them to pass:
		{"192.168.1.1", &ErrInvalidBucketName},     // IP addresses are not allowed as bucket names. These may trip the "3-char min" rule first.
		{"192.168.111.111", &ErrInvalidBucketName}, // These should not trip the 3-char min but should still fail.
	}

	nameCases := []tcase{
		{"yep", nil},
		{"0yep", nil},
		{"yep0", nil},
		{"y-p", nil},
		{"y--p", nil},

		{"NUP", &ErrInvalidBucketName},
		{"n🤡p", &ErrInvalidBucketName}, // UTF-8 is effectively invalid because the high bytes fall outside the legal range
		{"-nup", &ErrInvalidBucketName},
		{"nup-", &ErrInvalidBucketName},
		{"-nup-", &ErrInvalidBucketName},

		{"1", &ErrInvalidBucketName},  // Too short
		{"12", &ErrInvalidBucketName}, // Too short
		{"123", nil},
		{strings.Repeat("1", 64), &ErrInvalidBucketName},
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
