package s3

import (
	"errors"
	"fmt"
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
