package s3_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

// assertS3Error is a helper to check an error returned from the AWS SDK against
// an expected s3.S3Error. Unfortunately the SDK doesn't expose its internal
// error type so reflection is not an option and we need to extract the status
// code from the string.
func assertS3Error(t testing.TB, expected s3.Error, got error) {
	t.Helper()
	if got == nil {
		t.Fatal("expected error, got nil")
	}

	// check status code
	re := regexp.MustCompile(`StatusCode: (\d{3})`)
	matches := re.FindStringSubmatch(got.Error())
	if len(matches) != 2 {
		t.Fatalf("expected error to contain status code, got: %v", got)
	}
	var code int
	fmt.Sscanf(matches[1], "%d", &code)
	if code != expected.HTTPStatus {
		t.Fatalf("expected status code %d, got %d", expected.HTTPStatus, code)
	}

	// check error
	if !strings.Contains(got.Error(), expected.Code) {
		t.Fatalf("expected error code %q, got %q", expected.Code, got.Error())
	}
}

func TestBuckets(t *testing.T) {
	run := func(t *testing.T, pathStyle bool) {
		s3Tester := newTester(t, func(o *service.Options) {
			o.UsePathStyle = pathStyle
		})

		// create the bucket
		err := s3Tester.CreateBucket(t.Context(), "bucket")
		if err != nil {
			t.Fatal(err)
		}

		// make sure it shows up in the list
		buckets, err := s3Tester.ListBuckets(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 1 || *buckets[0].Name != "bucket" {
			t.Fatalf("unexpected buckets: %v", buckets)
		}

		// creating it again should fail
		err = s3Tester.CreateBucket(t.Context(), "bucket")
		assertS3Error(t, s3.ErrBucketAlreadyOwnedByYou, err)

		// creating a bucket with invalid name should fail
		err = s3Tester.CreateBucket(t.Context(), "invalid_bucket")
		assertS3Error(t, s3.ErrInvalidBucketName, err)
	}

	t.Run("VirtualHostedStyle", func(t *testing.T) {
		run(t, false)
	})

	t.Run("PathStyle", func(t *testing.T) {
		run(t, true)
	})
}
