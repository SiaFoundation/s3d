package s3_test

import (
	"testing"

	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestBuckets(t *testing.T) {
	run := func(t *testing.T, pathStyle bool) {
		s3Tester := testutil.NewTester(t, func(o *service.Options) {
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
		testutil.AssertS3Error(t, s3errs.ErrBucketAlreadyOwnedByYou, err)

		// creating a bucket with invalid name should fail
		err = s3Tester.CreateBucket(t.Context(), "invalid_bucket")
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketName, err)
	}

	t.Run("VirtualHostedStyle", func(t *testing.T) {
		run(t, false)
	})

	t.Run("PathStyle", func(t *testing.T) {
		run(t, true)
	})
}
