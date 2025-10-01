package s3_test

import (
	"testing"

	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestBuckets(t *testing.T) {
	run := func(t *testing.T, pathStyle bool) {
		s3Tester := newTester(t, func(o *service.Options) {
			o.UsePathStyle = pathStyle
		})
		err := s3Tester.CreateBucket(t.Context(), "bucket")
		if err != nil {
			t.Fatal(err)
		}
		buckets, err := s3Tester.ListBuckets(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 1 || *buckets[0].Name != "bucket" {
			t.Fatalf("unexpected buckets: %v", buckets)
		}
	}

	t.Run("VirtualHostedStyle", func(t *testing.T) {
		run(t, false)
	})

	t.Run("PathStyle", func(t *testing.T) {
		run(t, true)
	})
}
