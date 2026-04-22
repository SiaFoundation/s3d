package s3_test

import (
	"bytes"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestBuckets(t *testing.T) {
	const (
		bucket = "bucket"
		key    = "key"
	)

	run := func(t *testing.T, pathStyle bool) {
		// TODO: restore multi-owner coverage once the sia backend supports owners.
		// // prepare a backend with 2 keypairs
		// backend := testutil.NewMemoryBackend(
		// 	testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey),
		// 	testutil.WithKeyPair("other", "foo", "bar"),
		// )

		s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
			o.UsePathStyle = pathStyle
		}))

		// // create another valid keypair and a tester to use it
		// otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")

		// check that the bucket doesn't exist yet
		err := s3Tester.HeadBucket(t.Context(), bucket)
		testutil.AssertS3StatusCode(t, s3errs.ErrNoSuchBucket, err)

		// create the bucket
		err = s3Tester.CreateBucket(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}

		// bucket should exist now
		err = s3Tester.HeadBucket(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}

		// bucket location should be "null"
		location, err := s3Tester.BucketLocation(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if location != s3.Null {
			t.Fatalf("unexpected location: %q", location)
		}

		// // bucket should not be accessible by other account
		// err = otherTester.HeadBucket(t.Context(), bucket)
		// testutil.AssertS3StatusCode(t, s3errs.ErrAccessDenied, err)

		// make sure it shows up in the list
		buckets, err := s3Tester.ListBuckets(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(buckets) != 1 || *buckets[0].Name != bucket {
			t.Fatalf("unexpected buckets: %v", buckets)
		}

		// add an object to the bucket
		_, err = s3Tester.PutObject(t.Context(), bucket, key, bytes.NewReader([]byte("value")), nil)
		if err != nil {
			t.Fatal(err)
		}

		// creating it again should fail
		err = s3Tester.CreateBucket(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrBucketAlreadyOwnedByYou, err)

		// creating a bucket with invalid name should fail
		err = s3Tester.CreateBucket(t.Context(), "invalid_bucket")
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketName, err)

		// // creating an existing bucket with different account should fail
		// err = otherTester.CreateBucket(t.Context(), bucket)
		// testutil.AssertS3Error(t, s3errs.ErrBucketAlreadyExists, err)

		// deleting the bucket should fail since it's not empty
		err = s3Tester.DeleteBucket(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrBucketNotEmpty, err)

		// delete the object
		err = s3Tester.DeleteObject(t.Context(), bucket, key)
		if err != nil {
			t.Fatal(err)
		}

		// now deleting the bucket should succeed
		err = s3Tester.DeleteBucket(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}

		// recreate the bucket
		err = s3Tester.CreateBucket(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}

		// start a multipart upload
		upload, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, key, nil)
		if err != nil {
			t.Fatal(err)
		}

		// deleting the bucket should fail since there's a pending multipart upload
		err = s3Tester.DeleteBucket(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrBucketNotEmpty, err)

		// abort the multipart upload
		err = s3Tester.AbortMultipartUpload(t.Context(), bucket, key, *upload.UploadId)
		if err != nil {
			t.Fatal(err)
		}

		// now deleting the bucket should succeed
		err = s3Tester.DeleteBucket(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("VirtualHostedStyle", func(t *testing.T) {
		run(t, false)
	})

	t.Run("PathStyle", func(t *testing.T) {
		run(t, true)
	})
}
