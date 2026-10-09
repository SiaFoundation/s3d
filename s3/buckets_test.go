package s3_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap/zaptest"
)

func TestBuckets(t *testing.T) {
	const (
		bucket = "bucket"
		key    = "key"
	)

	run := func(t *testing.T, pathStyle bool) {
		// prepare a tester with an additional keypair
		s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
			o.UsePathStyle = pathStyle
		}), testutil.WithKeyPair("other", "foo", "bar"))

		// create another valid keypair and a tester to use it
		otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")

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

		// bucket location should be empty, which is how S3 reports us-east-1
		location, err := s3Tester.BucketLocation(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if location != "" {
			t.Fatalf("unexpected location: %q", location)
		}

		// bucket should not be accessible by other account
		err = otherTester.HeadBucket(t.Context(), bucket)
		testutil.AssertS3StatusCode(t, s3errs.ErrAccessDenied, err)

		// bucket location should not be readable by other account
		_, err = otherTester.BucketLocation(t.Context(), bucket)
		testutil.AssertS3StatusCode(t, s3errs.ErrAccessDenied, err)

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

		// re-creating an owned bucket should fail with BucketAlreadyOwnedByYou
		err = s3Tester.CreateBucket(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrBucketAlreadyOwnedByYou, err)

		// creating a bucket with invalid name should fail
		err = s3Tester.CreateBucket(t.Context(), "invalid_bucket")
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketName, err)

		// creating an existing bucket with different account should fail
		err = otherTester.CreateBucket(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrBucketAlreadyExists, err)

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

// TestCreateBucketObjectLockHeader verifies that a present object lock header
// must carry a boolean. The AWS SDK serializes a bool and can never send a
// malformed value, so the requests are signed by hand. The header has to be
// signed because an unsigned x-amz header is rejected before it reaches the
// handler.
func TestCreateBucketObjectLockHeader(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	signer := v4.NewSigner()
	creds := aws.Credentials{
		AccessKeyID:     testutil.AccessKeyID,
		SecretAccessKey: testutil.SecretAccessKey,
	}

	do := func(t *testing.T, method, path, lock string) *httptest.ResponseRecorder {
		t.Helper()

		req := httptest.NewRequest(method, "http://localhost:8000"+path, nil)
		req.Host = "localhost:8000"
		if lock != "" {
			req.Header.Set(s3.HeaderBucketObjectLockEnabled, lock)
		}
		payloadHash := sha256.Sum256(nil)
		hashHex := hex.EncodeToString(payloadHash[:])
		req.Header.Set("X-Amz-Content-Sha256", hashHex)
		if err := signer.SignHTTP(t.Context(), creds, req, hashHex, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// a malformed value is refused instead of being read as false
	for _, lock := range []string{"invalid", "1", "yes", "enabled"} {
		rec := do(t, http.MethodPut, "/malformed", lock)
		if rec.Code != http.StatusBadRequest {
			t.Fatal("unexpected", lock, rec.Code, rec.Body)
		} else if !strings.Contains(rec.Body.String(), s3errs.ErrInvalidRequest.Code) {
			t.Fatal("unexpected", lock, rec.Body)
		}
	}

	// the refused requests created nothing, so the name is still free
	if rec := do(t, http.MethodPut, "/malformed", "TRUE"); rec.Code != http.StatusOK {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}

	// a true value in any case enables object lock
	if rec := do(t, http.MethodGet, "/malformed?object-lock", ""); rec.Code != http.StatusOK {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}

	// false is accepted and leaves object lock off
	if rec := do(t, http.MethodPut, "/unlocked", "false"); rec.Code != http.StatusOK {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}
	if rec := do(t, http.MethodGet, "/unlocked?object-lock", ""); rec.Code != http.StatusNotFound {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}

	// an absent header behaves the same as false
	if rec := do(t, http.MethodPut, "/absent", ""); rec.Code != http.StatusOK {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}
	if rec := do(t, http.MethodGet, "/absent?object-lock", ""); rec.Code != http.StatusNotFound {
		t.Fatal("unexpected", rec.Code, rec.Body)
	}
}
