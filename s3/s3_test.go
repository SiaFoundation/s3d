package s3_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUploadStats(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	opts := s3Tester.Client().Options()
	httpClient := opts.HTTPClient
	baseURL := *opts.BaseEndpoint

	t.Run("GET returns 200 with zero stats", func(t *testing.T) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/_s3d/status/uploads", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		} else if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", ct)
		}

		var stats s3.UploadStats
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			t.Fatal(err)
		} else if stats != (s3.UploadStats{}) {
			t.Fatalf("expected zero stats, got %+v", stats)
		}
	})

	t.Run("anonymous request is allowed", func(t *testing.T) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/_s3d/status/uploads", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected anonymous access to succeed, got %d", resp.StatusCode)
		}
	})
}

// TestInvalidCredentials tests that API calls with invalid credentials fail.
func TestInvalidCredentials(t *testing.T) {
	s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
		o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "wrongID",
				SecretAccessKey: "wrongSecret",
			},
		})
	}))
	err := s3Tester.CreateBucket(t.Context(), "bucket")
	testutil.AssertS3Error(t, s3errs.ErrInvalidAccessKeyId, err)
}

// TestAccessDenied tests that API calls that are not supported for anonymous
// users fail with AccessDenied.
func TestAccessDenied(t *testing.T) {
	assertAccessDenied := func(t *testing.T, name string, run func(t *testing.T, s3 *testutil.S3Tester) error) {
		t.Run(name, func(t *testing.T) {
			s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
				o.Credentials = aws.NewCredentialsCache(aws.AnonymousCredentials{})
			}))
			testutil.AssertS3Error(t, s3errs.ErrAccessDenied, run(t, s3Tester))
		})
	}

	// bucket routes
	assertAccessDenied(t, "CreateBucket", func(t *testing.T, s3 *testutil.S3Tester) error {
		return s3.CreateBucket(t.Context(), "bucket")
	})
	assertAccessDenied(t, "ListBuckets", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListBuckets(t.Context())
		return err
	})

	// object routes
	assertAccessDenied(t, "PutObject", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.PutObject(t.Context(), "foo", "bar", bytes.NewReader(nil), nil)
		return err
	})
	assertAccessDenied(t, "DeleteObject", func(t *testing.T, s3 *testutil.S3Tester) error {
		err := s3.DeleteObject(t.Context(), "bucket", "object")
		return err
	})
	assertAccessDenied(t, "DeleteObjects", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.DeleteObjects(t.Context(), "bucket", testutil.ObjectIdentifiers("object1", "object2"), nil)
		return err
	})

	// multipart upload routes
	assertAccessDenied(t, "CreateMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.CreateMultipartUpload(t.Context(), "bucket", "object", nil)
		return err
	})
	assertAccessDenied(t, "AbortMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		err := s3.AbortMultipartUpload(t.Context(), "bucket", "object", "uploadID")
		return err
	})
	assertAccessDenied(t, "CompleteMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.CompleteMultipartUpload(t.Context(), "bucket", "object", "uploadID", nil)
		return err
	})
	assertAccessDenied(t, "UploadPart", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.UploadPart(t.Context(), "bucket", "object", "uploadID", 1, nil)
		return err
	})
	assertAccessDenied(t, "ListParts", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListParts(t.Context(), "bucket", "object", "uploadID", nil, nil)
		return err
	})
	assertAccessDenied(t, "ListMultipartUploads", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListMultipartUploads(t.Context(), "bucket", nil)
		return err
	})
}
