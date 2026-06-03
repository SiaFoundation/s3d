package s3_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func newAdminServer(t *testing.T) (string, *http.Client) {
	t.Helper()
	backend := testutil.NewMemoryBackend(testutil.WithKeyPair(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey))
	server := httptest.NewServer(s3.NewAdmin(backend))
	t.Cleanup(server.Close)
	return server.URL, server.Client()
}

func TestPrometheus(t *testing.T) {
	baseURL, httpClient := newAdminServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/prometheus", nil)
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
	} else if ct := resp.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Fatalf("expected Prometheus Content-Type, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("s3d_upload_pending_objects 0")) {
		t.Fatalf("expected prometheus metrics, got %q", body)
	}
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
