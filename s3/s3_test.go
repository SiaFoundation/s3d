package s3_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap/zaptest"
)

func newAdminServer(t *testing.T) (string, *http.Client) {
	t.Helper()
	backend := testutil.NewBackend(t)
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

func TestUploadStats(t *testing.T) {
	baseURL, httpClient := newAdminServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/stats/uploads", nil)
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
		t.Fatalf("expected JSON Content-Type, got %q", ct)
	}

	var stats s3.UploadStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	} else if (stats != s3.UploadStats{}) {
		t.Fatal("expected zero-value stats on an empty backend", stats)
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

// TestHostBucketStyles verifies that path-style and virtual-hosted-style
// requests both work, with and without a port in the Host header.
// "localhost" is implicitly available as a host bucket base.
func TestHostBucketStyles(t *testing.T) {
	backend := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	signer := v4.NewSigner()
	creds := aws.Credentials{
		AccessKeyID:     testutil.AccessKeyID,
		SecretAccessKey: testutil.SecretAccessKey,
	}

	do := func(t *testing.T, method, host, path, body string) *httptest.ResponseRecorder {
		t.Helper()

		req := httptest.NewRequest(method, "http://"+host+path, strings.NewReader(body))
		req.Host = host
		if body != "" {
			req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		}
		payloadHash := sha256.Sum256([]byte(body))
		hashHex := hex.EncodeToString(payloadHash[:])
		req.Header.Set("X-Amz-Content-Sha256", hashHex)
		if err := signer.SignHTTP(t.Context(), creds, req, hashHex, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// create a bucket containing an object using path-style requests
	if rec := do(t, http.MethodPut, "localhost:8000", "/bucket", ""); rec.Code != http.StatusOK {
		t.Fatalf("failed to create bucket: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, http.MethodPut, "localhost:8000", "/bucket/object", "hello"); rec.Code != http.StatusOK {
		t.Fatalf("failed to create object: %d %s", rec.Code, rec.Body)
	}

	tests := []struct {
		name string
		host string
		path string
	}{
		{"path style with port", "localhost:8000", "/bucket/object"},
		{"path style without port", "localhost", "/bucket/object"},
		{"virtual-hosted style with port", "bucket.localhost:8000", "/object"},
		{"virtual-hosted style without port", "bucket.localhost", "/object"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := do(t, http.MethodGet, test.host, test.path, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body)
			} else if body, err := io.ReadAll(rec.Result().Body); err != nil {
				t.Fatal(err)
			} else if string(body) != "hello" {
				t.Fatalf("expected body %q, got %q", "hello", body)
			}
		})
	}
}
