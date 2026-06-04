package s3_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"lukechampine.com/frand"
)

// recordingTransport wraps an http.RoundTripper and records the
// x-amz-content-sha256 header sent on each request so tests can assert which
// payload-signing mode the client picked.
type recordingTransport struct {
	base http.RoundTripper

	mu      sync.Mutex
	sha256s []string
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	rt.sha256s = append(rt.sha256s, req.Header.Get("X-Amz-Content-Sha256"))
	rt.mu.Unlock()
	return rt.base.RoundTrip(req)
}

func (rt *recordingTransport) contains(want string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Contains(rt.sha256s, want)
}

// recordingHTTPClient adapts a recordingTransport to aws.HTTPClient so it can
// be injected via service.Options.HTTPClient.
type recordingHTTPClient struct{ rt *recordingTransport }

func (c *recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.rt.RoundTrip(req)
}

// readerOnly hides any optional interfaces (io.Seeker, io.ReaderAt, ...) the
// underlying reader may satisfy, forcing clients that would otherwise rewind
// the body onto their streaming path.
type readerOnly struct{ io.Reader }

// newMinioClient builds a minio-go client pointing at the testutil S3
// server. trailingHeaders=true switches PutObject to the signed-trailer
// variant; false keeps it on plain signed-streaming.
func newMinioClient(t *testing.T, serverURL string, trailingHeaders bool, rt http.RoundTripper) *minio.Client {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := minio.New(u.Host, &minio.Options{
		Creds:           credentials.NewStaticV4(testutil.AccessKeyID, testutil.SecretAccessKey, ""),
		Secure:          u.Scheme == "https",
		Region:          "us-east-1",
		BucketLookup:    minio.BucketLookupPath,
		TrailingHeaders: trailingHeaders,
		Transport:       rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestStreamingAuthE2E exercises the aws-chunked content-sha256 modes
// against real third-party S3 clients. aws-sdk-go-v2 only emits the
// unsigned-trailer variant, so the signed variants are driven through
// minio-go which still produces them by default over HTTP.
func TestStreamingAuthE2E(t *testing.T) {
	payload := frand.Bytes(200 * 1024) // > 64 KiB so the body spans multiple chunks

	t.Run("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", func(t *testing.T) {
		rt := &recordingTransport{base: http.DefaultTransport}
		s3Tester := testutil.NewTester(t)
		if err := s3Tester.CreateBucket(t.Context(), "bucket"); err != nil {
			t.Fatal(err)
		}

		mc := newMinioClient(t, *s3Tester.Client().Options().BaseEndpoint, false, rt)
		if _, err := mc.PutObject(t.Context(), "bucket", "obj",
			bytes.NewReader(payload), int64(len(payload)),
			minio.PutObjectOptions{DisableMultipart: true},
		); err != nil {
			t.Fatal("put:", err)
		}
		if !rt.contains("STREAMING-AWS4-HMAC-SHA256-PAYLOAD") {
			t.Fatalf("no request used STREAMING-AWS4-HMAC-SHA256-PAYLOAD; observed=%v", rt.sha256s)
		}

		obj, err := s3Tester.GetObject(t.Context(), "bucket", "obj", nil)
		if err != nil {
			t.Fatal("get:", err)
		}
		got, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	})

	t.Run("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", func(t *testing.T) {
		rt := &recordingTransport{base: http.DefaultTransport}
		s3Tester := testutil.NewTester(t)
		if err := s3Tester.CreateBucket(t.Context(), "bucket"); err != nil {
			t.Fatal(err)
		}

		mc := newMinioClient(t, *s3Tester.Client().Options().BaseEndpoint, true, rt)
		if _, err := mc.PutObject(t.Context(), "bucket", "obj",
			bytes.NewReader(payload), int64(len(payload)),
			minio.PutObjectOptions{
				DisableMultipart: true,
				Checksum:         minio.ChecksumCRC32C,
			},
		); err != nil {
			t.Fatal("put:", err)
		}
		if !rt.contains("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER") {
			t.Fatalf("no request used STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER; observed=%v", rt.sha256s)
		}

		obj, err := s3Tester.GetObject(t.Context(), "bucket", "obj", nil)
		if err != nil {
			t.Fatal("get:", err)
		}
		got, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	})

	t.Run("STREAMING-UNSIGNED-PAYLOAD-TRAILER", func(t *testing.T) {
		// aws-sdk-go-v2 gates trailing checksums on HTTPS, so use a TLS
		// httptest server.
		rt := &recordingTransport{}
		s3Tester := testutil.NewTester(t,
			testutil.WithTLS(),
			testutil.WithServiceOptions(func(o *service.Options) {
				// wrap whatever HTTPClient testutil already installed so
				// TLS roots from httptest.Server are preserved.
				inner, ok := o.HTTPClient.(*http.Client)
				if !ok {
					t.Fatal("HTTPClient is not *http.Client")
				}
				rt.base = inner.Transport
				o.HTTPClient = &recordingHTTPClient{rt: rt}
			}),
		)
		if err := s3Tester.CreateBucket(t.Context(), "bucket"); err != nil {
			t.Fatal(err)
		}

		if _, err := s3Tester.Client().PutObject(t.Context(), &service.PutObjectInput{
			Bucket:            aws.String("bucket"),
			Key:               aws.String("obj"),
			Body:              readerOnly{bytes.NewReader(payload)},
			ContentLength:     aws.Int64(int64(len(payload))),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		}); err != nil {
			t.Fatal("put:", err)
		}
		if !rt.contains("STREAMING-UNSIGNED-PAYLOAD-TRAILER") {
			t.Fatalf("no request used STREAMING-UNSIGNED-PAYLOAD-TRAILER; observed=%v", rt.sha256s)
		}

		obj, err := s3Tester.GetObject(t.Context(), "bucket", "obj", nil)
		if err != nil {
			t.Fatal("get:", err)
		}
		got, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	})
}
