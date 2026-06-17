package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"go.uber.org/zap/zaptest"
)

func TestCORS(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	assertCORSHeaders := func(t *testing.T, h http.Header) {
		t.Helper()
		if got := h.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
		} else if got := h.Get("Access-Control-Allow-Methods"); got == "" {
			t.Error("Access-Control-Allow-Methods is missing")
		} else if got := h.Get("Access-Control-Allow-Headers"); got != "Authorization, *" {
			// the "*" wildcard does not authorize the Authorization header used by
			// AWS SigV4 signing, so it must be advertised explicitly.
			t.Errorf("Access-Control-Allow-Headers = %q, want %q", got, "Authorization, *")
		} else if got := h.Get("Access-Control-Expose-Headers"); got != "ETag" {
			t.Errorf("Access-Control-Expose-Headers = %q, want %q", got, "ETag")
		} else if got := h.Get("Access-Control-Max-Age"); got == "" {
			t.Error("Access-Control-Max-Age is missing")
		} else if got := h.Get("Vary"); got != "Origin" {
			t.Errorf("Vary = %q, want %q", got, "Origin")
		}
	}

	t.Run("preflight", func(t *testing.T) {
		for _, target := range []string{"http://localhost/", "http://localhost/bucket", "http://localhost/bucket/object"} {
			t.Run(target, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodOptions, target, nil)
				req.Header.Set("Origin", "https://example.com")
				req.Header.Set("Access-Control-Request-Method", http.MethodPut)
				req.Header.Set("Access-Control-Request-Headers", "authorization,x-amz-content-sha256")
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				if rec.Code != http.StatusNoContent {
					t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
				}
				assertCORSHeaders(t, rec.Header())
			})
		}
	})

	t.Run("response", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
		req.Header.Set("Origin", "https://example.com")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assertCORSHeaders(t, rec.Header())
	})
}
