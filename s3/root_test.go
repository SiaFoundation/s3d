package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"go.uber.org/zap/zaptest"
)

// TestServiceRootMethods tests that requests to the service root which are not
// ListBuckets are answered the way AWS answers them: 405 with "Allow: GET",
// dispatched on the method before authentication. Clients probe the root before
// logging in -- Cyberduck sends an unsigned HEAD / -- and treat anything else,
// including a 403, as a failed connection.
func TestServiceRootMethods(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	for _, method := range []string{http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "http://localhost/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
			}
			if got := rec.Header().Get("Allow"); got != http.MethodGet {
				t.Errorf("Allow = %q, want %q", got, http.MethodGet)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "xml") {
				t.Errorf("Content-Type = %q, want an XML type", ct)
			}
			// HEAD responses carry no body
			if method != http.MethodHead {
				if body := rec.Body.String(); !strings.Contains(body, "MethodNotAllowed") {
					t.Errorf("body = %q, want it to contain MethodNotAllowed", body)
				}
			}
		})
	}
}
