package s3

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// ErrorResponse is the standard XML error response returned by S3.
type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`

	Code      string `xml:"Code"`
	Message   string `xml:"Message,omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
	HostID    string `xml:"HostId,omitempty"`
}

// writeErrorResponse writes an error response to the ResponseWriter. The
// provided err must not be nil. If err is not an [Error], [ErrInternalError]
// is used.
func writeErrorResponse(w http.ResponseWriter, err error) {
	if err == nil {
		panic("WriteErrorResponse called with nil error")
	}

	var s3Err *s3errs.Error
	if inner := new(s3errs.Error); errors.As(err, inner) {
		s3Err = inner
	} else {
		s3Err = &s3errs.ErrInternalError
	}

	if s3Err.HTTPStatus == http.StatusNotModified {
		// 304 must preserve ETag/Last-Modified but must not include a body
		// or headers that trigger body parsing in SDKs
		clearHeadersExceptCORS(w.Header(), "ETag", "Last-Modified")
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// a delete-marker error must still carry x-amz-delete-marker (and, for 405,
	// Last-Modified/Allow) so clients can tell it apart from a missing object.
	var keep []string
	if w.Header().Get("x-amz-delete-marker") != "" {
		keep = []string{"X-Amz-Delete-Marker", "X-Amz-Version-Id", "Last-Modified", "Allow"}
	}

	// clear any headers that may have been set before the error was detected
	// (e.g. conditional GET sets ETag and metadata before checking If-Match)
	clearHeadersExceptCORS(w.Header(), keep...)

	writeXMLResponse(w, s3Err.HTTPStatus, ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Description,
		RequestID: "", // unused right now (AWS uses it for diagnostic purposes)
		HostID:    "", // unused right now (AWS uses it to identify their server)
	})
}

// clearHeadersExceptCORS removes every header from h except CORS headers set
// by corsMiddleware (Vary and Access-Control-*). Without this, error responses
// drop the CORS headers and browser clients see opaque failures. The current
// values of any headers named in keep are also preserved.
func clearHeadersExceptCORS(h http.Header, keep ...string) {
	saved := make(map[string]string, len(keep))
	for _, k := range keep {
		if v := h.Get(k); v != "" {
			saved[k] = v
		}
	}
	for k := range h {
		if k == "Vary" || strings.HasPrefix(k, "Access-Control-") {
			continue
		}
		h.Del(k)
	}
	for k, v := range saved {
		h.Set(k, v)
	}
}

func writeXMLResponse(w http.ResponseWriter, statusCode int, resp any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	w.Write([]byte(xml.Header))

	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")

	return xe.Encode(resp)
}
