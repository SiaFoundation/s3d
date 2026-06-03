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
		etag := w.Header().Get("ETag")
		lastMod := w.Header().Get("Last-Modified")
		clearHeadersExceptCORS(w.Header())
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		if lastMod != "" {
			w.Header().Set("Last-Modified", lastMod)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// clear any headers that may have been set before the error was detected
	// (e.g. conditional GET sets ETag and metadata before checking If-Match)
	clearHeadersExceptCORS(w.Header())

	writeXMLResponse(w, s3Err.HTTPStatus, ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Description,
		RequestID: "", // unused right now (AWS uses it for diagnostic purposes)
		HostID:    "", // unused right now (AWS uses it to identify their server)
	})
}

// clearHeadersExceptCORS removes every header from h except CORS headers set
// by corsMiddleware (Vary and Access-Control-*). Without this, error responses
// drop the CORS headers and browser clients see opaque failures.
func clearHeadersExceptCORS(h http.Header) {
	for k := range h {
		if k == "Vary" || strings.HasPrefix(k, "Access-Control-") {
			continue
		}
		h.Del(k)
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
