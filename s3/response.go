package s3

import (
	"encoding/xml"
	"errors"
	"net/http"

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
		for k := range w.Header() {
			w.Header().Del(k)
		}
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
	for k := range w.Header() {
		w.Header().Del(k)
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(s3Err.HTTPStatus)

	writeXMLResponse(w, ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Description,
		RequestID: "", // unused right now (AWS uses it for diagnostic purposes)
		HostID:    "", // unused right now (AWS uses it to identify their server)
	})
}

func writeXMLResponse(w http.ResponseWriter, resp any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))

	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")

	return xe.Encode(resp)
}
