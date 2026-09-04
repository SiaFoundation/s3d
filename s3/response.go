package s3

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

const (
	// maxDrainBytes bounds how much of a rejected request body is drained
	// before giving up on connection reuse.
	maxDrainBytes = 2 * MinUploadPartSize

	// maxDrainTime bounds how long draining a rejected request body may take.
	maxDrainTime = 5 * time.Second
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
// is used. Any unread remainder of the request body is drained up to a cap
// first so the response is not lost to a connection reset.
func writeErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
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

	drained := drainRequestBody(w, r)

	// a delete-marker error must still carry x-amz-delete-marker (and, for 405,
	// Last-Modified/Allow) so clients can tell it apart from a missing object.
	var keep []string
	if w.Header().Get("x-amz-delete-marker") != "" {
		keep = []string{"X-Amz-Delete-Marker", "X-Amz-Version-Id", "Last-Modified", "Allow"}
	}
	if s3Err.HTTPStatus == http.StatusMethodNotAllowed {
		// a 405 has to say which methods are accepted
		keep = append(keep, "Allow")
	}

	// clear any headers that may have been set before the error was detected
	// (e.g. conditional GET sets ETag and metadata before checking If-Match)
	clearHeadersExceptCORS(w.Header(), keep...)

	// a connection with an unconsumed body cannot be reused
	if !drained {
		w.Header().Set("Connection", "close")
	}

	writeXMLResponse(w, s3Err.HTTPStatus, ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Description,
		RequestID: "", // unused right now (AWS uses it for diagnostic purposes)
		HostID:    "", // unused right now (AWS uses it to identify their server)
	})
}

// drainRequestBody consumes the unread remainder of a rejected request's body
// so the error response is delivered over a healthy connection. Without the
// drain net/http only discards small remainders itself and otherwise closes
// the connection, and the resulting TCP reset races with the response so
// clients may see a transport error instead of the S3 error. The drain is
// capped in size and time so a rejected upload never consumes an arbitrarily
// large body. It returns true if the body was fully consumed.
func drainRequestBody(w http.ResponseWriter, r *http.Request) bool {
	// skip the drain when the declared body already exceeds the cap so a
	// rejected upload does not delay its response reading bytes that end up
	// discarded anyway
	if r.ContentLength > maxDrainBytes {
		return false
	}

	rc := http.NewResponseController(w)
	hasDeadline := rc.SetReadDeadline(time.Now().Add(maxDrainTime)) == nil
	_, err := io.CopyN(io.Discard, r.Body, maxDrainBytes+1)
	if hasDeadline {
		_ = rc.SetReadDeadline(time.Time{})
	}
	return errors.Is(err, io.EOF)
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
