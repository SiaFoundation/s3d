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
