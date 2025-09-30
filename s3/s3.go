package s3

import (
	"net/http"

	"go.uber.org/zap"
)

// Backend defines the interface for an S3 backend that data uploaded via the S3
// API will be stored in.
type Backend interface {
}

type s3 struct {
	logger *zap.Logger
}

// New creates an instance of the S3 API handler using the provided backend.
func New(b Backend, logger *zap.Logger) http.Handler {
	s3 := &s3{
		logger: logger.Named("s3"),
	}
	return http.HandlerFunc(s3.handleRequest)
}

func (s *s3) handleRequest(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("new request", zap.Stringer("url", r.URL))
}
