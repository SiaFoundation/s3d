package s3

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"

	"github.com/SiaFoundation/s3d/internal/prometheus"
)

// statusUploadsPath is the request path of the upload statistics endpoint.
const statusUploadsPath = "_s3d/status/uploads"

// UploadStats contains statistics about the background upload pipeline.
type UploadStats struct {
	PendingObjects   int64 `json:"pendingObjects"`
	PendingSize      int64 `json:"pendingSize"`
	UploadedObjects  int64 `json:"uploadedObjects"`
	UploadedSize     int64 `json:"uploadedSize"`
	FailedUploads    int64 `json:"failedUploads"`
	OrphanedObjects  int64 `json:"orphanedObjects"`
	MultipartUploads int64 `json:"multipartUploads"`
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// upload stats response.
func (s UploadStats) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name:  "s3d_upload_pending_objects",
			Value: float64(s.PendingObjects),
		},
		{
			Name:  "s3d_upload_pending_size_bytes",
			Value: float64(s.PendingSize),
		},
		{
			Name:  "s3d_upload_uploaded_objects",
			Value: float64(s.UploadedObjects),
		},
		{
			Name:  "s3d_upload_uploaded_size_bytes",
			Value: float64(s.UploadedSize),
		},
		{
			Name:  "s3d_upload_failed_uploads",
			Value: float64(s.FailedUploads),
		},
		{
			Name:  "s3d_upload_orphaned_objects",
			Value: float64(s.OrphanedObjects),
		},
		{
			Name:  "s3d_upload_multipart_uploads",
			Value: float64(s.MultipartUploads),
		},
	}
}

// authenticateStatus checks the request's HTTP Basic authentication credentials
// against the configured status password. If authentication fails, it writes a
// 401 response and returns false. The status endpoints are inaccessible when no
// status password is configured.
func (s *s3) authenticateStatus(w http.ResponseWriter, r *http.Request) bool {
	_, password, ok := r.BasicAuth()
	if !ok || s.statusPassword == "" || subtle.ConstantTimeCompare([]byte(password), []byte(s.statusPassword)) != 1 {
		w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *s3) handleUploadStats(w http.ResponseWriter, r *http.Request) error {
	if !s.authenticateStatus(w, r) {
		return nil
	}
	stats, err := s.backend.UploadStats(r.Context())
	if err != nil {
		return err
	}

	switch r.URL.Query().Get("response") {
	case "prometheus":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		return prometheus.NewEncoder(w).Append(stats)
	default:
		w.Header().Set("Content-Type", "application/json")
		return json.NewEncoder(w).Encode(stats)
	}
}
