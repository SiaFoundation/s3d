package s3

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"go.sia.tech/jape"
)

// BackupSQLite3Request is the request body for the [POST] /system/sqlite3/backup
// endpoint.
type BackupSQLite3Request struct {
	// Path is the absolute filesystem path where the backup file will be
	// written. It must not already exist.
	Path string `json:"path"`
}

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

// handlePrometheus serves the admin API metrics in the Prometheus text
// exposition format. Currently the only metrics exposed are the background
// upload stats.
func (s *s3) handlePrometheus(jc jape.Context) {
	stats, err := s.backend.UploadStats(jc.Request.Context())
	if jc.Check("failed to get upload stats", err) != nil {
		return
	}

	jc.ResponseWriter.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if jc.Check("failed to marshal prometheus response", prometheus.NewEncoder(jc.ResponseWriter).Append(stats)) != nil {
		return
	}
}

// handleGetUploadStats serves the background upload pipeline stats as JSON.
func (s *s3) handleGetUploadStats(jc jape.Context) {
	stats, err := s.backend.UploadStats(jc.Request.Context())
	if jc.Check("failed to get upload stats", err) != nil {
		return
	}
	jc.Encode(stats)
}

// handleBackupSQLite3 creates a backup of the SQLite3 database at the path
// provided in the request body. The backup is a consistent snapshot even if
// the database is being written to concurrently.
func (s *s3) handleBackupSQLite3(jc jape.Context) {
	var req BackupSQLite3Request
	if jc.Decode(&req) != nil {
		return
	} else if req.Path == "" {
		jc.Error(fmt.Errorf("path must not be empty"), http.StatusBadRequest)
		return
	} else if !filepath.IsAbs(req.Path) {
		jc.Error(fmt.Errorf("path must be absolute: %q", req.Path), http.StatusBadRequest)
		return
	} else if _, err := os.Stat(req.Path); err == nil {
		jc.Error(fmt.Errorf("destination already exists: %q", req.Path), http.StatusBadRequest)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		jc.Error(fmt.Errorf("failed to stat destination: %w", err), http.StatusBadRequest)
		return
	}
	jc.Check("failed to backup database", s.backend.BackupSQLite3(jc.Request.Context(), req.Path))
}
