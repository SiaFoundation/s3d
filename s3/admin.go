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

// ActiveUpload describes an upload group being streamed from the local buffer
// to Sia.
type ActiveUpload struct {
	Label      string `json:"label"`
	Objects    int    `json:"objects"`
	Size       int64  `json:"size"`
	Sent       int64  `json:"sent"`
	Finalizing bool   `json:"finalizing"`
}

// TransferStats contains live transfer activity. Ingress covers request bodies
// read from S3 clients, upload covers buffered data handed to the Sia uploader.
// Byte totals cover the current process and rates cover the last few seconds.
type TransferStats struct {
	IngressActive int64          `json:"ingressActive"`
	IngressBytes  int64          `json:"ingressBytes"`
	IngressRate   int64          `json:"ingressRate"`
	UploadActive  int64          `json:"uploadActive"`
	UploadBytes   int64          `json:"uploadBytes"`
	UploadRate    int64          `json:"uploadRate"`
	BufferUsed    int64          `json:"bufferUsed"`
	BufferLimit   int64          `json:"bufferLimit"`
	ActiveUploads []ActiveUpload `json:"activeUploads,omitempty"`
}

// UploadStats contains statistics about the background upload pipeline.
type UploadStats struct {
	PendingObjects   int64         `json:"pendingObjects"`
	PendingSize      int64         `json:"pendingSize"`
	UploadedObjects  int64         `json:"uploadedObjects"`
	UploadedSize     int64         `json:"uploadedSize"`
	UnpinnedObjects  int64         `json:"unpinnedObjects"`
	FailedUploads    int64         `json:"failedUploads"`
	OrphanedObjects  int64         `json:"orphanedObjects"`
	MultipartUploads int64         `json:"multipartUploads"`
	Transfer         TransferStats `json:"transfer"`
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
			Name:  "s3d_upload_unpinned_objects",
			Value: float64(s.UnpinnedObjects),
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
		{
			Name:  "s3d_upload_buffer_used_bytes",
			Value: float64(s.Transfer.BufferUsed),
		},
		{
			Name:  "s3d_upload_buffer_limit_bytes",
			Value: float64(s.Transfer.BufferLimit),
		},
		{
			Name:  "s3d_transfer_ingress_active",
			Value: float64(s.Transfer.IngressActive),
		},
		{
			Name:  "s3d_transfer_ingress_bytes_total",
			Value: float64(s.Transfer.IngressBytes),
		},
		{
			Name:  "s3d_transfer_upload_active",
			Value: float64(s.Transfer.UploadActive),
		},
		{
			Name:  "s3d_transfer_upload_bytes_total",
			Value: float64(s.Transfer.UploadBytes),
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

// handleFlushObjects flushes all pending objects to Sia via Backend.FlushObjects.
func (s *s3) handleFlushObjects(jc jape.Context) {
	jc.Check("failed to flush objects", s.backend.FlushObjects(jc.Request.Context()))
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
