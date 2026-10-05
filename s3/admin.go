package s3

import (
	"time"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

// Snapshot describes a database backup uploaded to Sia. It is returned by the
// [POST] /snapshots endpoint.
type Snapshot struct {
	ID          int64         `json:"id"`
	CreatedAt   time.Time     `json:"createdAt"`
	SiaObjectID types.Hash256 `json:"siaObjectID"`
	ObjectCount int64         `json:"objectCount"`
}

// ActiveUpload describes an upload group being streamed from the local buffer
// to Sia.
type ActiveUpload struct {
	Label      string `json:"label"`
	Objects    int64  `json:"objects"`
	Size       int64  `json:"size"`
	Sent       int64  `json:"sent"`
	Finalizing bool   `json:"finalizing"`
}

// TransferStats contains live transfer activity. Byte totals cover the current
// process and rates cover a five second window.
type TransferStats struct {
	IngressActive int64 `json:"ingressActive"`
	IngressBytes  int64 `json:"ingressBytes"`
	IngressRate   int64 `json:"ingressRate"`
	UploadBytes   int64 `json:"uploadBytes"`
	UploadRate    int64 `json:"uploadRate"`

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
	BufferUsed       int64         `json:"bufferUsed"`
	BufferLimit      int64         `json:"bufferLimit"`
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
			Value: float64(s.BufferUsed),
		},
		{
			Name:  "s3d_upload_buffer_limit_bytes",
			Value: float64(s.BufferLimit),
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
			Value: float64(len(s.Transfer.ActiveUploads)),
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

// handleCreateSnapshot backs up the database, uploads it to Sia as a tagged
// snapshot object, and records the object ID.
func (s *s3) handleCreateSnapshot(jc jape.Context) {
	snapshot, err := s.backend.CreateSnapshot(jc.Request.Context())
	if jc.Check("failed to create snapshot", err) != nil {
		return
	}
	jc.Encode(snapshot)
}
