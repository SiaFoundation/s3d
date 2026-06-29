package s3

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

// ErrSnapshotNotFound is returned when deleting a snapshot that does not exist.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// Snapshot describes a database backup uploaded to Sia. It is returned by the
// [POST] /snapshots endpoint.
type Snapshot struct {
	ID          int64         `json:"id"`
	CreatedAt   time.Time     `json:"createdAt"`
	SiaObjectID types.Hash256 `json:"siaObjectID"`
	ObjectCount int64         `json:"objectCount"`
}

// UploadStats contains statistics about the background upload pipeline.
type UploadStats struct {
	PendingObjects   int64 `json:"pendingObjects"`
	PendingSize      int64 `json:"pendingSize"`
	UploadedObjects  int64 `json:"uploadedObjects"`
	UploadedSize     int64 `json:"uploadedSize"`
	UnpinnedObjects  int64 `json:"unpinnedObjects"`
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

// handleListSnapshots lists the recorded database backups.
func (s *s3) handleListSnapshots(jc jape.Context) {
	snapshots, err := s.backend.ListSnapshots(jc.Request.Context())
	if jc.Check("failed to list snapshots", err) != nil {
		return
	}
	jc.Encode(snapshots)
}

// handleDeleteSnapshot deletes the snapshot with the id given in the path.
func (s *s3) handleDeleteSnapshot(jc jape.Context) {
	var id int64
	if jc.DecodeParam("id", &id) != nil {
		return
	} else if id <= 0 {
		jc.Error(fmt.Errorf("id must be positive: %d", id), http.StatusBadRequest)
		return
	}

	err := s.backend.DeleteSnapshot(jc.Request.Context(), id)
	if errors.Is(err, ErrSnapshotNotFound) {
		jc.Error(ErrSnapshotNotFound, http.StatusNotFound)
		return
	}
	jc.Check("failed to delete snapshot", err)
}
