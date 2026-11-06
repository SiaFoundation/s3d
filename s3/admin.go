package s3

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"github.com/robfig/cron/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

// ErrSnapshotNotFound is returned when deleting a snapshot that does not exist.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// Snapshot describes a database snapshot uploaded to Sia. It is returned by the
// [POST] /snapshots endpoint.
type Snapshot struct {
	ID          int64         `json:"-"`
	CreatedAt   time.Time     `json:"createdAt"`
	SiaObjectID types.Hash256 `json:"siaObjectID"`
	ObjectCount int64         `json:"objectCount"`
	Scheduled   bool          `json:"scheduled"`
}

// SnapshotList describes the recorded database snapshots and whether the
// instance has finished syncing with the Sia network. It is returned by the
// [GET] /snapshots endpoint.
type SnapshotList struct {
	Synced    bool       `json:"synced"`
	Snapshots []Snapshot `json:"snapshots"`
}

// SnapshotSchedule defines when snapshots are taken and how many calendar days,
// weeks and months of scheduled snapshots are kept. An empty expression disables
// scheduled snapshots.
type SnapshotSchedule struct {
	Expression  string    `json:"expression"`
	Since       time.Time `json:"since"`
	KeepDaily   int       `json:"keepDaily"`
	KeepWeekly  int       `json:"keepWeekly"`
	KeepMonthly int       `json:"keepMonthly"`
}

// Validate rejects a schedule the snapshot loop cannot apply.
func (sched SnapshotSchedule) Validate() error {
	if sched.KeepDaily < 0 || sched.KeepWeekly < 0 || sched.KeepMonthly < 0 {
		return errors.New("snapshot retention counts must not be negative")
	} else if sched.Expression == "" {
		if sched.KeepDaily != 0 || sched.KeepWeekly != 0 || sched.KeepMonthly != 0 {
			return errors.New("snapshot retention requires a snapshot schedule")
		}
		return nil
	} else if sched.KeepDaily == 0 && sched.KeepWeekly == 0 && sched.KeepMonthly == 0 {
		return errors.New("a snapshot schedule requires at least one positive retention count")
	} else if sched.Since.IsZero() {
		return errors.New("a snapshot schedule must record the time it runs from")
	}
	_, err := ParseSnapshotSchedule(sched.Expression)
	return err
}

// ParseSnapshotSchedule parses a standard five field cron expression or one of
// the @hourly, @daily, @weekly, @monthly and @yearly descriptors. The schedule
// fires in the location of the time it is evaluated against.
func ParseSnapshotSchedule(expr string) (cron.Schedule, error) {
	// the parser panics on a bare time zone prefix and accepts a zero @every
	// interval, which would fire on every tick
	if expr = strings.TrimSpace(expr); strings.Contains(expr, "=") || strings.HasPrefix(expr, "@every") {
		return nil, fmt.Errorf("invalid snapshot schedule %q, expected a cron expression or a descriptor like @daily", expr)
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot schedule %q: %w", expr, err)
	} else if sched.Next(time.Now()).IsZero() {
		return nil, fmt.Errorf("invalid snapshot schedule %q, it never fires", expr)
	}
	return sched, nil
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

// handleListSnapshots lists the recorded database snapshots.
func (s *s3) handleListSnapshots(jc jape.Context) {
	list, err := s.backend.ListSnapshots(jc.Request.Context())
	if jc.Check("failed to list snapshots", err) != nil {
		return
	}
	jc.Encode(list)
}

// handleDeleteSnapshot unpins a snapshot's Sia object and removes its record,
// releasing the orphaned objects it was withholding.
func (s *s3) handleDeleteSnapshot(jc jape.Context) {
	var objectID types.Hash256
	if jc.DecodeParam("objectID", &objectID) != nil {
		return
	}

	err := s.backend.DeleteSnapshot(jc.Request.Context(), objectID)
	if errors.Is(err, ErrSnapshotNotFound) {
		jc.Error(ErrSnapshotNotFound, http.StatusNotFound)
		return
	}
	jc.Check("failed to delete snapshot", err)
}

// handleGetSnapshotSchedule returns the snapshot schedule.
func (s *s3) handleGetSnapshotSchedule(jc jape.Context) {
	sched, err := s.backend.SnapshotSchedule(jc.Request.Context())
	if jc.Check("failed to load snapshot schedule", err) != nil {
		return
	}
	jc.Encode(sched)
}

// handleUpdateSnapshotSchedule replaces the snapshot schedule, which runs from
// the time of the request.
func (s *s3) handleUpdateSnapshotSchedule(jc jape.Context) {
	var sched SnapshotSchedule
	if jc.Decode(&sched) != nil {
		return
	}
	sched.Expression = strings.TrimSpace(sched.Expression)
	sched.Since = time.Now()
	if err := sched.Validate(); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}
	jc.Check("failed to set snapshot schedule", s.backend.UpdateSnapshotSchedule(jc.Request.Context(), sched))
}

// handleDeleteSnapshotSchedule disables scheduled snapshots, keeping the
// existing ones.
func (s *s3) handleDeleteSnapshotSchedule(jc jape.Context) {
	jc.Check("failed to disable scheduled snapshots", s.backend.UpdateSnapshotSchedule(jc.Request.Context(), SnapshotSchedule{}))
}
