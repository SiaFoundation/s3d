package s3

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/SiaFoundation/s3d/internal/prometheus"
	"github.com/SiaFoundation/s3d/s3/auth"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

var (
	// ErrSnapshotNotFound is returned when deleting a snapshot that does not exist.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrUserAlreadyExists is returned when creating a user that already exists.
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrReservedUserName is returned when creating a user whose name bucket
	// policies use for anonymous callers.
	ErrReservedUserName = errors.New("user name is reserved")

	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrUserHasBuckets is returned when deleting a user that still owns buckets.
	ErrUserHasBuckets = errors.New("user still owns buckets")

	// ErrAccessKeyAlreadyExists is returned when creating an access key that
	// already exists.
	ErrAccessKeyAlreadyExists = errors.New("access key already exists")

	// ErrAccessKeyNotFound is returned when an access key does not exist.
	ErrAccessKeyNotFound = errors.New("access key not found")
)

// AdminBackend defines the interface the admin API is served from.
type AdminBackend interface {
	// UploadStats returns statistics about the background upload pipeline.
	UploadStats(ctx context.Context) (UploadStats, error)

	// FlushObjects uploads all pending objects to Sia regardless of padding,
	// rather than waiting for the background pipeline to batch them into
	// efficiently packed slabs. It blocks until the uploads complete.
	FlushObjects(ctx context.Context) error

	// CreateSnapshot backs up the database, uploads it to Sia as a tagged
	// snapshot object, and records the object ID.
	CreateSnapshot(ctx context.Context) (Snapshot, error)

	// ListSnapshots returns the recorded database snapshots and whether the
	// last metadata sync drained the network's event stream.
	ListSnapshots(ctx context.Context) (SnapshotList, error)

	// DeleteSnapshot unpins a snapshot's Sia object from the network and
	// removes its record, releasing the orphaned objects it was withholding.
	//
	// - If no such snapshot exists, [ErrSnapshotNotFound] must be returned.
	DeleteSnapshot(ctx context.Context, objectID types.Hash256) error

	// CreateUser creates a user with the given name.
	//
	// - If the user already exists, [ErrUserAlreadyExists] must be returned.
	CreateUser(ctx context.Context, name string) error

	// DeleteUser deletes the user with the given name and their access keys.
	//
	// - If the user does not exist, [ErrUserNotFound] must be returned.
	//
	// - If the user still owns buckets, [ErrUserHasBuckets] must be returned.
	DeleteUser(ctx context.Context, name string) error

	// ListUsers returns the names of all users.
	ListUsers(ctx context.Context) ([]string, error)

	// CreateAccessKey creates an access key pair for the given user.
	//
	// - If the user does not exist, [ErrUserNotFound] must be returned.
	//
	// - If the access key ID is taken, [ErrAccessKeyAlreadyExists] must be
	//   returned.
	CreateAccessKey(ctx context.Context, userName, accessKeyID, secretKey string) error

	// DeleteAccessKey deletes the access key with the given ID.
	//
	// - If the access key does not exist, [ErrAccessKeyNotFound] must be
	//   returned.
	DeleteAccessKey(ctx context.Context, accessKeyID string) error

	// ListAccessKeys returns the access keys of the given user, or of every
	// user when userName is nil.
	ListAccessKeys(ctx context.Context, userName *string) ([]AccessKey, error)
}

// Snapshot describes a database snapshot uploaded to Sia. It is returned by the
// [POST] /snapshots endpoint.
type Snapshot struct {
	ID          int64         `json:"-"`
	CreatedAt   time.Time     `json:"createdAt"`
	SiaObjectID types.Hash256 `json:"siaObjectID"`
	ObjectCount int64         `json:"objectCount"`
}

// SnapshotList describes the recorded database snapshots and whether the
// instance has finished syncing with the Sia network. It is returned by the
// [GET] /snapshots endpoint.
type SnapshotList struct {
	Synced    bool       `json:"synced"`
	Snapshots []Snapshot `json:"snapshots"`
}

// AccessKey describes an access key pair and the user it belongs to. The secret
// is only returned when the key is created.
type AccessKey struct {
	AccessKeyID string `json:"accessKeyID"`
	SecretKey   string `json:"secretKey,omitempty"`
	UserName    string `json:"userName"`
}

// CreateUserRequest is the request body of the [POST] /users endpoint.
type CreateUserRequest struct {
	Name string `json:"name"`
}

// CreateAccessKeyRequest is the request body of the [POST] /keys endpoint. The
// pair is generated when both AccessKeyID and SecretKey are empty.
type CreateAccessKeyRequest struct {
	UserName    string `json:"userName"`
	AccessKeyID string `json:"accessKeyID"`
	SecretKey   string `json:"secretKey"`
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
	stats, err := s.admin.UploadStats(jc.Request.Context())
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
	stats, err := s.admin.UploadStats(jc.Request.Context())
	if jc.Check("failed to get upload stats", err) != nil {
		return
	}
	jc.Encode(stats)
}

// handleFlushObjects flushes all pending objects to Sia via AdminBackend.FlushObjects.
func (s *s3) handleFlushObjects(jc jape.Context) {
	jc.Check("failed to flush objects", s.admin.FlushObjects(jc.Request.Context()))
}

// handleCreateSnapshot backs up the database, uploads it to Sia as a tagged
// snapshot object, and records the object ID.
func (s *s3) handleCreateSnapshot(jc jape.Context) {
	snapshot, err := s.admin.CreateSnapshot(jc.Request.Context())
	if jc.Check("failed to create snapshot", err) != nil {
		return
	}
	jc.Encode(snapshot)
}

// handleListSnapshots lists the recorded database snapshots.
func (s *s3) handleListSnapshots(jc jape.Context) {
	list, err := s.admin.ListSnapshots(jc.Request.Context())
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

	err := s.admin.DeleteSnapshot(jc.Request.Context(), objectID)
	if errors.Is(err, ErrSnapshotNotFound) {
		jc.Error(ErrSnapshotNotFound, http.StatusNotFound)
		return
	}
	jc.Check("failed to delete snapshot", err)
}

// handleListUsers lists the names of all users.
func (s *s3) handleListUsers(jc jape.Context) {
	users, err := s.admin.ListUsers(jc.Request.Context())
	if jc.Check("failed to list users", err) != nil {
		return
	}
	jc.Encode(users)
}

// handleCreateUser creates a user.
func (s *s3) handleCreateUser(jc jape.Context) {
	var req CreateUserRequest
	if jc.Decode(&req) != nil {
		return
	} else if req.Name == "" {
		jc.Error(errors.New("name is required"), http.StatusBadRequest)
		return
	}

	err := s.admin.CreateUser(jc.Request.Context(), req.Name)
	if errors.Is(err, ErrUserAlreadyExists) {
		jc.Error(err, http.StatusConflict)
		return
	} else if errors.Is(err, ErrReservedUserName) {
		jc.Error(err, http.StatusBadRequest)
		return
	}
	jc.Check("failed to create user", err)
}

// handleDeleteUser deletes a user and their access keys.
func (s *s3) handleDeleteUser(jc jape.Context) {
	err := s.admin.DeleteUser(jc.Request.Context(), jc.PathParam("name"))
	if errors.Is(err, ErrUserNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, ErrUserHasBuckets) {
		jc.Error(err, http.StatusConflict)
		return
	}
	jc.Check("failed to delete user", err)
}

// handleListAccessKeys lists access keys without their secrets.
func (s *s3) handleListAccessKeys(jc jape.Context) {
	var filter string
	if jc.DecodeForm("userName", &filter) != nil {
		return
	}
	var userName *string
	if filter != "" {
		userName = &filter
	}

	keys, err := s.admin.ListAccessKeys(jc.Request.Context(), userName)
	if jc.Check("failed to list access keys", err) != nil {
		return
	}
	for i := range keys {
		keys[i].SecretKey = ""
	}
	jc.Encode(keys)
}

// handleCreateAccessKey creates an access key pair for a user.
func (s *s3) handleCreateAccessKey(jc jape.Context) {
	var req CreateAccessKeyRequest
	if jc.Decode(&req) != nil {
		return
	} else if req.UserName == "" {
		jc.Error(errors.New("userName is required"), http.StatusBadRequest)
		return
	}

	key := AccessKey{AccessKeyID: req.AccessKeyID, SecretKey: req.SecretKey, UserName: req.UserName}
	if key.AccessKeyID == "" && key.SecretKey == "" {
		key.AccessKeyID, key.SecretKey = auth.GenerateAccessKey()
	} else if err := auth.ValidateAccessKey(key.AccessKeyID, key.SecretKey); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	err := s.admin.CreateAccessKey(jc.Request.Context(), key.UserName, key.AccessKeyID, key.SecretKey)
	if errors.Is(err, ErrUserNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, ErrAccessKeyAlreadyExists) {
		jc.Error(err, http.StatusConflict)
		return
	} else if jc.Check("failed to create access key", err) != nil {
		return
	}
	jc.Encode(key)
}

// handleDeleteAccessKey deletes an access key.
func (s *s3) handleDeleteAccessKey(jc jape.Context) {
	err := s.admin.DeleteAccessKey(jc.Request.Context(), jc.PathParam("accessKeyID"))
	if errors.Is(err, ErrAccessKeyNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("failed to delete access key", err)
}
