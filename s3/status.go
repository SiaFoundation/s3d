package s3

import (
	"encoding/json"
	"net/http"
)

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

func (s *s3) handleUploadStats(w http.ResponseWriter, r *http.Request) error {
	stats, err := s.backend.UploadStats(r.Context())
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(stats)
}
