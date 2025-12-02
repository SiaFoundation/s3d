package objects

import (
	"time"

	"go.sia.tech/core/types"
)

// Object represents a stored object with its metadata.
type Object struct {
	ID         types.Hash256
	ContentMD5 [16]byte
	Meta       map[string]string
	Size       int64
	UpdatedAt  time.Time
}
