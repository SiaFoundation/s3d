package objects

import (
	"time"

	"go.sia.tech/core/types"
)

// Object represents a stored object with its metadata.
type Object struct {
	Name       string
	ID         types.Hash256
	ContentMD5 [16]byte
	Meta       map[string]string
	Size       int64
	UpdatedAt  time.Time
}

// Part represents a single part of a multipart upload.
type Part struct {
	PartNumber int
	Filename   string
	Size       int64
	ContentMD5 [16]byte
}
