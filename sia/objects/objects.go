package objects

import (
	"time"

	"go.sia.tech/core/types"
)

// Object represents a stored object with its metadata.
type Object struct {
	ID           types.Hash256
	Name         string
	PartsCount   int32
	Meta         map[string]string
	Offset       int64
	Length       int64
	ContentMD5   [16]byte
	LastModified time.Time
}

// Part represents a single part of a multipart upload.
type Part struct {
	PartNumber int
	Filename   string
	Size       int64
	ContentMD5 [16]byte
}
