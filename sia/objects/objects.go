package objects

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// Object represents a stored object with its metadata.
type Object struct {
	SiaObject    *SiaObject
	FileName     *string
	Name         string
	PartsCount   int32
	Meta         map[string]string
	Offset       int64
	Length       int64
	ContentMD5   [16]byte
	LastModified time.Time
}

// SiaObject pairs a Sia object ID with its sealed metadata.
type SiaObject struct {
	ID     types.Hash256
	Sealed slabs.SealedObject
}

// Part represents a single part of a multipart upload.
type Part struct {
	PartNumber int
	Filename   string
	Size       int64
	ContentMD5 [16]byte
}
