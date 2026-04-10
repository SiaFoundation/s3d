package objects

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// ErrObjectModified is returned by FinalizeObject when the object was
// modified between reading and finalizing.
var ErrObjectModified = errors.New("object was modified")

// Object represents a stored object with its metadata.
type Object struct {
	ID           types.Hash256
	Name         string
	Bucket       string
	PartsCount   int32
	Meta         map[string]string
	Offset       int64
	Length       int64
	ContentMD5   [16]byte
	LastModified time.Time

	SiaObject slabs.SealedObject // sealed Sia object for downloads (must be unsealed before use)
	CachedAt  time.Time          // zero if not cached
	Filename  *string            // set if stored on disk
}

// PackedObject contains the fields needed to pack an object.
type PackedObject struct {
	Bucket   string
	Name     string
	Filename string
	Length   int64
}

// Part represents a single part of a multipart upload.
type Part struct {
	PartNumber int
	Filename   string
	Size       int64
	ContentMD5 [16]byte
}
