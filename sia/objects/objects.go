package objects

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

var (
	// ErrObjectModified is returned by MarkObjectUploaded when the object's
	// content MD5 no longer matches the expected value.
	ErrObjectModified = errors.New("object was modified")

	// ErrObjectNotFound is returned by MarkObjectUploaded when the pending
	// object does not exist.
	ErrObjectNotFound = errors.New("object not found")
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
	Sealed sdk.SealedObject
}

// IsMultipart returns true if the object is a multipart upload (i.e. has parts).
func (o *Object) IsMultipart() bool {
	return o.PartsCount > 0
}

// ObjectForUpload contains the fields needed to upload an object.
type ObjectForUpload struct {
	Bucket     string
	Name       string
	Filename   string
	ContentMD5 [16]byte
	Length     int64
	Multipart  bool
}

// Part represents a single part of a multipart upload.
type Part struct {
	PartNumber int
	Filename   string
	Size       int64
	ContentMD5 [16]byte
}
