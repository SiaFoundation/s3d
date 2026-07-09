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

	// ErrSnapshotNotFound is returned by DeleteSnapshot when no snapshot with
	// the given id exists.
	ErrSnapshotNotFound = errors.New("snapshot not found")
)

// Object represents a stored object with its metadata.
type Object struct {
	SiaObject *SiaObject
	FileName  *string
	Name      string
	VersionID string // "" represents the null version
	// Versioned reports whether the bucket has versioning configured
	// (Enabled or Suspended).
	Versioned      bool
	IsDeleteMarker bool
	PartsCount     int32
	Meta           map[string]string
	Offset         int64
	Length         int64
	Size           int64
	ContentMD5     [16]byte
	LastModified   time.Time
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
	VersionID  string
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

// UnpinnedObject identifies a Sia object that has been uploaded but not yet
// pinned, along with the deadline before which it must be pinned.
type UnpinnedObject struct {
	SiaObject SiaObject
	PinBefore time.Time
}

// OrphanedFile identifies an on-disk upload file that is no longer referenced
// by any object row.
type OrphanedFile struct {
	Filename string
	Size     int64
}

// SnapshotType is the discriminator written to a snapshot object's metadata so
// recovery can identify snapshots among all account objects.
const SnapshotType = "s3d-snapshot"

// SnapshotEncodingGzip is the encoding recorded for snapshots whose backup is
// gzip compressed before upload.
const SnapshotEncodingGzip = "gzip"

// SnapshotMetadata is attached to a snapshot's Sia object. It lets recovery
// find snapshots and refuse ones it cannot restore.
type SnapshotMetadata struct {
	Type        string    `json:"type"`
	CreatedAt   time.Time `json:"createdAt"`
	DBVersion   int64     `json:"dbVersion"`
	Encoding    string    `json:"encoding"`
	ObjectCount int64     `json:"objectCount"`
	S3DVersion  string    `json:"s3dVersion"`
}
