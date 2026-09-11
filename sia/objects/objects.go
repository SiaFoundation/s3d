package objects

import (
	"errors"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

const (
	// SnapshotType is the discriminator written to a snapshot object's metadata
	// so recovery can identify snapshots among all account objects.
	SnapshotType = "s3d-snapshot"

	// SnapshotEncodingGzip is the encoding recorded for snapshots whose backup
	// is gzip compressed before upload.
	SnapshotEncodingGzip = "gzip"

	// maxSnapshotGeneration caps the generation accepted from snapshot
	// metadata. Adopting a snapshot raises the local counter to its generation
	// and later bumps it further, so a value near the int64 limit would
	// overflow the counter into a SQLite REAL and wedge the sync loop on the
	// event.
	maxSnapshotGeneration = 1 << 62 // 4.6e18
)

var (
	// ErrObjectModified is returned by MarkObjectUploaded when the object's
	// content MD5 no longer matches the expected value.
	ErrObjectModified = errors.New("object was modified")

	// ErrObjectNotFound is returned by MarkObjectUploaded when the pending
	// object does not exist.
	ErrObjectNotFound = errors.New("object not found")

	// ErrSnapshotNotFound is returned by snapshot operations when no snapshot
	// matches the addressed id and expected state.
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

// ETag returns the object's S3 ETag, which encodes its part count for a
// multipart object.
func (o *Object) ETag() string {
	return s3.FormatETag(o.ContentMD5[:], int(o.PartsCount))
}

// Attrs returns the attributes preconditions are matched against.
func (o *Object) Attrs() s3.ObjectAttrs {
	return s3.ObjectAttrs{
		ETag:           o.ETag(),
		Size:           o.Size,
		LastModified:   o.LastModified,
		IsDeleteMarker: o.IsDeleteMarker,
	}
}

// PutOptions are the options for storing an object's metadata.
type PutOptions struct {
	ContentMD5 [16]byte
	Meta       map[string]string
	Length     int64

	// FileName is the pending upload file backing the object, or nil when the
	// object has no data on disk.
	FileName *string

	Preconditions s3.ObjectPreconditions
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

// PinningSnapshot identifies a snapshot awaiting confirmation that its backup
// object reached the indexer.
type PinningSnapshot struct {
	ID       int64
	ObjectID types.Hash256
}

// DeletingSnapshot identifies a snapshot marked for deletion and when it was
// marked.
type DeletingSnapshot struct {
	ObjectID types.Hash256
	Since    time.Time
}

// SnapshotMetadata is attached to a snapshot's Sia object. It lets recovery
// find snapshots and refuse ones it cannot restore.
type SnapshotMetadata struct {
	Type        string    `json:"type"`
	CreatedAt   time.Time `json:"createdAt"`
	DBVersion   int64     `json:"dbVersion"`
	Encoding    string    `json:"encoding"`
	Generation  int64     `json:"generation"`
	ObjectCount int64     `json:"objectCount"`
	S3DVersion  string    `json:"s3dVersion"`
}

// Validate rejects metadata no version of s3d can have written. Unknown
// encodings and database versions pass so a snapshot from a newer s3d is still
// recognized. S3DVersion is unchecked, it is empty in unstamped builds.
func (m SnapshotMetadata) Validate() error {
	switch {
	case m.Type != SnapshotType:
		return fmt.Errorf("unexpected type %q", m.Type)
	case m.CreatedAt.IsZero():
		return errors.New("missing creation time")
	case m.Encoding == "":
		return errors.New("missing encoding")
	case m.DBVersion <= 0:
		return fmt.Errorf("invalid database version %d", m.DBVersion)
	case m.Generation <= 0 || m.Generation > maxSnapshotGeneration:
		// the generation is bumped before the snapshot row is inserted, so it
		// is never 0, and no counter ever gets near the upper cap
		return fmt.Errorf("invalid generation %d", m.Generation)
	case m.ObjectCount < 0:
		return fmt.Errorf("invalid object count %d", m.ObjectCount)
	}
	return nil
}
