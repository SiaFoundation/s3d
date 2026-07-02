package sqlite

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"lukechampine.com/frand"
)

// nullVersion is the version ID of an object written to a suspended or
// unversioned bucket. Being non-random, it never collides with an ID from
// [newDBVersion].
const nullVersion = ""

// newDBVersion returns a random, opaque object version ID.
func newDBVersion() string {
	return hex.EncodeToString(frand.Bytes(32))
}

// reportedWriteVersion is the version ID to report for a write; only an enabled
// bucket reports one.
func reportedWriteVersion(status, v string) string {
	if status != s3.VersioningStatusEnabled {
		return ""
	}
	return s3.FormatVersion(v)
}

// reportedDeleteVersion is the version ID to report for a delete. A suspended
// bucket reports the null version (it leaves a null delete marker); an
// unversioned bucket reports nothing.
func reportedDeleteVersion(status, v string) string {
	if status == "" {
		return ""
	}
	return s3.FormatVersion(v)
}

// objectMutationResult carries every effect of a write or delete transition the
// caller must observe.
type objectMutationResult struct {
	// dbVersionID is the version of the row created or removed.
	dbVersionID string
	// reportVersionID is the wire-encoded version to return to the client, empty
	// when the bucket reports none.
	reportVersionID string
	// deleteMarker reports whether a delete marker was created or removed.
	deleteMarker bool
	// orphanFile is the no-longer-referenced upload file to remove; the zero
	// value means nothing was orphaned.
	orphanFile objects.OrphanedFile
}

// putObject inserts a new object at (bid, name): an enabled bucket gets a
// fresh version and retains existing ones; otherwise the null version is
// replaced. The replaced null version's data is orphaned only after the new row
// exists, so data shared with it (a dedup or self-copy) is retained.
func putObject(tx *txn, bid int64, name string, status string, contentMD5 [16]byte, meta map[string]string, length int64, partsCount int32, fileName *string, siaObject *objects.SiaObject) (objectMutationResult, error) {
	if meta == nil {
		meta = make(map[string]string) // force '{}' instead of 'null' in JSON
	}

	// an enabled bucket gets a fresh ID; otherwise remove any existing null
	// version in place and orphan it below once the new row exists.
	var (
		version     string
		old         deletedRow
		replacedOld bool
		err         error
	)
	if status == s3.VersioningStatusEnabled {
		version = newDBVersion()
	} else if old, replacedOld, err = deleteObject(tx, bid, name, nullVersion); err != nil {
		return objectMutationResult{}, fmt.Errorf("failed to delete null version: %w", err)
	}

	seq, err := claimCurrentSeq(tx, bid, name)
	if err != nil {
		return objectMutationResult{}, fmt.Errorf("failed to claim current sequence: %w", err)
	}

	var id *sqlHash256
	var sealed *sqlSiaObject
	if siaObject != nil {
		id = (*sqlHash256)(&siaObject.ID)
		sealed = (*sqlSiaObject)(&siaObject.Sealed)
	}

	if _, err := tx.Exec(`
		INSERT INTO objects (bucket_id, name, version_id, seq, is_delete_marker, is_latest, sia_object_id, content_md5, metadata, size, parts_count, updated_at, filename, sia_object)
		VALUES ($1, $2, $3, $4, FALSE, TRUE, $5, $6, $7, $8, $9, $10, $11, $12)
	`, bid, name, version, seq, id, sqlMD5(contentMD5),
		sqlMetaJSON(meta), length, partsCount, sqlTime(time.Now()),
		fileName, sealed); err != nil {
		return objectMutationResult{}, fmt.Errorf("failed to insert object: %w", err)
	}

	// a sia_object_id counts as uploaded even when a filename is kept as backup;
	// only a filename without a sia_object_id is pending.
	if fileName != nil && siaObject == nil {
		if err := addPendingObject(tx, length); err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to track pending object: %w", err)
		}
	}
	if siaObject != nil {
		if err := addUploadedObject(tx, length); err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to track uploaded object: %w", err)
		}
	}

	var orphan objects.OrphanedFile
	if replacedOld {
		if orphan, err = orphanDeleted(tx, old); err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to orphan replaced version: %w", err)
		}
	}

	return objectMutationResult{
		dbVersionID:     version,
		reportVersionID: reportedWriteVersion(status, version),
		orphanFile:      orphan,
	}, nil
}

// deleteCurrentObject applies a versioning-aware delete to the current object
// of (bid, name): an enabled bucket inserts a delete marker, a suspended bucket
// replaces the null version with a null delete marker, and an unversioned
// bucket deletes the null version outright. Preconditions are enforced against
// the current version (the removed null version when unversioned). Returns
// sql.ErrNoRows only for an unversioned bucket with no null version.
func deleteCurrentObject(tx *txn, bid int64, name string, status string, objectID s3.ObjectID) (objectMutationResult, error) {
	switch status {
	case s3.VersioningStatusEnabled:
		if err := checkObjectPreconditions(tx, bid, name, objectID); err != nil {
			return objectMutationResult{}, err
		}
		marker := newDBVersion()
		if err := insertDeleteMarker(tx, bid, name, marker); err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to insert delete marker: %w", err)
		}
		return objectMutationResult{
			dbVersionID:     marker,
			reportVersionID: reportedDeleteVersion(status, marker),
			deleteMarker:    true,
		}, nil

	case s3.VersioningStatusSuspended:
		// preconditions match the current version, not the null version being
		// replaced (which may not be current).
		if err := checkObjectPreconditions(tx, bid, name, objectID); err != nil {
			return objectMutationResult{}, err
		}
		var orphan objects.OrphanedFile
		row, found, err := deleteObject(tx, bid, name, nullVersion)
		if err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to delete null version: %w", err)
		}
		if found {
			if orphan, err = orphanDeleted(tx, row); err != nil {
				return objectMutationResult{}, fmt.Errorf("failed to orphan null version: %w", err)
			}
		}
		if err := insertDeleteMarker(tx, bid, name, nullVersion); err != nil {
			return objectMutationResult{}, fmt.Errorf("failed to insert null delete marker: %w", err)
		}
		return objectMutationResult{
			dbVersionID:     nullVersion,
			reportVersionID: reportedDeleteVersion(status, nullVersion),
			deleteMarker:    true,
			orphanFile:      orphan,
		}, nil

	default: // unversioned: permanently delete the null version
		res, err := deleteSpecificVersion(tx, bid, name, nullVersion, objectID)
		if err != nil {
			return objectMutationResult{}, err
		}
		res.reportVersionID = reportedDeleteVersion(status, res.dbVersionID)
		return res, nil
	}
}

// deleteSpecificVersion permanently deletes the (bid, name, version) row after
// checking objectID's preconditions, then orphans its backing data. Returns
// sql.ErrNoRows if no such row exists.
func deleteSpecificVersion(tx *txn, bid int64, name string, version string, objectID s3.ObjectID) (objectMutationResult, error) {
	row, found, err := deleteObject(tx, bid, name, version)
	if err != nil {
		return objectMutationResult{}, fmt.Errorf("failed to delete version: %w", err)
	} else if !found {
		return objectMutationResult{}, sql.ErrNoRows
	}
	if err := matchPreconditions(objectID, row.contentMD5, row.size, row.updatedAt); err != nil {
		return objectMutationResult{}, err
	}
	orphan, err := orphanDeleted(tx, row)
	if err != nil {
		return objectMutationResult{}, fmt.Errorf("failed to orphan deleted version: %w", err)
	}
	return objectMutationResult{
		dbVersionID:     version,
		reportVersionID: s3.FormatVersion(version),
		deleteMarker:    row.isDeleteMarker,
		orphanFile:      orphan,
	}, nil
}

// insertDeleteMarker inserts a delete marker row for (bid, name) with a fresh
// sequence so it becomes the current version. Delete markers carry no data.
func insertDeleteMarker(tx *txn, bid int64, name string, version string) error {
	seq, err := claimCurrentSeq(tx, bid, name)
	if err != nil {
		return fmt.Errorf("failed to claim current sequence: %w", err)
	}
	var zero [16]byte
	if _, err := tx.Exec(`
		INSERT INTO objects (bucket_id, name, version_id, seq, is_delete_marker, is_latest, content_md5, metadata, size, parts_count, updated_at)
		VALUES ($1, $2, $3, $4, TRUE, TRUE, $5, '{}', 0, 0, $6)
	`, bid, name, version, seq, sqlMD5(zero), sqlTime(time.Now())); err != nil {
		return fmt.Errorf("failed to insert delete marker: %w", err)
	}
	return nil
}
