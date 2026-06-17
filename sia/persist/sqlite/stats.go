package sqlite

import (
	"fmt"

	"github.com/SiaFoundation/s3d/s3"
)

// stat names tracked incrementally in the stats table. Each corresponds to a
// field returned by UploadStats.
const (
	statPendingObjects   = "pending_objects"
	statPendingSize      = "pending_size"
	statUploadedObjects  = "uploaded_objects"
	statUploadedSize     = "uploaded_size"
	statUnpinnedObjects  = "unpinned_objects"
	statOrphanedObjects  = "orphaned_objects"
	statMultipartUploads = "multipart_uploads"
)

// incrementStat adjusts the named stat counter by delta, which may be negative.
func incrementStat(tx *txn, stat string, delta int64) error {
	if res, err := tx.Exec(`UPDATE stats SET stat_value = stat_value + $1 WHERE stat = $2`, delta, stat); err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("unknown stat %q", stat)
	}
	return nil
}

// addPendingObject records that an object of the given size is now pending
// upload to Sia.
func addPendingObject(tx *txn, size int64) error {
	if err := incrementStat(tx, statPendingObjects, 1); err != nil {
		return err
	}
	return incrementStat(tx, statPendingSize, size)
}

// removePendingObject records that a pending object of the given size is no
// longer awaiting upload to Sia.
func removePendingObject(tx *txn, size int64) error {
	if err := incrementStat(tx, statPendingObjects, -1); err != nil {
		return err
	}
	return incrementStat(tx, statPendingSize, -size)
}

// addUploadedObject records that an object of the given size has been uploaded
// to Sia.
func addUploadedObject(tx *txn, size int64) error {
	if err := incrementStat(tx, statUploadedObjects, 1); err != nil {
		return err
	}
	return incrementStat(tx, statUploadedSize, size)
}

// removeUploadedObject records that an uploaded object of the given size is no
// longer stored on Sia.
func removeUploadedObject(tx *txn, size int64) error {
	if err := incrementStat(tx, statUploadedObjects, -1); err != nil {
		return err
	}
	return incrementStat(tx, statUploadedSize, -size)
}

// UploadStats returns statistics about the background upload pipeline, read
// from the incrementally maintained stats table.
func (s *Store) UploadStats() (stats s3.UploadStats, err error) {
	err = s.transaction(func(tx *txn) error {
		stats = s3.UploadStats{} // reset per transaction attempt
		rows, err := tx.Query(`SELECT stat, stat_value FROM stats`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var stat string
			var value int64
			if err := rows.Scan(&stat, &value); err != nil {
				return err
			}
			switch stat {
			case statPendingObjects:
				stats.PendingObjects = value
			case statPendingSize:
				stats.PendingSize = value
			case statUploadedObjects:
				stats.UploadedObjects = value
			case statUploadedSize:
				stats.UploadedSize = value
			case statUnpinnedObjects:
				stats.UnpinnedObjects = value
			case statOrphanedObjects:
				stats.OrphanedObjects = value
			case statMultipartUploads:
				stats.MultipartUploads = value
			}
		}
		return rows.Err()
	})
	return
}
