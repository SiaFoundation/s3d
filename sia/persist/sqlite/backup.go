package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"
)

// Backup writes a consistent, compacted copy of the database to destPath
// using VACUUM INTO on a dedicated readonly connection, so the store keeps
// serving reads and writes while the backup runs. Objects whose data is still
// buffered on disk and multipart uploads in progress are removed from the copy,
// which is verified with an integrity check before it is moved to destPath.
// Returns the number of objects in the copy.
func (s *Store) Backup(ctx context.Context, destPath string) (objectCount int64, err error) {
	// prevent overwriting the destination file
	if destPath == "" {
		return 0, errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return 0, errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("failed to stat destination file: %w", err)
	}

	// a dedicated readonly connection leaves the store's connection free, a
	// reader never blocks the writer in WAL mode
	src, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", s.path, time.Minute.Milliseconds()))
	if err != nil {
		return 0, fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	// vacuum into a temporary file so destPath only ever holds a verified copy
	tmpPath := destPath + ".tmp"
	defer func() {
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := src.ExecContext(ctx, "VACUUM INTO ?", tmpPath); err != nil {
		return 0, fmt.Errorf("failed to vacuum into backup: %w", err)
	}

	// prune and verify the copy before moving it into place
	backup, err := OpenDatabase(tmpPath, s.log)
	if err != nil {
		return 0, fmt.Errorf("failed to open backup: %w", err)
	}
	objectCount, err = backup.removeBufferedObjects()
	if err != nil {
		backup.Close()
		return 0, fmt.Errorf("failed to remove buffered objects from backup: %w", err)
	}
	var result string
	err = backup.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result)
	if err = errors.Join(err, backup.Close()); err != nil {
		return 0, fmt.Errorf("failed to check backup integrity: %w", err)
	} else if result != "ok" {
		return 0, fmt.Errorf("backup failed integrity check: %s", result)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		return 0, fmt.Errorf("failed to move backup into place: %w", err)
	}
	return objectCount, nil
}

// removeBufferedObjects deletes every object whose data is still buffered on disk and every multipart upload in
// progress, and returns the number of objects left.
func (s *Store) removeBufferedObjects() (remaining int64, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT bucket_id, name, version_id FROM objects WHERE filename IS NOT NULL`)
		if err != nil {
			return err
		}
		defer rows.Close()

		type bufferedObject struct {
			bid     int64
			name    string
			version string
		}
		var buffered []bufferedObject
		for rows.Next() {
			var obj bufferedObject
			if err := rows.Scan(&obj.bid, &obj.name, &obj.version); err != nil {
				return err
			}
			buffered = append(buffered, obj)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, obj := range buffered {
			row, _, err := deleteObjectRow(tx, obj.bid, obj.name, obj.version)
			if err != nil {
				return err
			} else if _, err := orphanDeleted(tx, row); err != nil {
				return err
			}
		}

		res, err := tx.Exec(`DELETE FROM multipart_uploads`)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if err := incrementStat(tx, statMultipartUploads, -n); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT stat_value FROM stats WHERE stat = $1`, statUploadedObjects).Scan(&remaining)
	})
	return
}
