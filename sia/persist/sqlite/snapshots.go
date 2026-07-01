package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap"
)

// CreateSnapshot records a snapshot of the current objects in the store and
// writes a database backup to destPath. It bumps the snapshot generation so any
// object orphaned while the backup references it is withheld from the orphan
// loop until the snapshot is deleted. If the backup fails the snapshot is rolled
// back.
func (s *Store) CreateSnapshot(ctx context.Context, destPath string) error {
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	// create snapshot
	var snapshotID int64
	if err := s.transaction(func(tx *txn) error {
		var gen int64
		err := tx.QueryRow("UPDATE global_settings SET snapshot_generation = snapshot_generation + 1 RETURNING snapshot_generation").Scan(&gen)
		if err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, path, gen, object_count)
			VALUES ($1, $2, $3, (SELECT stat_value FROM stats WHERE stat = $4))
			RETURNING id`, sqlTime(time.Now()), destPath, gen, statUploadedObjects).Scan(&snapshotID)
	}); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// proceed to backup the database, rolling back the snapshot if it fails
	if err := s.Backup(ctx, destPath); err != nil {
		if dErr := s.DeleteSnapshot(snapshotID); dErr != nil {
			s.log.Error("failed to roll back snapshot after backup error", zap.Int64("snapshotID", snapshotID), zap.Error(dErr))
		}
		return fmt.Errorf("failed to create backup: %w", err)
	}
	return nil
}

// ListSnapshots returns all recorded snapshots ordered by id, each with the
// number of objects it captured.
func (s *Store) ListSnapshots() (snapshots []objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT id, created_at, path, object_count
			FROM snapshots
			ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var snap objects.Snapshot
			if err := rows.Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.Path, &snap.ObjectCount); err != nil {
				return err
			}
			snapshots = append(snapshots, snap)
		}
		return rows.Err()
	})
	return
}

// DeleteSnapshot removes a snapshot from the store. Objects orphaned during its
// lifetime that no longer fall under any surviving snapshot's generation become
// eligible for unpinning on the next orphan loop.
func (s *Store) DeleteSnapshot(snapshotID int64) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE id = $1", snapshotID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return objects.ErrSnapshotNotFound
		}
		return nil
	})
}
