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
// writes a database backup to destPath. The recorded sia_object_ids prevent the
// orphan loop from unpinning data the backup references. If the backup fails the
// snapshot is rolled back.
func (s *Store) CreateSnapshot(ctx context.Context, destPath string) error {
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	var snapshotID int64
	err := s.transaction(func(tx *txn) error {
		if err := tx.QueryRow("INSERT INTO snapshots (created_at, path) VALUES ($1, $2) RETURNING id", sqlTime(time.Now()), destPath).Scan(&snapshotID); err != nil {
			return err
		}
		_, err := tx.Exec(`
			INSERT INTO snapshot_objects (snapshot_id, sia_object_id)
			SELECT DISTINCT $1, sia_object_id FROM objects WHERE sia_object_id IS NOT NULL`, snapshotID)
		return err
	})
	if err != nil {
		return err
	}

	if err := s.Backup(ctx, destPath); err != nil {
		if dErr := s.DeleteSnapshot(snapshotID); dErr != nil {
			s.log.Error("failed to roll back snapshot after backup error", zap.Int64("snapshotID", snapshotID), zap.Error(dErr))
		}
		return fmt.Errorf("failed to create backup: %w", err)
	}
	return nil
}

// ListSnapshots returns all recorded snapshots ordered by id, each with the
// number of objects it references.
func (s *Store) ListSnapshots() (snapshots []objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT s.id, s.created_at, s.path, COUNT(so.sia_object_id)
			FROM snapshots s
			LEFT JOIN snapshot_objects so ON so.snapshot_id = s.id
			GROUP BY s.id
			ORDER BY s.id`)
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

// DeleteSnapshot removes a snapshot and its object references from the store.
// Objects no longer referenced by any snapshot or live object become eligible
// for unpinning on the next orphan loop.
func (s *Store) DeleteSnapshot(snapshotID int64) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("DELETE FROM snapshots WHERE id = $1", snapshotID)
		return err
	})
}
