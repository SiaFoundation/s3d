package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// CreateSnapshot bumps the snapshot generation, records a snapshot of the
// current uploaded object count, and writes a consistent database backup to
// destPath. It returns the recorded snapshot. The Sia object ID is filled in
// later with SetSnapshotSiaObject once the backup is uploaded. If the backup
// fails the snapshot is rolled back.
func (s *Store) CreateSnapshot(ctx context.Context, destPath string) (snap objects.Snapshot, err error) {
	if destPath == "" {
		return objects.Snapshot{}, errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return objects.Snapshot{}, errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return objects.Snapshot{}, fmt.Errorf("failed to stat destination file: %w", err)
	}

	createdAt := time.Now()
	var objectCount int64
	if err := s.transaction(func(tx *txn) error {
		var gen int64
		if err := tx.QueryRow("UPDATE global_settings SET snapshot_generation = snapshot_generation + 1 RETURNING snapshot_generation").Scan(&gen); err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, gen, object_count)
			VALUES ($1, $2, (SELECT stat_value FROM stats WHERE stat = $3))
			RETURNING id, object_count`, sqlTime(createdAt), gen, statUploadedObjects).Scan(&snap.ID, &objectCount)
	}); err != nil {
		return objects.Snapshot{}, fmt.Errorf("failed to create snapshot: %w", err)
	}
	snap.CreatedAt = createdAt
	snap.ObjectCount = int(objectCount)

	// proceed to backup the database, rolling back the snapshot if it fails
	if err := s.Backup(ctx, destPath); err != nil {
		if dErr := s.DeleteSnapshot(snap.ID); dErr != nil {
			s.log.Error("failed to roll back snapshot after backup error", zap.Int64("snapshotID", snap.ID), zap.Error(dErr))
		}
		return objects.Snapshot{}, fmt.Errorf("failed to create backup: %w", err)
	}
	return snap, nil
}

// SetSnapshotSiaObject records the Sia object ID for an uploaded snapshot.
func (s *Store) SetSnapshotSiaObject(id int64, objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("UPDATE snapshots SET sia_object_id = $1 WHERE id = $2", sqlHash256(objectID), id)
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

// ListSnapshots returns all uploaded snapshots ordered by id.
func (s *Store) ListSnapshots() (snapshots []objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT id, created_at, sia_object_id, object_count
			FROM snapshots
			WHERE sia_object_id IS NOT NULL
			ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var snap objects.Snapshot
			if err := rows.Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), (*sqlHash256)(&snap.SiaObjectID), &snap.ObjectCount); err != nil {
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
